//! End-to-end Peer↔Peer tests over an in-process mpsc loopback.
//!
//! Two peers are wired together so each peer's outbound feeds the
//! other peer's inbound. Tests exercise: unary call/response,
//! notifications, server-side handlers, streaming RPCs, and
//! cancel-on-drop semantics. The runtime module is runtime-agnostic;
//! we use tokio here only as the test driver.
//!
//! Params on the wire are always JSON Objects — tests use small
//! `#[derive(Serialize, Deserialize)]` arg structs rather than tuples.

#![cfg(feature = "runtime")]

use futures::stream::StreamExt;
use peerline::runtime::{self, Peer, loopback};
use peerline::wire::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Wire two peers in-process using the lib's `loopback` helper.
fn wire_loopback() -> (Peer, Peer) {
    let (a, b, driver) = loopback();
    tokio::spawn(driver);
    (a, b)
}

// ---------------------------------------------------------------------------
// Param structs — declared once, reused by tests
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct AddArgs {
    a: i32,
    b: i32,
}

#[derive(Serialize, Deserialize)]
struct EchoArgs {
    s: String,
}

#[derive(Serialize, Deserialize)]
struct CountQuery {
    n: u32,
}

// ---------------------------------------------------------------------------
// Unary call / response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_returns_handler_result() {
    let (a, b) = wire_loopback();

    b.on_request("add", |args: AddArgs| async move {
        Ok::<_, RpcError>(args.a + args.b)
    });

    let result: i32 = a.call("add", &AddArgs { a: 2, b: 3 }).await.unwrap();
    assert_eq!(result, 5);
}

#[tokio::test]
async fn call_unknown_method_returns_method_not_found() {
    let (a, _b) = wire_loopback();

    let err = a.call::<_, ()>("nope", &json!({})).await.unwrap_err();
    match err {
        runtime::Error::Rpc(rpc_err) => {
            assert_eq!(rpc_err.error_type(), peerline::wire::ErrorType::MethodNotFound);
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }
}

#[tokio::test]
async fn call_handler_error_propagates() {
    let (a, b) = wire_loopback();

    b.on_request("fail", |_: serde_json::Value| async move {
        Err::<(), _>(RpcError {
            code: -32000,
            message: "boom".into(),
            data: None,
        })
    });

    let err = a
        .call::<_, serde_json::Value>("fail", &json!({}))
        .await
        .unwrap_err();
    match err {
        runtime::Error::Rpc(rpc_err) => assert_eq!(rpc_err.code, -32000),
        other => panic!("expected Rpc error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Notification
// ---------------------------------------------------------------------------

#[tokio::test]
async fn notify_delivers_to_handler_without_reply() {
    let (a, b) = wire_loopback();
    let seen = Arc::new(Notify::new());
    let seen_clone = seen.clone();

    b.on_notification("ping", move |_: serde_json::Value| {
        let seen = seen_clone.clone();
        async move {
            seen.notify_one();
        }
    });

    a.notify("ping", &json!({})).unwrap();
    tokio::time::timeout(Duration::from_secs(1), seen.notified())
        .await
        .expect("notification should arrive");
}

// ---------------------------------------------------------------------------
// Concurrent handlers — verify FuturesUnordered scheduling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handlers_run_concurrently() {
    let (a, b) = wire_loopback();

    b.on_request("slow", |_: serde_json::Value| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok::<_, RpcError>("slow-done")
    });
    b.on_request("fast", |_: serde_json::Value| async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok::<_, RpcError>("fast-done")
    });

    let empty1 = json!({});
    let empty2 = json!({});
    let start = std::time::Instant::now();
    let (slow, fast) = tokio::join!(
        a.call::<_, String>("slow", &empty1),
        a.call::<_, String>("fast", &empty2),
    );
    let elapsed = start.elapsed();

    assert_eq!(slow.unwrap(), "slow-done");
    assert_eq!(fast.unwrap(), "fast-done");
    assert!(
        elapsed < Duration::from_millis(150),
        "handlers ran sequentially: {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// Streaming RPC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_stream_yields_items_until_terminal() {
    let (a, b) = wire_loopback();

    b.on_stream_request("count", |q: CountQuery, sender| async move {
        for i in 0..q.n {
            sender.send_item(&i).unwrap();
        }
        Ok::<_, RpcError>(())
    });

    let mut items: Vec<u32> = Vec::new();
    let mut seqs: Vec<i64> = Vec::new();
    let mut stream: runtime::StreamReceiver<u32> =
        a.call_stream("count", &CountQuery { n: 5 }).unwrap();
    while let Some(item) = stream.next().await {
        let item = item.unwrap();
        seqs.push(item.seq);
        items.push(item.data);
    }
    assert_eq!(items, vec![0, 1, 2, 3, 4]);
    assert_eq!(seqs, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn call_stream_propagates_handler_error() {
    let (a, b) = wire_loopback();

    b.on_stream_request("boom", |_: serde_json::Value, sender| async move {
        sender.send_item(&"first").unwrap();
        Err::<(), _>(RpcError {
            code: -32000,
            message: "kaboom".into(),
            data: None,
        })
    });

    let mut stream: runtime::StreamReceiver<String> =
        a.call_stream("boom", &json!({})).unwrap();

    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.seq, 0);
    assert_eq!(first.data, "first");

    let second = stream.next().await.unwrap();
    match second {
        Err(runtime::Error::Rpc(e)) => {
            assert_eq!(e.code, -32000);
            assert_eq!(e.message, "kaboom");
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }
}

#[tokio::test]
async fn dropping_stream_receiver_is_silent_handler_still_runs() {
    // Dropping the StreamReceiver removes the entry from the local
    // stream registry; no wire frame is sent. The producer keeps
    // running and the handler completes normally — we verify by
    // signalling from inside the handler.
    let (a, b) = wire_loopback();
    let handler_done = Arc::new(Notify::new());
    let handler_done_clone = handler_done.clone();

    b.on_stream_request("tail", move |_: serde_json::Value, _sender| {
        let handler_done = handler_done_clone.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            handler_done.notify_one();
            Ok::<_, RpcError>(())
        }
    });

    {
        let _stream: runtime::StreamReceiver<String> =
            a.call_stream("tail", &json!({})).unwrap();
    }

    tokio::time::timeout(Duration::from_secs(1), handler_done.notified())
        .await
        .expect("server-side handler should still run after receiver drop");
}

// ---------------------------------------------------------------------------
// Peer symmetry — server-initiated calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn either_peer_can_initiate_a_call() {
    let (a, b) = wire_loopback();

    a.on_request("echo", |p: EchoArgs| async move {
        Ok::<_, RpcError>(p.s)
    });
    b.on_request("echo", |p: EchoArgs| async move {
        Ok::<_, RpcError>(p.s)
    });

    let from_a: String = a.call("echo", &EchoArgs { s: "hello-b".into() }).await.unwrap();
    let from_b: String = b.call("echo", &EchoArgs { s: "hello-a".into() }).await.unwrap();
    assert_eq!(from_a, "hello-b");
    assert_eq!(from_b, "hello-a");
}
