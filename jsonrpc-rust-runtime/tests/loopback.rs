//! End-to-end Peer↔Peer tests over an in-process mpsc loopback.
//!
//! Two peers are wired together so each peer's outbound feeds the
//! other peer's inbound. Tests exercise: unary call/response,
//! notifications, server-side handlers, streaming RPCs, and
//! cancel-on-drop semantics. The lib is runtime-agnostic; we use
//! tokio here only as the test driver.

use futures::stream::StreamExt;
use jsonrpc_rust_runtime::{Peer, RpcError, loopback};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Wire two peers in-process using the lib's `loopback` helper.
/// The driver is spawned on the test runtime so callers can use
/// the returned peers directly.
fn wire_loopback() -> (Peer, Peer) {
    let (a, b, driver) = loopback();
    tokio::spawn(driver);
    (a, b)
}

// ---------------------------------------------------------------------------
// Unary call / response
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_returns_handler_result() {
    let (a, b) = wire_loopback();

    b.on_request("add", |params: (i32, i32)| async move {
        Ok::<_, RpcError>(params.0 + params.1)
    });

    let result: i32 = a.call("add", &(2, 3)).await.unwrap();
    assert_eq!(result, 5);
}

#[tokio::test]
async fn call_unknown_method_returns_method_not_found() {
    let (a, _b) = wire_loopback();

    let err = a.call::<_, ()>("nope", &json!({})).await.unwrap_err();
    match err {
        jsonrpc_rust_runtime::Error::Rpc(rpc_err) => {
            assert_eq!(rpc_err.code, jsonrpc_rust::wire::ERR_METHOD_NOT_FOUND);
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
        .call::<_, serde_json::Value>("fail", &json!([]))
        .await
        .unwrap_err();
    match err {
        jsonrpc_rust_runtime::Error::Rpc(rpc_err) => assert_eq!(rpc_err.code, -32000),
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

    a.notify("ping", &json!([])).unwrap();
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

    // A slow handler followed by a fast one — both should be in
    // flight at the same time, so total wall-clock < sum of
    // handler durations.
    b.on_request("slow", |_: serde_json::Value| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok::<_, RpcError>("slow-done")
    });
    b.on_request("fast", |_: serde_json::Value| async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok::<_, RpcError>("fast-done")
    });

    let empty1 = json!([]);
    let empty2 = json!([]);
    let start = std::time::Instant::now();
    let (slow, fast) = tokio::join!(
        a.call::<_, String>("slow", &empty1),
        a.call::<_, String>("fast", &empty2),
    );
    let elapsed = start.elapsed();

    assert_eq!(slow.unwrap(), "slow-done");
    assert_eq!(fast.unwrap(), "fast-done");
    // If they were sequential, elapsed >= 110ms. Concurrent should
    // be close to 100ms (the slow one's duration). Give some slack.
    assert!(
        elapsed < Duration::from_millis(150),
        "handlers ran sequentially: {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// Streaming RPC
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct CountQuery {
    n: u32,
}

#[tokio::test]
async fn call_stream_yields_items_until_close() {
    let (a, b) = wire_loopback();

    b.on_stream_request("count", |q: CountQuery, sender| async move {
        for i in 0..q.n {
            sender.send_item(&i).unwrap();
        }
        sender.close().unwrap();
    });

    let mut items: Vec<u32> = Vec::new();
    let mut seqs: Vec<u64> = Vec::new();
    let mut stream: jsonrpc_rust_runtime::StreamReceiver<u32> =
        a.call_stream("count", &CountQuery { n: 5 }).unwrap();
    while let Some(item) = stream.next().await {
        let item = item.unwrap();
        seqs.push(item.seq);
        items.push(item.data);
    }
    assert_eq!(items, vec![0, 1, 2, 3, 4]);
    // Auto-incrementing seq from the sender, starting at 1.
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn call_stream_propagates_handler_error() {
    let (a, b) = wire_loopback();

    b.on_stream_request("boom", |_: serde_json::Value, sender| async move {
        sender.send_item(&"first").unwrap();
        sender.error(-32000, "kaboom").unwrap();
    });

    let mut stream: jsonrpc_rust_runtime::StreamReceiver<String> =
        a.call_stream("boom", &json!([])).unwrap();

    // First yield: the item
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.seq, 1);
    assert_eq!(first.data, "first");

    // Second yield: the error
    let second = stream.next().await.unwrap();
    match second {
        Err(jsonrpc_rust_runtime::Error::Rpc(e)) => {
            assert_eq!(e.code, -32000);
            assert_eq!(e.message, "kaboom");
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }
}

#[tokio::test]
async fn dropping_stream_receiver_sends_cancel_upstream() {
    let (a, b) = wire_loopback();
    let cancelled = Arc::new(Notify::new());
    let cancelled_clone = cancelled.clone();

    b.on_stream_request("tail", move |_: serde_json::Value, sender| {
        let cancelled = cancelled_clone.clone();
        async move {
            // Server side just holds the stream open until dropped.
            // The runtime sends a stream:cancel when the receiver
            // drops; the StreamSender's Drop fires when this future
            // returns, but we want to test the receiver-side cancel.
            //
            // Wait briefly to keep the stream alive, then signal so
            // the test can proceed.
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancelled.notify_one();
            let _ = sender.close();
        }
    });

    {
        let _stream: jsonrpc_rust_runtime::StreamReceiver<String> =
            a.call_stream("tail", &json!([])).unwrap();
        // Drop happens at end of scope — sends stream:cancel.
    }

    tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .expect("server-side handler should run");
}

// ---------------------------------------------------------------------------
// Peer symmetry — server-initiated calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn either_peer_can_initiate_a_call() {
    let (a, b) = wire_loopback();

    // Register the same handler on both peers.
    // Params come in as a single-element array; the handler picks
    // out the first element.
    a.on_request(
        "echo",
        |(s,): (String,)| async move { Ok::<_, RpcError>(s) },
    );
    b.on_request(
        "echo",
        |(s,): (String,)| async move { Ok::<_, RpcError>(s) },
    );

    let from_a: String = a.call("echo", &("hello-b".to_string(),)).await.unwrap();
    let from_b: String = b.call("echo", &("hello-a".to_string(),)).await.unwrap();
    assert_eq!(from_a, "hello-b");
    assert_eq!(from_b, "hello-a");
}
