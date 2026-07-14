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
            assert_eq!(
                rpc_err.error_type(),
                peerline::wire::ErrorType::MethodNotFound
            );
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

    // A 2-party barrier only releases once BOTH handlers are awaiting it
    // at the same time. If the dispatch loop ran handlers sequentially,
    // the first would block on the barrier forever and the outer timeout
    // would fire. This proves real overlap structurally — no dependence
    // on wall-clock sleep precision (a fixed time threshold is flaky
    // because tokio's timer can stretch a 100ms sleep well past that).
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let barrier_a = barrier.clone();
    b.on_request("task_a", move |_: serde_json::Value| {
        let barrier = barrier_a.clone();
        async move {
            barrier.wait().await;
            Ok::<_, RpcError>("a-done")
        }
    });
    let barrier_b = barrier.clone();
    b.on_request("task_b", move |_: serde_json::Value| {
        let barrier = barrier_b.clone();
        async move {
            barrier.wait().await;
            Ok::<_, RpcError>("b-done")
        }
    });

    let empty1 = json!({});
    let empty2 = json!({});
    let (a_res, b_res) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            a.call::<_, String>("task_a", &empty1),
            a.call::<_, String>("task_b", &empty2),
        )
    })
    .await
    .expect("handlers ran sequentially: barrier never released");

    assert_eq!(a_res.unwrap(), "a-done");
    assert_eq!(b_res.unwrap(), "b-done");
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

    let mut stream: runtime::StreamReceiver<String> = a.call_stream("boom", &json!({})).unwrap();

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
async fn dropping_stream_receiver_cancels_but_nonsending_handler_completes() {
    // Dropping the StreamReceiver now emits a reserved cancel
    // notification and removes the local registry entry. A handler
    // that never sends can't observe the cancellation (cancellation
    // surfaces as a send failure), so it still completes normally —
    // we verify by signalling from inside the handler. See
    // tests/fairness.rs for the case where a *sending* handler
    // observes the cancel via `Error::Closed`.
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
        let _stream: runtime::StreamReceiver<String> = a.call_stream("tail", &json!({})).unwrap();
    }

    tokio::time::timeout(Duration::from_secs(1), handler_done.notified())
        .await
        .expect("server-side handler should still run after receiver drop");
}

#[tokio::test]
async fn parked_stream_handler_completes_on_connection_close() {
    // A stream handler parked on `cancelled()` that never sends must wake and
    // finish when the *connection* drops (not just on a graceful receiver
    // drop) — otherwise it would hang the inbound-loop drain forever and leak
    // the connection. Built over explicit channels with a separate client
    // driver so we can kill the client→server pipe.
    use futures::channel::mpsc;
    use std::convert::Infallible;

    let (a_to_b_tx, a_to_b_rx) = mpsc::unbounded::<String>();
    let (b_to_a_tx, b_to_a_rx) = mpsc::unbounded::<String>();
    let (client, client_driver) = Peer::new(a_to_b_tx, b_to_a_rx.map(Ok::<_, Infallible>));
    let (server, server_driver) = Peer::new(b_to_a_tx, a_to_b_rx.map(Ok::<_, Infallible>));

    let client_driver = tokio::spawn(client_driver);
    tokio::spawn(server_driver);

    let started = Arc::new(Notify::new());
    let done = Arc::new(Notify::new());
    let (s, d) = (started.clone(), done.clone());
    server.on_stream_request("hold", move |_: serde_json::Value, sender| {
        let (s, d) = (s.clone(), d.clone());
        async move {
            s.notify_one(); // handler entered
            sender.cancelled().await; // park until the consumer / connection is gone
            d.notify_one(); // handler finished
            Ok::<_, RpcError>(())
        }
    });

    // Open the stream and wait until the handler is parked.
    let _stream: runtime::StreamReceiver<serde_json::Value> =
        client.call_stream("hold", &json!({})).unwrap();
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("stream handler should start");

    // Hard-close: dropping the client driver drops its outbound sink, so the
    // server sees EOF. The parked handler must complete promptly.
    client_driver.abort();
    tokio::time::timeout(Duration::from_secs(2), done.notified())
        .await
        .expect("parked handler must complete on connection close (drain must not hang)");
}

// ---------------------------------------------------------------------------
// Peer symmetry — server-initiated calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn either_peer_can_initiate_a_call() {
    let (a, b) = wire_loopback();

    a.on_request("echo", |p: EchoArgs| async move { Ok::<_, RpcError>(p.s) });
    b.on_request("echo", |p: EchoArgs| async move { Ok::<_, RpcError>(p.s) });

    let from_a: String = a
        .call(
            "echo",
            &EchoArgs {
                s: "hello-b".into(),
            },
        )
        .await
        .unwrap();
    let from_b: String = b
        .call(
            "echo",
            &EchoArgs {
                s: "hello-a".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(from_a, "hello-b");
    assert_eq!(from_b, "hello-a");
}

// ---------------------------------------------------------------------------
// Regression — null-serializing payloads (0.0.3 RawValue deadlock)
// ---------------------------------------------------------------------------

// A response whose payload serializes to JSON `null` (`call::<_, ()>` and
// any handler returning `()` / `None`) must round-trip. The 0.0.3
// hand-written RawValue codec collapsed a present `"data": null` to "no
// data", rejected the response frame, and the caller's waiter hung forever.
#[tokio::test]
async fn call_with_unit_response_does_not_hang() {
    let (a, b) = wire_loopback();
    b.on_request("noop", |_: serde_json::Value| async {
        Ok::<(), RpcError>(())
    });

    let args = json!({});
    let out = tokio::time::timeout(Duration::from_secs(2), a.call::<_, ()>("noop", &args))
        .await
        .expect("call::<_, ()> must not hang — unit/null response must match its waiter");
    out.expect("unit call should succeed");
}

// The same null-payload hazard on the stream path: items whose payload
// serializes to `null` must be delivered (not silently swallowed), and the
// stream must still close gracefully — a call + stream-close round-trip.
#[tokio::test]
async fn stream_delivers_null_payload_items_then_closes() {
    let (a, b) = wire_loopback();
    b.on_stream_request("nulls", |_: serde_json::Value, sender| async move {
        sender.send_item(&()).unwrap(); // → "data": null
        sender.send_item(&Option::<u32>::None).unwrap(); // → "data": null
        Ok::<_, RpcError>(())
    });

    let args = json!({});
    let mut stream: runtime::StreamReceiver<serde_json::Value> =
        a.call_stream("nulls", &args).unwrap();

    let mut items = Vec::new();
    while let Some(item) = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("stream must not hang")
    {
        items.push(item.expect("stream item"));
    }

    assert_eq!(items.len(), 2, "both null-payload items must be delivered");
    assert!(
        items.iter().all(|i| i.data.is_null()),
        "payloads should decode as JSON null"
    );
    assert_eq!(items.iter().map(|i| i.seq).collect::<Vec<_>>(), vec![0, 1]);
}
