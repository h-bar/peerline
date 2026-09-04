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

// ---------------------------------------------------------------------------
// Error `data` forwarding — the wire supports an optional data payload on
// RpcError; the runtime must relay it, not strip it to code + msg.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_handler_error_preserves_data_payload() {
    let (a, b) = wire_loopback();

    b.on_request("fail", |_: serde_json::Value| async move {
        Err::<(), _>(RpcError {
            code: -32000,
            message: "boom".into(),
            data: Some(json!({"hint": "retry", "attempt": 3})),
        })
    });

    let err = a
        .call::<_, serde_json::Value>("fail", &json!({}))
        .await
        .unwrap_err();
    match err {
        runtime::Error::Rpc(e) => {
            assert_eq!(e.code, -32000);
            assert_eq!(e.data, Some(json!({"hint": "retry", "attempt": 3})));
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_error_terminal_preserves_data_payload() {
    let (a, b) = wire_loopback();

    b.on_stream_request("boom", |_: serde_json::Value, _sender| async move {
        Err::<(), _>(RpcError {
            code: -32001,
            message: "mid-stream".into(),
            data: Some(json!({"at": 7})),
        })
    });

    let mut stream: runtime::StreamReceiver<String> = a.call_stream("boom", &json!({})).unwrap();
    match stream.next().await.unwrap() {
        Err(runtime::Error::Rpc(e)) => {
            assert_eq!(e.code, -32001);
            assert_eq!(e.data, Some(json!({"at": 7})));
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Finished streams leave the registry — no leak, accurate metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn finished_stream_is_removed_from_registry() {
    let (a, b) = wire_loopback();

    b.on_stream_request("three", |_: serde_json::Value, sender| async move {
        for i in 0..3u32 {
            sender.send_item(&i).unwrap();
        }
        Ok::<_, RpcError>(())
    });

    let mut stream: runtime::StreamReceiver<u32> = a.call_stream("three", &json!({})).unwrap();
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        got.push(item.expect("stream item").data);
    }
    assert_eq!(got, vec![0, 1, 2]);

    // The terminal removed the registry entry — the handle is still held
    // (not dropped), so this is the terminal's doing, not Drop's.
    assert_eq!(
        a.metrics().active_streams,
        0,
        "a finished stream must not linger in the registry"
    );
    drop(stream);
}

// ---------------------------------------------------------------------------
// Teardown must not deadlock on a handler that's awaiting a nested call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn teardown_completes_when_handler_awaits_nested_call() {
    // A server handler performs a nested server→client call (the `reverse`
    // pattern). The client goes away before answering, so the nested call
    // can never complete on its own. Teardown must reject it (instead of
    // draining handlers first and deadlocking on the pending reply), let
    // the handler finish, and resolve the driver. The timeout is a
    // hang-detector, not a performance assertion.
    use futures::channel::mpsc;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (a_to_b_tx, a_to_b_rx) = mpsc::unbounded::<String>();
    let (b_to_a_tx, b_to_a_rx) = mpsc::unbounded::<String>();
    let (client, client_driver) = Peer::new(a_to_b_tx, b_to_a_rx.map(Ok::<_, Infallible>));
    let (server, server_driver) = Peer::new(b_to_a_tx, a_to_b_rx.map(Ok::<_, Infallible>));

    let client_driver = tokio::spawn(client_driver);
    let server_driver = tokio::spawn(server_driver);

    // The client "answers" upstream calls by never answering.
    client.on_request("upstream", |_: serde_json::Value| async move {
        futures::future::pending::<()>().await;
        Ok::<_, RpcError>(())
    });

    let nested_failed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let (nf, en, srv) = (nested_failed.clone(), entered.clone(), server.clone());
    server.on_request("outer", move |_: serde_json::Value| {
        let (nf, en, srv) = (nf.clone(), en.clone(), srv.clone());
        async move {
            en.notify_one();
            if srv
                .call::<_, serde_json::Value>("upstream", &json!({}))
                .await
                .is_err()
            {
                nf.store(true, Ordering::SeqCst);
            }
            Ok::<_, RpcError>(())
        }
    });

    // Kick off the outer call (its reply will never arrive — ignore it) and
    // wait until the server handler is inside, awaiting upstream.
    let c = client.clone();
    tokio::spawn(async move {
        let _ = c.call::<_, serde_json::Value>("outer", &json!({})).await;
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("server handler should start");

    // Client leaves: its outbound sink drops, the server sees clean EOF and
    // begins teardown while `outer` is still awaiting the nested call.
    client_driver.abort();

    tokio::time::timeout(Duration::from_secs(5), server_driver)
        .await
        .expect("server teardown must not deadlock on the nested call")
        .expect("server driver task");
    assert!(
        nested_failed.load(Ordering::SeqCst),
        "the nested call should have been rejected, not left pending"
    );
}

// ---------------------------------------------------------------------------
// Writer death must still sweep caller state — no permanently hung calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn writer_failure_rejects_pending_calls() {
    // The transport's write side dies (sink error) while the read side is
    // still open. The driver resolves via the writer arm — the reader (and
    // its teardown) is dropped mid-await — so the sweep must run in the
    // driver, or this call waits forever on a reply that can't arrive.
    use futures::channel::mpsc;
    use std::convert::Infallible;

    let (dead_tx, dead_rx) = mpsc::unbounded::<String>();
    drop(dead_rx); // first sink.send() will error
    let (in_tx, in_rx) = mpsc::unbounded::<String>();
    let (peer, driver) = Peer::new(dead_tx, in_rx.map(Ok::<_, Infallible>));
    tokio::spawn(driver);

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        peer.call::<_, serde_json::Value>("ping", &json!({})),
    )
    .await
    .expect("call must be rejected when the writer dies, not hang")
    .unwrap_err();
    match err {
        runtime::Error::Rpc(e) => assert_eq!(e.message, "connection closed"),
        runtime::Error::Closed => {} // raced the sweep — also fine
        other => panic!("expected connection-closed rejection, got {other:?}"),
    }
    drop(in_tx); // read side stayed open the whole time
}

// ---------------------------------------------------------------------------
// Clean half-close flushes the tail response to the wire
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clean_close_flushes_last_drained_response() {
    // A handler is still running when the inbound side half-closes cleanly.
    // Its response is enqueued during the teardown drain — the very last
    // frame before the reader returns — and must still reach the transport
    // sink (the driver awaits the writer's flush) rather than being dropped
    // with the outbound queue.
    use futures::channel::mpsc;
    use std::convert::Infallible;

    let (out_tx, mut out_rx) = mpsc::unbounded::<String>();
    let (in_tx, in_rx) = mpsc::unbounded::<String>();
    let (peer, driver) = Peer::new(out_tx, in_rx.map(Ok::<_, Infallible>));
    tokio::spawn(driver);

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (en, rel) = (entered.clone(), release.clone());
    peer.on_request("slow", move |_: serde_json::Value| {
        let (en, rel) = (en.clone(), rel.clone());
        async move {
            en.notify_one();
            rel.notified().await;
            Ok::<_, RpcError>("flushed")
        }
    });

    in_tx
        .unbounded_send(r#"{"ver":"1","kind":"req","id":1,"op":"slow","args":{}}"#.into())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("handler should start");

    // Half-close the inbound while the handler is parked, and give the
    // dispatch loop a chance to see EOF and enter its drain before the
    // handler is released (current-thread runtime makes this determinate).
    drop(in_tx);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    release.notify_one();

    let frame = tokio::time::timeout(Duration::from_secs(5), out_rx.next())
        .await
        .expect("tail response must be flushed on clean close")
        .expect("sink closed before the response was written");
    assert!(
        frame.contains(r#""data":"flushed""#) && frame.contains(r#""id":1"#),
        "expected the drained handler's response, got: {frame}"
    );
}

// ---------------------------------------------------------------------------
// An escaped StreamSender must not hang the driver's post-close flush
// ---------------------------------------------------------------------------

#[tokio::test]
async fn escaped_stream_sender_does_not_hang_driver_shutdown() {
    // A handler may legally move its StreamSender out (e.g. into a spawned
    // producer task). Once the handler returns, the stream is finished on
    // the wire — the writer must not wait on that stream's channel just
    // because a sender clone is still alive somewhere, or the driver's
    // clean-close flush never terminates.
    use futures::channel::mpsc;
    use std::convert::Infallible;

    let (out_tx, _out_rx) = mpsc::unbounded::<String>(); // keep sink healthy
    let (in_tx, in_rx) = mpsc::unbounded::<String>();
    let (server, driver) = Peer::new(out_tx, in_rx.map(Ok::<_, Infallible>));
    let driver = tokio::spawn(driver);

    let stash: Arc<std::sync::Mutex<Option<runtime::StreamSender>>> =
        Arc::new(std::sync::Mutex::new(None));
    let stashed = Arc::new(Notify::new());
    let (st, sn) = (stash.clone(), stashed.clone());
    server.on_stream_request("keep", move |_: serde_json::Value, sender| {
        let (st, sn) = (st.clone(), sn.clone());
        async move {
            sender.send_item(&1u32).unwrap();
            *st.lock().unwrap() = Some(sender); // escapes the handler
            sn.notify_one();
            Ok::<_, RpcError>(())
        }
    });

    in_tx
        .unbounded_send(r#"{"ver":"1","kind":"req","id":1,"op":"keep","args":{}}"#.into())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), stashed.notified())
        .await
        .expect("handler should run and stash its sender");

    drop(in_tx); // clean EOF → teardown → flush → the driver must resolve
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("driver must resolve despite the escaped StreamSender")
        .expect("driver task");
    assert!(stash.lock().unwrap().is_some());
}

// ---------------------------------------------------------------------------
// A bundled-data terminal fully ends the stream: next poll is None, and a
// frame arriving after the terminal is discarded (not delivered)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn polling_past_bundled_terminal_ends_and_discards_late_frames() {
    use futures::channel::mpsc;
    use std::convert::Infallible;

    let (out_tx, _out_rx) = mpsc::unbounded::<String>();
    let (in_tx, in_rx) = mpsc::unbounded::<String>();
    let (peer, driver) = Peer::new(out_tx, in_rx.map(Ok::<_, Infallible>));
    tokio::spawn(driver);

    // First call_stream allocates id 1.
    let mut stream: runtime::StreamReceiver<u32> = peer.call_stream("tail", &json!({})).unwrap();
    in_tx
        .unbounded_send(r#"{"ver":"1","kind":"stream","id":1,"seq":0,"data":10}"#.into())
        .unwrap();
    in_tx
        .unbounded_send(r#"{"ver":"1","kind":"stream","id":1,"seq":-1,"data":20}"#.into())
        .unwrap();

    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("first item")
        .unwrap()
        .unwrap();
    assert_eq!((first.seq, first.data), (0, 10));
    let last = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("bundled terminal item")
        .unwrap()
        .unwrap();
    assert_eq!((last.seq, last.data), (-1, 20));

    // The terminal removed the registry entry: this late frame has nowhere
    // to route and is discarded...
    in_tx
        .unbounded_send(r#"{"ver":"1","kind":"stream","id":1,"seq":1,"data":99}"#.into())
        .unwrap();
    // ...and polling past the bundled terminal yields a clean end instead
    // of hanging (the old leak kept the channel open forever) or yielding
    // the late item.
    let end = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("poll past bundled terminal must not hang");
    assert!(end.is_none(), "expected clean end, got {end:?}");
    drop(in_tx);
}
