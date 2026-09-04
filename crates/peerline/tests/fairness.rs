//! Outbound fairness-scheduler tests.
//!
//! These exercise the per-stream round-robin scheduler that replaced
//! the single outbound FIFO: independent streams interleave on the wire
//! instead of one draining fully before another starts; per-stream
//! sequence order stays monotonic under that interleaving; dropping a
//! receiver cancels the producer; and a stalled transport never blocks
//! the producer (backlog is allowed to grow and is surfaced via
//! metrics).

#![cfg(feature = "runtime")]

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc;
use futures::stream::StreamExt;
use peerline::peer as pl;
use peerline::runtime::{self, Peer, loopback};
use peerline::wire::{Content, Frame};
use serde_json::json;
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Serialize a stream request frame for `op` with id `id`.
fn stream_req(id: u64, op: &str) -> String {
    let req = pl::request(id, op, &json!({})).unwrap();
    serde_json::to_string(&Frame::from(req)).unwrap()
}

/// Drive one server peer with a controllable inbound (we push request
/// frames) and a controllable outbound (we drain and inspect the raw
/// wire frames). Registers a `big` stream handler that emits
/// `big_n` items in a tight loop and a `small` stream handler that
/// emits `small_n`, both via the synchronous `send_item`. Fires a
/// request for each (id 1 = big, id 2 = small) and returns every
/// stream frame `(id, seq)` in the exact order it hit the wire.
async fn capture_two_streams(big_n: u32, small_n: u32) -> Vec<(u64, i64)> {
    let (in_tx, in_rx) = mpsc::unbounded::<Result<String, Infallible>>();
    let (out_tx, mut out_rx) = mpsc::unbounded::<String>();

    let (server, driver) = Peer::new(out_tx, in_rx);
    tokio::spawn(driver);

    server.on_stream_request("big", move |_: serde_json::Value, sender| async move {
        for i in 0..big_n {
            sender.send_item(&i).unwrap();
        }
        Ok::<_, peerline::wire::RpcError>(())
    });
    server.on_stream_request("small", move |_: serde_json::Value, sender| async move {
        for i in 0..small_n {
            sender.send_item(&i).unwrap();
        }
        Ok::<_, peerline::wire::RpcError>(())
    });

    // Fire big first, then small.
    in_tx.unbounded_send(Ok(stream_req(1, "big"))).unwrap();
    in_tx.unbounded_send(Ok(stream_req(2, "small"))).unwrap();

    // Collect wire frames until both streams have sent their terminal.
    let mut order: Vec<(u64, i64)> = Vec::new();
    let mut terminals = 0;
    let collect = async {
        while let Some(text) = out_rx.next().await {
            if let Frame::V1(Content::Stream(s)) = serde_json::from_str::<Frame>(&text).unwrap() {
                order.push((s.id, s.seq));
                if s.seq == -1 {
                    terminals += 1;
                    if terminals == 2 {
                        break;
                    }
                }
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), collect)
        .await
        .expect("both streams should terminate");
    order
}

// ---------------------------------------------------------------------------
// Interleaving + monotonic sequence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn independent_streams_interleave_on_the_wire() {
    // Big enqueues its whole burst before small even starts, yet the
    // round-robin writer must not drain big fully before small: every
    // small-stream frame must reach the wire before big's terminal.
    // Under the old single FIFO the entire small stream came *after*
    // big finished.
    let order = capture_two_streams(50, 3).await;

    let big_terminal = order
        .iter()
        .position(|&(id, seq)| id == 1 && seq == -1)
        .expect("big terminal present");
    let last_small = order
        .iter()
        .rposition(|&(id, _)| id == 2)
        .expect("small frames present");

    assert!(
        last_small < big_terminal,
        "small stream did not interleave — it finished at {last_small}, big ended at {big_terminal}"
    );

    // And it is genuine interleaving: big items still arrive after the
    // first small item (not "all small, then all big").
    let first_small = order.iter().position(|&(id, _)| id == 2).unwrap();
    assert!(
        order[first_small..]
            .iter()
            .any(|&(id, seq)| id == 1 && seq >= 0),
        "expected big items after the first small item"
    );
}

#[tokio::test]
async fn per_stream_sequence_is_monotonic_under_interleaving() {
    let order = capture_two_streams(20, 8).await;

    for id in [1u64, 2] {
        let seqs: Vec<i64> = order
            .iter()
            .filter(|&&(fid, seq)| fid == id && seq >= 0)
            .map(|&(_, seq)| seq)
            .collect();
        let expected: Vec<i64> = (0..seqs.len() as i64).collect();
        assert_eq!(seqs, expected, "stream {id} seq not monotonic/contiguous");
    }
}

// ---------------------------------------------------------------------------
// Cancel-on-drop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropping_receiver_makes_producer_send_fail() {
    let (a, b, driver) = loopback();
    tokio::spawn(driver);

    let saw_closed = Arc::new(Notify::new());
    let saw_closed_h = saw_closed.clone();

    b.on_stream_request("tail", move |_: serde_json::Value, sender| {
        let saw_closed = saw_closed_h.clone();
        async move {
            let mut i = 0u64;
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
                match sender.send_item(&i) {
                    Ok(()) => i += 1,
                    Err(runtime::Error::Closed) => {
                        // Cancellation surfaced as a send failure.
                        saw_closed.notify_one();
                        return Ok::<_, peerline::wire::RpcError>(());
                    }
                    Err(other) => panic!("unexpected send error: {other:?}"),
                }
            }
        }
    });

    let mut stream: runtime::StreamReceiver<u64> = a.call_stream("tail", &json!({})).unwrap();
    // Pull at least one item so the stream is live on both sides.
    let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("first item")
        .expect("stream open")
        .unwrap();
    assert_eq!(first.seq, 0);

    // Drop the receiver → reserved cancel notification → producer's
    // next send returns Closed.
    drop(stream);

    tokio::time::timeout(Duration::from_secs(2), saw_closed.notified())
        .await
        .expect("producer send should fail with Closed after receiver drop");
}

#[tokio::test]
async fn handler_observes_cancellation_via_cancelled_future() {
    let (a, b, driver) = loopback();
    tokio::spawn(driver);

    let stopped = Arc::new(Notify::new());
    let stopped_h = stopped.clone();

    b.on_stream_request("work", move |_: serde_json::Value, sender| {
        let stopped = stopped_h.clone();
        async move {
            // Open the stream, then block on cancellation WITHOUT ever
            // sending again — so the only way to unblock is the
            // `cancelled()` future firing (not a send failure).
            sender.send_item(&0u64).unwrap();
            assert!(!sender.is_cancelled());
            sender.cancelled().await;
            assert!(sender.is_cancelled());
            stopped.notify_one();
            Ok::<_, peerline::wire::RpcError>(())
        }
    });

    let mut stream: runtime::StreamReceiver<u64> = a.call_stream("work", &json!({})).unwrap();
    let first = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("first item")
        .expect("stream open")
        .unwrap();
    assert_eq!(first.seq, 0);

    drop(stream);

    tokio::time::timeout(Duration::from_secs(2), stopped.notified())
        .await
        .expect("cancelled() should resolve after the receiver is dropped");
}

// ---------------------------------------------------------------------------
// Non-blocking under a stalled transport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stalled_transport_does_not_block_producer_and_backlog_is_visible() {
    const N: u32 = 500;

    // Server whose transport sink we never drain: the writer blocks
    // after buffering a frame or two. The contract is that this must
    // NOT block the producer — sends stay non-blocking and the backlog
    // is allowed to grow, surfaced via metrics rather than throttled.
    let (sink_tx, _sink_rx) = mpsc::channel::<String>(1); // held, never read
    let (in_tx, in_rx) = mpsc::unbounded::<Result<String, Infallible>>();

    let (server, driver) = Peer::new(sink_tx, in_rx);
    tokio::spawn(driver);

    let sent = Arc::new(Notify::new());
    let sent_h = sent.clone();
    server.on_stream_request("flood", move |_: serde_json::Value, sender| {
        let sent = sent_h.clone();
        async move {
            for i in 0..N {
                // Non-blocking: returns immediately even though the
                // transport is stalled.
                sender.send_item(&i).unwrap();
            }
            sent.notify_one();
            // Stay alive so the stream's registry entry (and its depth)
            // persists while the test samples metrics.
            std::future::pending::<()>().await;
            Ok::<_, peerline::wire::RpcError>(())
        }
    });

    in_tx.unbounded_send(Ok(stream_req(1, "flood"))).unwrap();

    // The producer must finish its whole burst promptly despite the
    // dead transport — proof it never blocked.
    tokio::time::timeout(Duration::from_secs(1), sent.notified())
        .await
        .expect("producer blocked on a stalled transport");

    // The backlog grew and is observable (not throttled away).
    let m = server.metrics();
    assert!(
        m.outbound_depth >= 100,
        "expected a visible backlog, outbound_depth = {}",
        m.outbound_depth
    );
    assert!(
        m.stream_items_queued_max >= 100,
        "per-stream backlog not surfaced, max = {}",
        m.stream_items_queued_max
    );
}
