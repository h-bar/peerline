//! Stress test: many concurrent streams pushing large payloads over a
//! multi-threaded runtime.
//!
//! This hammers the outbound fairness scheduler and the dispatch loop at
//! once: `STREAMS` streaming requests run concurrently, each handler
//! floods `ITEMS_PER_STREAM` frames whose payload is a multi-KiB blob, so
//! the per-stream queues, the round-robin writer, and the inbound
//! decoder all move tens of MiB. The contract under load is unchanged —
//! nothing is lost, nothing is reordered, every per-stream sequence stays
//! contiguous, and the whole thing drains without deadlocking.
//!
//! Correctness is checked structurally (item counts, per-stream seq
//! contiguity, payload integrity), never by wall-clock timing; the outer
//! timeout only guards against a hang.

#![cfg(feature = "runtime")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::stream::StreamExt;
use peerline::runtime::{StreamReceiver, loopback};
use peerline::wire::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Concurrent streams in flight.
const STREAMS: u64 = 16;
/// Items each stream emits before its terminal.
const ITEMS_PER_STREAM: u64 = 200;
/// Payload size per item, in bytes — large enough that serialization and
/// wire moves dominate, exercising the queues rather than the framing.
const BLOB_BYTES: usize = 8 * 1024;

/// One stream item: its own index plus a large blob, so the consumer can
/// verify both ordering (`i`) and payload integrity (`blob`).
#[derive(Serialize, Deserialize)]
struct Chunk {
    i: u64,
    blob: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_streams_large_payload_stress() {
    let (client, server, driver) = loopback();
    tokio::spawn(driver);

    // Handler floods ITEMS_PER_STREAM large chunks in a tight loop. The
    // blob is built once and cloned per item — the clone is part of the
    // load. send_item is non-blocking, so this races the writer.
    server.on_stream_request("bulk", move |_: serde_json::Value, sender| async move {
        let blob = "x".repeat(BLOB_BYTES);
        for i in 0..ITEMS_PER_STREAM {
            sender
                .send_item(&Chunk {
                    i,
                    blob: blob.clone(),
                })
                .expect("send_item on a live stream");
        }
        Ok::<_, RpcError>(())
    });

    // Total items actually received across every stream — cross-checked
    // against the expected STREAMS * ITEMS_PER_STREAM at the end.
    let received = Arc::new(AtomicU64::new(0));

    let mut consumers = Vec::with_capacity(STREAMS as usize);
    for s in 0..STREAMS {
        let client = client.clone();
        let received = received.clone();
        consumers.push(tokio::spawn(async move {
            let mut stream: StreamReceiver<Chunk> = client.call_stream("bulk", &json!({})).unwrap();

            // Each stream's seq must be contiguous 0..ITEMS_PER_STREAM and
            // each payload intact, independent of how the scheduler
            // interleaved this stream with the other fifteen.
            let mut expected: u64 = 0;
            while let Some(item) = stream.next().await {
                let item = item.expect("stream item, not an error");
                assert_eq!(
                    item.seq as u64, expected,
                    "stream {s}: seq gap — got {}, expected {expected}",
                    item.seq
                );
                assert_eq!(item.data.i, expected, "stream {s}: payload index mismatch");
                assert_eq!(
                    item.data.blob.len(),
                    BLOB_BYTES,
                    "stream {s}: payload corrupted at seq {expected}"
                );
                expected += 1;
                received.fetch_add(1, Ordering::Relaxed);
            }
            assert_eq!(
                expected, ITEMS_PER_STREAM,
                "stream {s}: short read — got {expected} items"
            );
        }));
    }

    // Guard against a hang (lost terminal, deadlocked scheduler); the
    // assertions above — not this timeout — are what prove correctness.
    let join_all = async {
        for c in consumers {
            c.await.expect("consumer task panicked");
        }
    };
    tokio::time::timeout(Duration::from_secs(30), join_all)
        .await
        .expect("streams did not drain within 30s — possible deadlock or lost frames");

    assert_eq!(
        received.load(Ordering::Relaxed),
        STREAMS * ITEMS_PER_STREAM,
        "total item count across all streams is wrong"
    );

    // Scheduler odometer must account for at least every stream item it
    // dispatched, and every stream ended normally (consumers drained to
    // the terminal), so none were cancelled.
    let m = server.metrics();
    assert!(
        m.scheduler_rounds >= STREAMS * ITEMS_PER_STREAM,
        "scheduler dispatched fewer frames ({}) than items sent",
        m.scheduler_rounds
    );
    assert_eq!(
        m.cancelled_streams, 0,
        "no stream should have been cancelled"
    );
}
