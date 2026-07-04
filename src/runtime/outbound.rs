//! Fair outbound scheduler.
//!
//! Replaces the old single unbounded FIFO (`mpsc::UnboundedSender<String>`
//! drained in insertion order) with a scheduler that keeps independent
//! streams from head-of-line-blocking each other:
//!
//! - **One control channel** — unary responses, errors, notifications,
//!   this peer's own outgoing requests, and stream-cancel notifications.
//!   Unbounded (control must never backpressure) and drained at higher
//!   priority than stream items.
//! - **Per-stream channels** — one channel of pre-serialized frame
//!   strings per active outbound stream, so a handler dumping a large
//!   replay fills only its own queue.
//! - **A single writer task** ([`forward_outbound`]) that multiplexes
//!   them: control first (`select_biased!`), then round-robin across the
//!   live stream channels via [`SelectAll`], which keeps a rotating
//!   cursor and auto-drops streams whose channel has closed.
//!
//! The scheduler is uniformly **non-blocking**: a slow or dead
//! transport never blocks a producer. Per-stream channels stay
//! unbounded, so `send_item` always succeeds (until the stream is
//! cancelled / the connection closes) and a terminal frame always
//! enqueues in order behind the stream's items. Backlog that a slow
//! consumer causes is allowed to grow and is surfaced — not throttled —
//! via the depth counters ([`super::Metrics`]); the application decides
//! whether to react.
//!
//! Each dequeued [`OutboundFrame`] carries the producing stream's
//! [`StreamMeta`] (which includes the stream `id`). That is the seam a
//! future multi-connection transport would use to pin a stream to one
//! connection without re-parsing the frame; the single-connection
//! writer only uses it for depth accounting.

use super::error::Error;
use crate::wire::Id;
use futures::channel::mpsc;
use futures::select_biased;
use futures::sink::{Sink, SinkExt};
use futures::stream::{BoxStream, SelectAll, StreamExt};
use futures::task::AtomicWaker;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// Per-stream shared state, held jointly by the stream's
/// [`super::StreamSender`] and its registry [`StreamHandle`] (both via
/// `Arc`). Carries the outbound depth counter (queued items not yet
/// written, cloned into every [`OutboundFrame`] so the writer can
/// decrement it) and the cancellation signal.
pub(crate) struct StreamMeta {
    /// The stream's request id. Unused by the single-connection writer;
    /// reserved as the routing key for a future multi-connection
    /// transport that pins a stream to one connection.
    #[allow(dead_code)]
    pub(crate) id: Id,
    /// Items enqueued but not yet written to the transport. Pure
    /// observability — backs [`super::Metrics`]'s per-stream depth.
    pub(crate) depth: AtomicUsize,
    /// Set once the consumer cancels this stream (drops its receiver).
    /// Lets a handler observe cancellation via
    /// [`super::StreamSender::cancelled`] before its next send fails.
    pub(crate) cancelled: AtomicBool,
    /// Woken alongside `cancelled` so a handler awaiting cancellation
    /// re-polls promptly.
    pub(crate) cancel_waker: AtomicWaker,
}

/// One frame handed to the writer: the serialized text plus, for stream
/// frames, the producing stream's [`StreamMeta`] (control frames carry
/// `None`).
pub(crate) struct OutboundFrame {
    pub(crate) text: String,
    pub(crate) meta: Option<Arc<StreamMeta>>,
}

/// Producer-side registry entry for one active outbound stream. Held by
/// the [`Outbound`] so the runtime can send the terminal frame
/// ([`Outbound::finish_stream`]) or cancel the stream
/// ([`Outbound::cancel_stream`]) independently of the handler's own
/// sender.
struct StreamHandle {
    close_tx: mpsc::UnboundedSender<String>,
    meta: Arc<StreamMeta>,
}

/// The outbound scheduler. Lives behind an [`Arc`] shared by the peer
/// handle, the dispatch loop, and every [`super::StreamSender`].
pub(crate) struct Outbound {
    control_tx: mpsc::UnboundedSender<String>,
    new_stream_tx: mpsc::UnboundedSender<BoxStream<'static, OutboundFrame>>,
    streams: Mutex<HashMap<Id, StreamHandle>>,
    /// Aggregate frames queued but not yet written (control + all
    /// stream queues). Surfaced as [`super::Metrics::outbound_depth`].
    pub(crate) outbound_depth: AtomicUsize,
    pub(crate) cancelled_streams: AtomicU64,
    pub(crate) scheduler_rounds: AtomicU64,
}

/// The receiver halves the [`forward_outbound`] writer owns. Returned
/// by [`Outbound::new`] alongside the shared [`Arc<Outbound>`] so the
/// driver can hand them straight to the writer.
pub(crate) struct OutboundReceivers {
    control_rx: mpsc::UnboundedReceiver<String>,
    new_stream_rx: mpsc::UnboundedReceiver<BoxStream<'static, OutboundFrame>>,
}

impl Outbound {
    /// Construct the scheduler.
    pub(crate) fn new() -> (Arc<Self>, OutboundReceivers) {
        let (control_tx, control_rx) = mpsc::unbounded();
        let (new_stream_tx, new_stream_rx) = mpsc::unbounded();
        let outbound = Arc::new(Self {
            control_tx,
            new_stream_tx,
            streams: Mutex::new(HashMap::new()),
            outbound_depth: AtomicUsize::new(0),
            cancelled_streams: AtomicU64::new(0),
            scheduler_rounds: AtomicU64::new(0),
        });
        (
            outbound,
            OutboundReceivers {
                control_rx,
                new_stream_rx,
            },
        )
    }

    /// Enqueue a control frame (response / notification / request /
    /// cancel). Bumps [`Self::outbound_depth`]; rolls it back if the
    /// channel is closed.
    pub(crate) fn enqueue_control(&self, text: String) -> Result<(), Error> {
        self.outbound_depth.fetch_add(1, Ordering::AcqRel);
        self.control_tx.unbounded_send(text).map_err(|_| {
            self.outbound_depth.fetch_sub(1, Ordering::AcqRel);
            Error::Closed
        })
    }

    /// Open a new outbound stream: create its channel, register a
    /// handle, hand the receiver to the writer, and return the
    /// producer's sender + shared [`StreamMeta`].
    pub(crate) fn open_stream(&self, id: Id) -> (mpsc::UnboundedSender<String>, Arc<StreamMeta>) {
        let (tx, rx) = mpsc::unbounded::<String>();
        let meta = Arc::new(StreamMeta {
            id,
            depth: AtomicUsize::new(0),
            cancelled: AtomicBool::new(false),
            cancel_waker: AtomicWaker::new(),
        });
        let m = meta.clone();
        let mapped: BoxStream<'static, OutboundFrame> = rx
            .map(move |text| OutboundFrame {
                text,
                meta: Some(m.clone()),
            })
            .boxed();
        // If the writer is gone the stream simply never drains; the
        // producer's sends will surface `Closed` once the channel drops.
        let _ = self.new_stream_tx.unbounded_send(mapped);
        self.streams.lock().unwrap().insert(
            id,
            StreamHandle {
                close_tx: tx.clone(),
                meta: meta.clone(),
            },
        );
        (tx, meta)
    }

    /// Finish a stream normally: enqueue its terminal frame (ordered
    /// after any still-queued items) and drop the registry handle so
    /// the channel closes once drained. No-op if the stream was already
    /// cancelled / finished.
    pub(crate) fn finish_stream(&self, id: Id, terminal: String) {
        let handle = self.streams.lock().unwrap().remove(&id);
        if let Some(handle) = handle {
            handle.meta.depth.fetch_add(1, Ordering::AcqRel);
            self.outbound_depth.fetch_add(1, Ordering::AcqRel);
            if handle.close_tx.unbounded_send(terminal).is_err() {
                handle.meta.depth.fetch_sub(1, Ordering::AcqRel);
                self.outbound_depth.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    /// Cancel a stream (the consumer dropped its receiver): raise the
    /// cancellation signal (so a handler awaiting
    /// [`super::StreamSender::cancelled`] wakes), close the channel so
    /// the handler's next send returns [`Error::Closed`], and drop the
    /// handle without sending a terminal (the consumer is gone). No-op
    /// if already finished.
    pub(crate) fn cancel_stream(&self, id: Id) {
        let handle = self.streams.lock().unwrap().remove(&id);
        if let Some(handle) = handle {
            handle.meta.cancelled.store(true, Ordering::Release);
            handle.meta.cancel_waker.wake();
            handle.close_tx.close_channel();
            self.cancelled_streams.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `(max, total)` queued item count across all active outbound
    /// streams. Backs the per-stream queue metrics.
    pub(crate) fn stream_depths(&self) -> (usize, usize) {
        let guard = self.streams.lock().unwrap();
        let mut max = 0;
        let mut total = 0;
        for handle in guard.values() {
            let d = handle.meta.depth.load(Ordering::Acquire);
            total += d;
            if d > max {
                max = d;
            }
        }
        (max, total)
    }
}

/// Drain the scheduler into the transport sink, fairly.
///
/// Priority: control frames first (`select_biased!` polls control ahead
/// of streams), then round-robin across live stream channels via
/// [`SelectAll`]. New streams are pushed in as they open. Each written
/// stream frame decrements its depth counter (observability only).
/// Exits when the sink errors (or, once the peer is fully torn down,
/// when every source has terminated); the driver also drops this future
/// when the reader side sees transport EOF.
pub(crate) async fn forward_outbound<Si, SiE>(
    outbound: Arc<Outbound>,
    rxs: OutboundReceivers,
    mut sink: Si,
) where
    Si: Sink<String, Error = SiE> + Unpin,
    SiE: std::fmt::Display,
{
    let mut control = rxs
        .control_rx
        .map(|text| OutboundFrame { text, meta: None })
        .fuse();
    let mut new_streams = rxs.new_stream_rx.fuse();
    let mut streams: SelectAll<BoxStream<'static, OutboundFrame>> = SelectAll::new();

    loop {
        let frame = select_biased! {
            f = control.select_next_some() => f,
            bx = new_streams.select_next_some() => {
                streams.push(bx);
                continue;
            }
            f = streams.select_next_some() => f,
            complete => break,
        };

        outbound.scheduler_rounds.fetch_add(1, Ordering::Relaxed);
        outbound.outbound_depth.fetch_sub(1, Ordering::AcqRel);
        if let Some(meta) = &frame.meta {
            meta.depth.fetch_sub(1, Ordering::AcqRel);
        }
        if sink.send(frame.text).await.is_err() {
            break;
        }
    }

    let _ = sink.close().await;
}
