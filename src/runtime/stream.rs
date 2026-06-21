//! Stream-side wrappers for peerline's streaming layer.

use super::error::Error;
use super::peer::{PeerInner, send_frame};
use crate::peer as p;
use crate::wire::{Frame, Id, StreamFrame};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

/// Handle a server-side streaming handler uses to push items back
/// to the requesting peer. Construct via [`crate::Peer::on_stream_request`]
/// — the handler receives one of these to send into.
///
/// Owns a monotonic `seq` counter, incremented for every
/// [`Self::send_item`] call (and advanced explicitly via
/// [`Self::skip`] when an upstream source dropped items the sender
/// would otherwise have transmitted). Receivers see `seq` on every
/// `stream:item` frame and can detect dropped items by gaps.
///
/// All methods are sync (just push into a `futures::channel::mpsc::UnboundedSender`);
/// the actual write to the transport happens inside the Peer's
/// outbound drain task.
pub struct StreamSender {
    id: Id,
    state: Arc<PeerInner>,
    /// Next `seq` to stamp on an outgoing item. Starts at `1`.
    /// [`Self::send_item`] does `fetch_add(1)`; [`Self::skip`]
    /// does `fetch_add(n)` without sending — so the next
    /// `send_item` after `skip(n)` produces a `seq` that's `n + 1`
    /// past the previous.
    next_seq: AtomicU64,
    /// `true` until [`Self::close`] / [`Self::error`] is called, at
    /// which point the [`Drop`] impl skips the auto-cancel.
    active: bool,
}

impl StreamSender {
    pub(crate) fn new(id: Id, state: Arc<PeerInner>) -> Self {
        Self {
            id,
            state,
            next_seq: AtomicU64::new(1),
            active: true,
        }
    }

    /// Send an optional `stream:open` ack. Useful when producing the
    /// first item is slow and you want the receiver to know early
    /// that this RPC is a stream.
    pub fn open(&self) -> Result<(), Error> {
        send_frame(&self.state, Frame::Stream(p::stream_open(self.id.clone())))
    }

    /// Send one `stream:item` frame with the next monotonic `seq`.
    /// First call sends `seq = 1`, second `seq = 2`, etc., unless
    /// [`Self::skip`] has advanced the counter.
    pub fn send_item<T: Serialize>(&self, data: &T) -> Result<(), Error> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let item = p::stream_item(self.id.clone(), seq, data)?;
        send_frame(&self.state, Frame::Stream(item))
    }

    /// Advance the `seq` counter by `count` **without** sending an
    /// item. Use when the sender's upstream source dropped `count`
    /// items it would otherwise have transmitted (e.g. a
    /// `broadcast::Receiver` returned `Lagged(count)` because this
    /// sender's subscriber was too slow). The next [`Self::send_item`]
    /// will produce a `seq` that's `count + 1` past the last one,
    /// so receivers see the gap and can recover out of band.
    ///
    /// Sync + non-failing (just bumps an atomic). `count == 0` is a
    /// no-op.
    pub fn skip(&self, count: u64) {
        if count > 0 {
            self.next_seq.fetch_add(count, Ordering::AcqRel);
        }
    }

    /// Close the stream gracefully — sends `stream:close` and disarms
    /// the drop-cancel.
    pub fn close(mut self) -> Result<(), Error> {
        self.active = false;
        send_frame(&self.state, Frame::Stream(p::stream_close(self.id.clone())))
    }

    /// Close the stream with an error — sends `stream:error` and
    /// disarms the drop-cancel.
    pub fn error(mut self, code: i32, message: impl Into<String>) -> Result<(), Error> {
        self.active = false;
        send_frame(
            &self.state,
            Frame::Stream(p::stream_error(self.id.clone(), code, message)),
        )
    }
}

impl Drop for StreamSender {
    fn drop(&mut self) {
        if self.active {
            // Handler dropped without calling close() / error() —
            // send a cancel as the safe default. Best-effort.
            let _ = send_frame(
                &self.state,
                Frame::Stream(p::stream_cancel(self.id.clone())),
            );
        }
    }
}

/// One inbound item from a [`StreamReceiver`] — the typed payload
/// paired with the producer-assigned `seq`. Consumers that don't
/// care about ordering / drop detection can just read `.data`;
/// consumers that do can compare consecutive `.seq` values to
/// detect drops (a jump of more than 1 means the producer advanced
/// past `n` items it didn't send).
#[derive(Debug, Clone)]
pub struct StreamItem<R> {
    /// Producer-assigned monotonic sequence number for this item
    /// within the stream. First item the sender emitted is `1`.
    /// Non-consecutive seqs between items signal dropped items
    /// (see [`StreamSender::skip`]).
    pub seq: u64,
    /// The decoded payload (`R` from `peer.call_stream<_, R>(...)`).
    pub data: R,
}

/// Client-side handle for an inbound stream. Implements
/// [`futures::Stream`] yielding `Result<StreamItem<R>, Error>`. The
/// stream completes (yields `Ready(None)`) when the producer sends
/// `stream:close`; errors propagate as `Err(Error::Rpc(_))` when
/// the producer sends `stream:error`. Dropping this handle sends
/// `stream:cancel` upstream and clears the registry entry.
pub struct StreamReceiver<R> {
    id: Option<Id>,
    rx: mpsc::UnboundedReceiver<StreamFrame>,
    state: Arc<PeerInner>,
    _phantom: PhantomData<R>,
}

impl<R> StreamReceiver<R> {
    pub(crate) fn new(
        id: Id,
        rx: mpsc::UnboundedReceiver<StreamFrame>,
        state: Arc<PeerInner>,
    ) -> Self {
        Self {
            id: Some(id),
            rx,
            state,
            _phantom: PhantomData,
        }
    }
}

impl<R: DeserializeOwned + Unpin> Stream for StreamReceiver<R> {
    type Item = Result<StreamItem<R>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match this.rx.poll_next_unpin(cx) {
                Poll::Ready(Some(StreamFrame::Item { seq, data, .. })) => {
                    return Poll::Ready(Some(
                        serde_json::from_value(data)
                            .map(|d| StreamItem { seq, data: d })
                            .map_err(Error::Serde),
                    ));
                }
                Poll::Ready(Some(StreamFrame::Open { .. })) => {
                    // Open is an early ack; loop and keep polling for
                    // the first real item.
                    continue;
                }
                Poll::Ready(Some(StreamFrame::Close { .. } | StreamFrame::Cancel { .. })) => {
                    // Terminal frame — disarm the drop-cancel and end
                    // the stream.
                    this.id = None;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(StreamFrame::Error { error, .. })) => {
                    this.id = None;
                    return Poll::Ready(Some(Err(Error::Rpc(error))));
                }
                Poll::Ready(None) => {
                    // Channel closed (e.g. dispatch loop ended) — stream done.
                    this.id = None;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<R> Drop for StreamReceiver<R> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            // Stream wasn't drained to its terminal frame — send cancel
            // so the producer knows to stop. Best-effort.
            let _ = send_frame(&self.state, Frame::Stream(p::stream_cancel(id.clone())));
            // Clean up the registry entry. std::sync::Mutex is sync —
            // lock and remove directly. (If lock is poisoned the
            // peer is already torn down, so we silently skip.)
            if let Ok(mut guard) = self.state.streams.lock() {
                guard.remove(&id);
            }
        }
    }
}
