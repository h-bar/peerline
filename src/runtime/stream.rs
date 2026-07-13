//! Stream-side wrappers for peerline's streaming layer.

use super::error::Error;
use super::outbound::{Outbound, StreamMeta};
use super::peer::{PeerInner, send_frame};
use crate::peer as p;
use crate::wire::{Frame, Id, RpcError, STREAM_TERMINAL_SEQ, StreamFrame};
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

/// Args for the reserved stream-cancel notification (see
/// [`super::STREAM_CANCEL_OP`]) — carries just the stream id.
#[derive(Serialize)]
struct CancelArgs {
    id: Id,
}

// ---------------------------------------------------------------------------
// StreamSender — server-side handle for pushing items
// ---------------------------------------------------------------------------

/// Handle a server-side streaming handler uses to push items back
/// to the requesting peer. Construct via [`crate::Peer::on_stream_request`]
/// — the handler receives one of these to send into.
///
/// Owns a monotonic `seq` counter, incremented for every
/// [`Self::send_item`] call (and advanced explicitly via
/// [`Self::skip`] when an upstream source dropped items the sender
/// would otherwise have transmitted). Receivers see `seq` on every
/// item frame and can detect dropped items by gaps.
///
/// The handler does **not** explicitly close or error the stream —
/// the stream's terminal frame is sent automatically by the runtime
/// based on the handler's return value (`Ok(())` for normal end,
/// `Err(rpc_err)` for abnormal end). See [`crate::Peer::on_stream_request`].
pub struct StreamSender {
    pub(crate) id: Id,
    /// Shared scheduler — used for depth accounting and (by the runtime
    /// wrapper) the terminal frame via [`Outbound::finish_stream`].
    pub(crate) outbound: Arc<Outbound>,
    /// Producer half of this stream's outbound channel.
    tx: mpsc::UnboundedSender<String>,
    /// Shared depth counter for this stream (observability).
    meta: Arc<StreamMeta>,
    /// Next `seq` to stamp on an outgoing item. Items are 0-indexed —
    /// the first send stamps `seq = 0` (which implicitly opens the
    /// stream), the second `seq = 1`, etc.
    next_seq: AtomicU64,
}

impl StreamSender {
    pub(crate) fn new(id: Id, state: Arc<PeerInner>) -> Self {
        let outbound = state.outbound.clone();
        let (tx, meta) = outbound.open_stream(id);
        Self {
            id,
            outbound,
            tx,
            meta,
            next_seq: AtomicU64::new(0),
        }
    }

    /// Send one stream-item frame with the next monotonic `seq`.
    ///
    /// Non-blocking: the item is enqueued on this stream's outbound
    /// queue and the call returns immediately — a slow or dead consumer
    /// never blocks the producer. The queue is unbounded, so this only
    /// fails ([`Error::Closed`]) once the stream is cancelled (consumer
    /// dropped its receiver) or the connection closes. Backlog from a
    /// slow consumer is allowed to grow and is surfaced via
    /// [`super::Peer::metrics`] rather than throttled.
    ///
    /// First call stamps `seq = 0` (implicitly opens the stream), then
    /// `seq = 1`, etc., unless [`Self::skip`] has advanced the counter.
    pub fn send_item<T: Serialize>(&self, data: &T) -> Result<(), Error> {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let text = serde_json::to_string(&Frame::from(p::stream_item(self.id, seq, data)?))?;
        self.meta.depth.fetch_add(1, Ordering::AcqRel);
        self.outbound.outbound_depth.fetch_add(1, Ordering::AcqRel);
        self.tx.unbounded_send(text).map_err(|_| {
            self.meta.depth.fetch_sub(1, Ordering::AcqRel);
            self.outbound.outbound_depth.fetch_sub(1, Ordering::AcqRel);
            Error::Closed
        })
    }

    /// Advance the `seq` counter by `count` **without** sending an
    /// item. Use when the sender's upstream source dropped `count`
    /// items it would otherwise have transmitted (e.g. a
    /// `broadcast::Receiver` returned `Lagged(count)` because this
    /// sender's subscriber was too slow). The next send will produce a
    /// `seq` that's `count + 1` past the last one, so receivers see the
    /// gap and can recover out of band.
    ///
    /// Sync + non-failing (just bumps an atomic). `count == 0` is a
    /// no-op.
    pub fn skip(&self, count: u64) {
        if count > 0 {
            self.next_seq.fetch_add(count, Ordering::AcqRel);
        }
    }

    /// A future that resolves once the consumer cancels this stream
    /// (drops its [`StreamReceiver`]). Handlers doing expensive
    /// upstream work can `select!` on this to stop promptly, instead of
    /// only discovering the cancellation on their next
    /// [`Self::send_item`] (which returns [`Error::Closed`]):
    ///
    /// ```ignore
    /// loop {
    ///     tokio::select! {
    ///         _ = sender.cancelled() => break,          // consumer left
    ///         row = db.next() => sender.send_item(&row)?,
    ///     }
    /// }
    /// ```
    ///
    /// Resolves immediately if the stream is already cancelled. A
    /// [`StreamSender`] serves a single handler, so this is intended to
    /// be awaited from one place at a time.
    pub fn cancelled(&self) -> impl std::future::Future<Output = ()> + '_ {
        std::future::poll_fn(move |cx| {
            if self.meta.cancelled.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            self.meta.cancel_waker.register(cx.waker());
            // Re-check to close the race with a cancel landing between
            // the load above and the waker registration.
            if self.meta.cancelled.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    /// `true` if the consumer has cancelled this stream (dropped its
    /// receiver). Non-blocking snapshot; see [`Self::cancelled`] for an
    /// awaitable version.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.meta.cancelled.load(Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// TerminalGuard — RAII helper used by the runtime's stream-handler wrapper
// ---------------------------------------------------------------------------

/// Sends the terminal `seq == -1` frame for a stream handler. The
/// handler wrapper constructs one of these and uses [`Self::send_normal`]
/// or [`Self::send_error`] when the handler returns. If the handler
/// future is dropped before reaching the explicit terminal (panic,
/// executor cancellation, etc.), [`Drop`] fires a safety-net empty
/// terminal so the consumer doesn't hang.
pub(crate) struct TerminalGuard {
    id: Id,
    outbound: Arc<Outbound>,
    sent: bool,
}

impl TerminalGuard {
    pub(crate) fn new(id: Id, outbound: Arc<Outbound>) -> Self {
        Self {
            id,
            outbound,
            sent: false,
        }
    }

    pub(crate) fn send_normal(&mut self) {
        if !self.sent {
            self.sent = true;
            self.outbound
                .finish_stream(self.id, terminal_text(p::stream_terminal(self.id)));
        }
    }

    pub(crate) fn send_error(&mut self, error: RpcError) {
        if !self.sent {
            self.sent = true;
            self.outbound.finish_stream(
                self.id,
                terminal_text(p::stream_terminal_with_error(
                    self.id,
                    error.code,
                    error.message,
                )),
            );
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if !self.sent {
            // Handler future dropped without returning — send a plain
            // empty terminal so the consumer knows the stream is over.
            self.sent = true;
            self.outbound
                .finish_stream(self.id, terminal_text(p::stream_terminal(self.id)));
        }
    }
}

/// Serialize a terminal [`StreamFrame`] into a wire string. Terminal
/// frames carry no user payload, so serialization is infallible in
/// practice; on the impossible error we fall back to an empty string,
/// which `finish_stream` still delivers (and the peer would surface as
/// a parse error rather than a silent hang).
fn terminal_text(frame: StreamFrame) -> String {
    serde_json::to_string(&Frame::from(frame)).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// StreamItem — typed payload from the consumer's stream
// ---------------------------------------------------------------------------

/// One inbound item from a [`StreamReceiver`] — the typed payload
/// paired with the producer-assigned `seq`. Consumers that don't
/// care about ordering / drop detection can just read `.data`;
/// consumers that do can compare consecutive `.seq` values to
/// detect drops.
#[derive(Debug, Clone)]
pub struct StreamItem<R> {
    /// Producer-assigned sequence number. `>= 1` for items received
    /// before the terminal, possibly `-1` if the terminal carried a
    /// bundled last item.
    pub seq: i64,
    /// The decoded payload.
    pub data: R,
}

// ---------------------------------------------------------------------------
// StreamReceiver — client-side handle for receiving items
// ---------------------------------------------------------------------------

/// Client-side handle for an inbound stream. Implements
/// [`futures::Stream`] yielding `Result<StreamItem<R>, Error>`. The
/// stream completes (yields `Ready(None)`) when the producer sends
/// a terminal frame with no error; if the terminal carries an error
/// the receiver yields one final `Err(Error::Rpc(_))` before
/// completing.
///
/// Dropping this handle is purely local — the producer is not
/// notified. The producer keeps producing; the dispatch loop
/// silently discards frames for any stream id not in the registry.
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
                Poll::Ready(Some(frame)) => {
                    let is_terminal = frame.seq == STREAM_TERMINAL_SEQ || frame.error.is_some();
                    // Error always wins — yield it and end the stream.
                    if let Some(error) = frame.error {
                        this.id = None;
                        return Poll::Ready(Some(Err(Error::Rpc(error))));
                    }
                    // If the frame carries data, yield it. The terminal
                    // frame may carry a bundled last item. The payload was
                    // captured raw on the wire; deserialize it once here.
                    if let Some(data) = frame.data {
                        let item = match data.deserialize() {
                            Ok(d) => StreamItem {
                                seq: frame.seq,
                                data: d,
                            },
                            Err(e) => {
                                this.id = None;
                                return Poll::Ready(Some(Err(Error::Serde(e))));
                            }
                        };
                        if is_terminal {
                            this.id = None;
                        }
                        return Poll::Ready(Some(Ok(item)));
                    }
                    // Empty terminal (seq=-1, no data, no error) — end.
                    if is_terminal {
                        this.id = None;
                        return Poll::Ready(None);
                    }
                    // No data, no error, not terminal — odd frame; skip
                    // and poll again.
                    continue;
                }
                Poll::Ready(None) => {
                    // Channel closed (dispatch loop ended) — stream done.
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
            // Real cancel-on-drop: tell the producing peer to stop. The
            // reserved cancel notification is intercepted on the far
            // side before user handlers and closes that stream's
            // outbound queue, so its handler's next send fails. Any
            // frames still in flight for this id are discarded locally
            // (no registry entry left to route to).
            if let Ok(notif) = p::notification(super::STREAM_CANCEL_OP, &CancelArgs { id }) {
                let _ = send_frame(&self.state, notif);
            }
            if let Ok(mut guard) = self.state.streams.lock() {
                guard.remove(&id);
            }
        }
    }
}
