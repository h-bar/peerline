//! The runtime [`Peer`] — call / notify / call_stream / on_request /
//! on_notification / on_stream_request.
//!
//! Runtime-agnostic: uses [`futures::channel`] for channels,
//! [`std::sync::Mutex`] for state (held briefly, never across an
//! await), and [`FuturesUnordered`] to run handlers concurrently
//! within a single task. Construction binds the peer to its
//! transport ([`Peer::new`] takes a sink + stream and returns a
//! driver future); the caller awaits / spawns the driver on
//! whatever executor they prefer — tokio, async-std, smol,
//! `wasm-bindgen-futures`, `futures::executor::block_on`, …

use super::error::{Error, ProtocolError};
use super::outbound::{Outbound, forward_outbound};
use super::stream::{StreamReceiver, StreamSender};
use crate::peer as p;
use crate::peer::{InboundKind, RequestIdGen};
use crate::wire::{
    ErrorType, Frame, Id, Notification, Params, RawJson, Request, Response, RpcError, StreamFrame,
};
use futures::channel::{mpsc, oneshot};
use futures::future::BoxFuture;
use futures::sink::Sink;
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Handler types — boxed async closures registered by method name
// ---------------------------------------------------------------------------

type RequestHandler =
    Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<RawJson, RpcError>> + Send + Sync>;

type NotificationHandler = Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, ()> + Send + Sync>;

type StreamHandler =
    Arc<dyn Fn(serde_json::Value, StreamSender) -> BoxFuture<'static, ()> + Send + Sync>;

type ProtocolErrorHandler = Arc<dyn Fn(ProtocolError) + Send + Sync>;

// ---------------------------------------------------------------------------
// Shared inner state — owned by the Peer, also accessible from spawned tasks
// ---------------------------------------------------------------------------

/// Arc'd inner of [`Peer`]. Holds every piece of state shared
/// between the peer's public handle, the dispatch driver, in-flight
/// handler futures, and the [`StreamSender`] / [`StreamReceiver`]
/// wrappers — so a Peer clone is just an `Arc::clone`.
pub(crate) struct PeerInner {
    ids: RequestIdGen,
    /// Outgoing [`Peer::call`]s awaiting their `resp` frame.
    pub(crate) pending: Mutex<HashMap<Id, oneshot::Sender<Result<RawJson, RpcError>>>>,
    /// Outgoing [`Peer::call_stream`]s awaiting their `stream` frames.
    pub(crate) streams: Mutex<HashMap<Id, mpsc::UnboundedSender<StreamFrame>>>,
    request_handlers: Mutex<HashMap<String, RequestHandler>>,
    notification_handlers: Mutex<HashMap<String, NotificationHandler>>,
    stream_handlers: Mutex<HashMap<String, StreamHandler>>,
    /// Optional observer for frames the dispatch loop could not parse or
    /// route (see [`Peer::on_protocol_error`]). `None` — the default —
    /// discards them exactly as before.
    protocol_error_handler: Mutex<Option<ProtocolErrorHandler>>,
    /// Fair outbound scheduler — control-priority queue plus per-stream
    /// round-robin queues (see [`super::outbound`]). Every outgoing
    /// frame routes through here; [`send_frame`] enqueues control
    /// frames, [`StreamSender`] enqueues stream items.
    pub(crate) outbound: Arc<Outbound>,
    /// In-flight handler futures pushed onto the dispatch loop's
    /// `FuturesUnordered`. Bumped when [`on_inbound_text`] pushes
    /// a new handler; decremented when the handler completes (via
    /// [`InflightGuard`]'s `Drop`). Surfaces in [`Peer::metrics`].
    pub(crate) inflight_handlers: AtomicUsize,
    /// Set once the inbound side has ended — no response can ever
    /// arrive again. `call` / `call_stream` check it *after*
    /// registering, so a request issued concurrently with (or after)
    /// the teardown rejection sweep fails fast instead of waiting on
    /// a reply that can't come. Crucially this lets a handler that
    /// runs during the teardown drain issue a nested call and get an
    /// error back rather than deadlocking the drain.
    closed: AtomicBool,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Snapshot of a [`Peer`]'s current activity. The runtime
/// guarantees the dispatch loop never blocks on consumer
/// slowness (everything routes through unbounded mpsc /
/// `FuturesUnordered`), trading the risk of unbounded memory
/// growth for non-blocking semantics. These counters surface
/// that growth.
///
/// Sample with [`Peer::metrics`] at any cadence; the call is
/// cheap (two short mutex locks + two atomic loads).
#[derive(Debug, Clone, Copy, Default)]
pub struct Metrics {
    /// Outgoing requests this peer has sent but hasn't received a
    /// response for. Grows if the remote is slow / dead / dropping
    /// replies, or if the application leaks call futures.
    pub pending_responses: usize,
    /// Inbound streams open on this peer — `call_stream` requests
    /// whose `StreamReceiver` is still live. Each holds an
    /// unbounded mpsc that grows if the consumer falls behind.
    pub active_streams: usize,
    /// Outbound frames queued waiting for the wire writer to
    /// drain them. Grows when the TCP send side is slow (peer not
    /// reading) — the application's `send_item` / responses keep
    /// returning instantly while bytes pile up here.
    pub outbound_depth: usize,
    /// Handler futures currently in flight inside the dispatch
    /// task's `FuturesUnordered`. Bumped when an inbound request
    /// / notification arrives; decremented when the handler
    /// completes. Grows if handlers take longer to finish than
    /// they arrive.
    pub inflight_handlers: usize,
    /// Largest per-stream outbound queue depth across all active
    /// outbound streams — the single stream furthest behind the
    /// writer. A persistently high value points at one stream
    /// outrunning a slow transport.
    pub stream_items_queued_max: usize,
    /// Sum of queued items across all active outbound stream queues.
    /// Together with `outbound_depth` this separates stream-payload
    /// backlog from control-frame backlog.
    pub stream_items_queued_total: usize,
    /// Cumulative outbound streams cancelled before finishing — by a
    /// consumer dropping its `StreamReceiver` (the reserved cancel
    /// notification closed the producer's queue) or by connection
    /// teardown cancelling every stream still active.
    pub cancelled_streams: u64,
    /// Cumulative frames the outbound scheduler has dispatched to the
    /// transport. A fairness/throughput odometer.
    pub scheduler_rounds: u64,
}

// ---------------------------------------------------------------------------
// Public Peer API
// ---------------------------------------------------------------------------

/// Cheaply-clonable handle on a peer's state. Clone freely across
/// tasks — internally [`Arc`]-shared.
#[derive(Clone)]
pub struct Peer {
    pub(crate) inner: Arc<PeerInner>,
}

impl Peer {
    /// Construct a peer bound to its transport. The peer takes
    /// ownership of both halves: `sink` receives every outgoing
    /// JSON text frame, `stream` yields every inbound one. The
    /// returned driver future, when polled, drives the dispatch
    /// loop (reading from `stream`, running handlers, writing to
    /// `sink`). It resolves when the transport ends: immediately if
    /// the write side errors, or — when the read side ends first —
    /// after draining in-flight handlers and flushing their queued
    /// responses through `sink`. That flush means a sink that blocks
    /// forever without erroring delays resolution; embedders whose
    /// sink can wedge that way should time-bound the driver
    /// themselves.
    ///
    /// The driver is just a future — the caller chooses how to
    /// run it:
    ///
    /// ```ignore
    /// let (peer, driver) = Peer::new(sink, stream);
    /// // option 1 — spawn it on a runtime
    /// tokio::spawn(driver);
    /// // option 2 — co-await it with the rest of the app
    /// futures::join!(driver, other_work());
    /// // option 3 — block the current thread on it
    /// futures::executor::block_on(driver);
    /// ```
    ///
    /// Registering handlers (`on_request` / `on_notification` /
    /// `on_stream_request`) before the driver is polled (awaited /
    /// spawned) is safe — no inbound frame is dispatched until
    /// then.
    pub fn new<Si, St, SiE, StE>(
        sink: Si,
        stream: St,
    ) -> (Self, impl std::future::Future<Output = ()>)
    where
        Si: Sink<String, Error = SiE> + Unpin + 'static,
        St: Stream<Item = Result<String, StE>> + Unpin + 'static,
        SiE: std::fmt::Display + 'static,
        StE: std::fmt::Display + 'static,
    {
        let (outbound, rxs) = Outbound::new();
        let inner = Arc::new(PeerInner {
            ids: RequestIdGen::new(),
            pending: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            request_handlers: Mutex::new(HashMap::new()),
            notification_handlers: Mutex::new(HashMap::new()),
            stream_handlers: Mutex::new(HashMap::new()),
            protocol_error_handler: Mutex::new(None),
            outbound,
            inflight_handlers: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        });
        let peer = Self {
            inner: inner.clone(),
        };
        let driver = async move {
            let writer = Box::pin(forward_outbound(inner.outbound.clone(), rxs, sink));
            let reader = Box::pin(run_inbound(inner.clone(), stream));
            match futures::future::select(reader, writer).await {
                // Reader finished (EOF / transport error): its teardown
                // ran and shut the outbound down, so the writer's
                // sources are all closed — await it to flush the frames
                // still queued (e.g. the last drained handler's
                // response) before resolving.
                futures::future::Either::Left(((), writer)) => writer.await,
                // Writer died first (sink error): the reader was
                // dropped mid-await and never swept its state — sweep
                // here so blocked callers get an error instead of
                // hanging on replies that can never arrive.
                futures::future::Either::Right(((), _reader)) => teardown(&inner),
            }
        };
        (peer, driver)
    }

    /// Snapshot the peer's current activity. Useful for operators
    /// to monitor growth in the runtime's unbounded queues before
    /// they become pathological — the runtime never blocks
    /// dispatch on consumer slowness (everything routes through
    /// unbounded mpscs + `FuturesUnordered`), at the cost of
    /// unbounded memory growth if a consumer falls behind. These
    /// counters surface that growth so callers can alert /
    /// throttle / disconnect before OOM.
    ///
    /// Cheap to call (two mutex locks + two atomic loads); safe
    /// to invoke at any rate.
    #[must_use]
    pub fn metrics(&self) -> Metrics {
        let outbound = &self.inner.outbound;
        let (stream_items_queued_max, stream_items_queued_total) = outbound.stream_depths();
        Metrics {
            pending_responses: self.inner.pending.lock().unwrap().len(),
            active_streams: self.inner.streams.lock().unwrap().len(),
            outbound_depth: outbound.outbound_depth.load(Ordering::Acquire),
            inflight_handlers: self.inner.inflight_handlers.load(Ordering::Acquire),
            stream_items_queued_max,
            stream_items_queued_total,
            cancelled_streams: outbound.cancelled_streams.load(Ordering::Relaxed),
            scheduler_rounds: outbound.scheduler_rounds.load(Ordering::Relaxed),
        }
    }

    // -----------------------------------------------------------------------
    // Outgoing: call / notify / call_stream
    // -----------------------------------------------------------------------

    /// Issue a request and await the typed response.
    pub async fn call<A, R>(&self, op: &str, args: &A) -> Result<R, Error>
    where
        A: Serialize,
        R: DeserializeOwned,
    {
        let id = self.inner.ids.next_id();
        let req = p::request(id, op, args).map_err(|e| Error::Params(e.to_string()))?;

        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().unwrap().insert(id, tx);
        // Registered-then-check: if the teardown sweep already ran, our
        // entry would wait forever — fail fast. If it runs between the
        // insert and this load, the sweep rejects the entry instead;
        // either way the call ends.
        if self.inner.closed.load(Ordering::SeqCst) {
            self.inner.pending.lock().unwrap().remove(&id);
            return Err(Error::Closed);
        }
        if let Err(e) = send_frame(&self.inner, req) {
            self.inner.pending.lock().unwrap().remove(&id);
            return Err(e);
        }

        match rx.await {
            Ok(Ok(raw)) => Ok(raw.deserialize()?),
            Ok(Err(rpc_err)) => Err(Error::Rpc(rpc_err)),
            Err(_) => {
                self.inner.pending.lock().unwrap().remove(&id);
                Err(Error::Abandoned)
            }
        }
    }

    /// Fire-and-forget one-way notification — no `id`, no reply.
    pub fn notify<A: Serialize>(&self, op: &str, args: &A) -> Result<(), Error> {
        let n = p::notification(op, args).map_err(|e| Error::Params(e.to_string()))?;
        send_frame(&self.inner, n)
    }

    /// Issue a streaming request. The returned [`StreamReceiver`]
    /// implements [`futures::Stream<Item = Result<R, Error>>`].
    /// Dropping the receiver sends a `stream:cancel` upstream.
    pub fn call_stream<A, R>(&self, op: &str, args: &A) -> Result<StreamReceiver<R>, Error>
    where
        A: Serialize,
        R: DeserializeOwned + Unpin,
    {
        let id = self.inner.ids.next_id();
        let req = p::request(id, op, args).map_err(|e| Error::Params(e.to_string()))?;

        let (tx, rx) = mpsc::unbounded();
        self.inner.streams.lock().unwrap().insert(id, tx);
        // Same registered-then-check as `call`: an entry inserted after
        // the teardown sweep would never see frames or EOF.
        if self.inner.closed.load(Ordering::SeqCst) {
            self.inner.streams.lock().unwrap().remove(&id);
            return Err(Error::Closed);
        }
        if let Err(e) = send_frame(&self.inner, req) {
            self.inner.streams.lock().unwrap().remove(&id);
            return Err(e);
        }

        Ok(StreamReceiver::new(id, rx, self.inner.clone()))
    }

    // -----------------------------------------------------------------------
    // Inbound: register handlers
    // -----------------------------------------------------------------------

    /// Register a handler for incoming requests on `op`.
    pub fn on_request<A, R, F, Fut>(&self, op: impl Into<String>, f: F)
    where
        A: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(A) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<R, RpcError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: RequestHandler = Arc::new(move |args: serde_json::Value| {
            let f = f.clone();
            Box::pin(async move {
                let a: A = serde_json::from_value(args).map_err(|e| RpcError {
                    code: ErrorType::InvalidParams.into(),
                    message: e.to_string(),
                    data: None,
                })?;
                let r = f(a).await?;
                RawJson::from_serialize(&r).map_err(|e| RpcError {
                    code: ErrorType::Internal.into(),
                    message: e.to_string(),
                    data: None,
                })
            })
        });
        self.inner
            .request_handlers
            .lock()
            .unwrap()
            .insert(op.into(), h);
    }

    /// Register a handler for incoming notifications on `op`.
    /// Notifications produce no reply; errors inside the handler
    /// are not surfaced over the wire.
    pub fn on_notification<A, F, Fut>(&self, op: impl Into<String>, f: F)
    where
        A: DeserializeOwned + Send + 'static,
        F: Fn(A) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: NotificationHandler = Arc::new(move |args: serde_json::Value| {
            let f = f.clone();
            Box::pin(async move {
                let Ok(a) = serde_json::from_value::<A>(args) else {
                    return;
                };
                f(a).await;
            })
        });
        self.inner
            .notification_handlers
            .lock()
            .unwrap()
            .insert(op.into(), h);
    }

    /// Register a streaming-request handler for `op`. The closure
    /// receives the deserialised args plus a [`StreamSender`] for
    /// pushing items. The handler's return value drives the terminal
    /// frame: `Ok(())` sends an empty terminal, `Err(rpc_err)` sends
    /// an error terminal. A safety-net terminal is sent if the
    /// handler future drops before returning.
    pub fn on_stream_request<A, F, Fut>(&self, op: impl Into<String>, f: F)
    where
        A: DeserializeOwned + Send + 'static,
        F: Fn(A, StreamSender) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<(), RpcError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: StreamHandler = Arc::new(move |args: serde_json::Value, sender: StreamSender| {
            let f = f.clone();
            let id = sender.id;
            let outbound = sender.outbound.clone();
            Box::pin(async move {
                // Guard sends an empty terminal if the future drops
                // (panic, executor cancel) before reaching the
                // explicit terminal in the success / error branches.
                let mut guard = super::stream::TerminalGuard::new(id, outbound);
                match serde_json::from_value::<A>(args) {
                    Ok(a) => match f(a, sender).await {
                        Ok(()) => guard.send_normal(),
                        Err(e) => guard.send_error(e),
                    },
                    Err(_) => guard.send_error(RpcError {
                        code: ErrorType::InvalidParams.into(),
                        message: "invalid args for stream request".into(),
                        data: None,
                    }),
                }
            })
        });
        self.inner
            .stream_handlers
            .lock()
            .unwrap()
            .insert(op.into(), h);
    }

    /// Observe frames the dispatch loop could not parse or could not
    /// route to a waiting caller — see [`ProtocolError`]. These have no
    /// caller to be returned to, so by default they are discarded
    /// silently; register a hook to log or count them.
    ///
    /// The closure runs **inline on the dispatch loop**, so it must be
    /// cheap and must not block: do the formatting, hand the value to a
    /// channel, or bump a counter. Registering replaces any previous
    /// hook.
    pub fn on_protocol_error<F>(&self, f: F)
    where
        F: Fn(ProtocolError) + Send + Sync + 'static,
    {
        *self.inner.protocol_error_handler.lock().unwrap() = Some(Arc::new(f));
    }
}

// ---------------------------------------------------------------------------
// Dispatch loop — free fns owned by the driver future, not the Peer handle
// ---------------------------------------------------------------------------

/// Read JSON text frames from `inbound` forever, dispatching each
/// to the appropriate pending oneshot / handler / stream channel.
/// Handlers run concurrently within this task via
/// [`FuturesUnordered`] — no external runtime spawn required.
///
/// Returns when `inbound` ends — at which point every pending
/// request is rejected with an internal "connection closed" error
/// and all stream receivers see EOF.
async fn run_inbound<S, E>(inner: Arc<PeerInner>, inbound: S)
where
    S: Stream<Item = Result<String, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut inbound = inbound.fuse();
    let mut handler_tasks: FuturesUnordered<BoxFuture<'static, Option<Response>>> =
        FuturesUnordered::new();

    let transport_error = loop {
        futures::select! {
            // New inbound frame
            item = inbound.next() => match item {
                Some(Ok(text)) => on_inbound_text(&inner, &text, &mut handler_tasks),
                // Clean EOF (peer closed) vs a transport/decode error.
                None => break false,
                Some(Err(_)) => break true,
            },
            // A handler finished — send its response back if it produced one
            done = handler_tasks.select_next_some() => {
                if let Some(response) = done {
                    let _ = send_frame(&inner, response);
                }
            }
        }
    };

    // Sweep caller-facing state before draining handlers: a draining
    // handler that's awaiting a nested call (or stream) thereby
    // observes the failure and finishes, instead of deadlocking the
    // drain on a reply that can never arrive.
    teardown(&inner);

    // On a clean half-close the writer is still up, so drain in-flight
    // handlers to flush their responses. On a transport error the writer is
    // dead — responses can't be delivered — so skip the drain; dropping the
    // handler tasks (with the teardown cancel above) releases them promptly.
    if !transport_error {
        while let Some(done) = handler_tasks.next().await {
            if let Some(response) = done {
                let _ = send_frame(&inner, response);
            }
        }
    }

    // Close the writer's inputs so it drains what's queued (including
    // the responses just enqueued above) and terminates — the driver
    // awaits it to flush before resolving.
    inner.outbound.shutdown();
}

/// Sweep the peer's caller-facing state once the connection can no
/// longer deliver replies: wake every active outbound stream's
/// `cancelled()` (so parked handlers finish), mark the peer closed
/// (so a `call` / `call_stream` issued afterwards fails fast instead
/// of re-registering into the swept maps), reject every pending call,
/// and end every inbound stream receiver. Idempotent — runs from the
/// reader's normal teardown and again from the driver if the writer
/// dies first (in which case the reader was dropped mid-await and
/// never got here).
fn teardown(inner: &Arc<PeerInner>) {
    inner.outbound.cancel_all();
    inner.closed.store(true, Ordering::SeqCst);
    let pending: Vec<_> = inner.pending.lock().unwrap().drain().collect();
    for (_id, tx) in pending {
        let _ = tx.send(Err(RpcError {
            code: ErrorType::Internal.into(),
            message: "connection closed".into(),
            data: None,
        }));
    }
    // Dropping the senders ends each receiver's stream, which is how a
    // `call_stream` consumer sees the connection go away.
    inner.streams.lock().unwrap().clear();
}

fn on_inbound_text(
    inner: &Arc<PeerInner>,
    text: &str,
    handler_tasks: &mut FuturesUnordered<BoxFuture<'static, Option<Response>>>,
) {
    let frame = match p::parse_frame(text) {
        Ok(f) => f,
        Err(parse_err_response) => {
            report(
                inner,
                ProtocolError::MalformedFrame {
                    message: parse_err_response
                        .error()
                        .map_or_else(String::new, |e| e.message.clone()),
                },
            );
            let _ = send_frame(inner, parse_err_response);
            return;
        }
    };
    match p::classify(frame) {
        InboundKind::Response { id, outcome } => {
            let Some(id) = id else {
                // `id: null` — the remote couldn't recover the id of the
                // request it is answering, so neither can we. Nothing to
                // resolve; surface it rather than drop it silently.
                report(
                    inner,
                    ProtocolError::UncorrelatedResponse {
                        error: outcome.err(),
                    },
                );
                return;
            };
            if let Some(tx) = inner.pending.lock().unwrap().remove(&id) {
                let _ = tx.send(outcome);
                return;
            }
            // Not a unary waiter — so this id was declared by
            // `call_stream`, and the remote answered it as a unary
            // request: its handler for the op is unary, or there is none
            // and this is the `MethodNotFound` reply. Deliver the outcome
            // as an error terminal so the receiver ends, rather than
            // waiting for stream frames that will never come.
            //
            // `remove`, not `get`: a unary reply is final, so nothing can
            // legitimately follow this id. Taking the sender out means it
            // drops once the terminal is queued — the receiver drains
            // that frame and then sees the channel end — and a duplicate
            // reply falls through to `UnroutableFrame` instead of
            // yielding a second `Err` past the terminal.
            if let Some(tx) = inner.streams.lock().unwrap().remove(&id) {
                let _ = tx.unbounded_send(p::stream_terminal_with_rpc_error(
                    id,
                    unary_reply_to_stream_error(outcome),
                ));
                return;
            }
            report(inner, ProtocolError::UnroutableFrame { id });
        }
        InboundKind::IncomingRequest(req) => {
            let inner = inner.clone();
            let guard = InflightGuard::new(inner.clone());
            handler_tasks.push(Box::pin(async move {
                let _g = guard;
                process_request(inner, req).await
            }));
        }
        InboundKind::IncomingNotification(notif) => {
            // Reserved cancel notification is intercepted here, before
            // user notification handlers: route it to the outbound
            // scheduler to close the producing stream's queue (its
            // handler's next send then fails). Never dispatched to a
            // user handler.
            if notif.op == super::STREAM_CANCEL_OP {
                if let Some(id) = notif
                    .args
                    .as_ref()
                    .and_then(|a| a.get("id"))
                    .and_then(serde_json::Value::as_u64)
                {
                    inner.outbound.cancel_stream(id);
                }
                return;
            }
            let inner = inner.clone();
            let guard = InflightGuard::new(inner.clone());
            handler_tasks.push(Box::pin(async move {
                let _g = guard;
                process_notification(inner, notif).await;
                None
            }));
        }
        InboundKind::Stream(sf) => {
            let id = *sf.id();
            let tx_opt = inner.streams.lock().unwrap().get(&id).cloned();
            if let Some(tx) = tx_opt {
                let _ = tx.unbounded_send(sf);
                return;
            }
            // The mirror case: this id was declared by `call`, but the
            // remote handles the op as a stream. A unary waiter can only
            // take one value, so fail it — forwarding the stream's own
            // error when it is ending in one, since that says more than
            // the shape complaint would. Later frames of the same stream
            // find no waiter and report as unroutable.
            if let Some(tx) = inner.pending.lock().unwrap().remove(&id) {
                let _ = tx.send(Err(sf.error.unwrap_or_else(|| {
                    violation("responder answered with a stream; use call_stream")
                })));
                return;
            }
            report(inner, ProtocolError::UnroutableFrame { id });
        }
    }
}

/// Turn a unary reply that arrived for a `call_stream` id into the error
/// its terminal frame carries. An error reply is forwarded verbatim —
/// `MethodNotFound` is the actual cause and reads far better than a
/// shape complaint. A *successful* unary reply means the op exists but
/// is unary, which is a contract violation rather than a one-item
/// stream: coercing it would present a shape the caller never declared
/// and put a coercion into the cross-language contract that nothing on
/// the wire describes.
fn unary_reply_to_stream_error(outcome: Result<RawJson, RpcError>) -> RpcError {
    outcome
        .err()
        .unwrap_or_else(|| violation("responder answered with a unary response; use call"))
}

/// The error a reply gets when its shape contradicts what the caller
/// declared. `Internal` because the fault is in the peers' agreement
/// about the op, not in the request the caller sent.
fn violation(message: &str) -> RpcError {
    RpcError {
        code: ErrorType::Internal.into(),
        message: message.to_owned(),
        data: None,
    }
}

/// Hand a [`ProtocolError`] to the peer's hook, if one is registered.
/// The hook is cloned out from under the lock so a slow (or reentrant)
/// observer can't hold the registry.
fn report(inner: &Arc<PeerInner>, err: ProtocolError) {
    let handler = inner.protocol_error_handler.lock().unwrap().clone();
    if let Some(handler) = handler {
        handler(err);
    }
}

/// RAII counter for `inner.inflight_handlers`. Bumps on
/// construction, decrements on drop — so a handler future that
/// completes, is cancelled, or panics all decrement the same way.
struct InflightGuard(Arc<PeerInner>);

impl InflightGuard {
    fn new(inner: Arc<PeerInner>) -> Self {
        inner.inflight_handlers.fetch_add(1, Ordering::AcqRel);
        Self(inner)
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.inflight_handlers.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// Handler dispatch — free fns so they can be pushed into FuturesUnordered
// ---------------------------------------------------------------------------

async fn process_request(inner: Arc<PeerInner>, req: Request) -> Option<Response> {
    let id = req.id;
    let args_value = args_into_value(req.args);

    // Try streaming handler first — if present, it gets a StreamSender
    // and produces no Response (the stream:* frames carry the reply).
    let stream_handler = inner.stream_handlers.lock().unwrap().get(&req.op).cloned();
    if let Some(handler) = stream_handler {
        let sender = StreamSender::new(id, inner.clone());
        handler(args_value, sender).await;
        return None;
    }

    // Unary request handler — returns Some(Response).
    let unary_handler = inner.request_handlers.lock().unwrap().get(&req.op).cloned();
    let response = match unary_handler {
        Some(handler) => match handler(args_value).await {
            Ok(raw) => p::response_ok_raw(id, raw),
            // Forward the handler's full error — including its `data`
            // payload, which the wire supports and consumers may rely on.
            Err(rpc_err) => p::response_err_rpc(Some(id), rpc_err),
        },
        None => p::response_err(
            Some(id),
            ErrorType::MethodNotFound,
            format!("op not found: {}", req.op),
        ),
    };
    Some(response)
}

async fn process_notification(inner: Arc<PeerInner>, notif: Notification) {
    let handler = inner
        .notification_handlers
        .lock()
        .unwrap()
        .get(&notif.op)
        .cloned();
    if let Some(handler) = handler {
        let args_value = args_into_value(notif.args);
        handler(args_value).await;
    }
    // No reply for notifications, even if no handler is registered.
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Serialize a frame and enqueue it on the scheduler's priority
/// control queue (see [`super::outbound`]). Used for responses,
/// notifications, this peer's own requests, and cancel notifications —
/// everything except stream item frames, which route through the
/// per-stream queues via [`StreamSender`]. The scheduler's writer
/// decrements the queue depth as it dequeues, just before the wire
/// write.
pub(crate) fn send_frame<F: Into<Frame>>(inner: &PeerInner, frame: F) -> Result<(), Error> {
    let text = serde_json::to_string(&frame.into())?;
    inner.outbound.enqueue_control(text)
}

fn args_into_value(args: Option<Params>) -> serde_json::Value {
    args.map(serde_json::Value::Object)
        .unwrap_or(serde_json::Value::Null)
}
