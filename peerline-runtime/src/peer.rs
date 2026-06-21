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

use crate::error::Error;
use crate::stream::{StreamReceiver, StreamSender};
use futures::channel::{mpsc, oneshot};
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use peerline::peer as p;
use peerline::peer::{InboundKind, RequestIdGen};
use peerline::wire::{
    ERR_INTERNAL, ERR_INVALID_PARAMS, ERR_METHOD_NOT_FOUND, Frame, Id, Notification, Params,
    Request, Response, RpcError, StreamFrame,
};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Handler types — boxed async closures registered by method name
// ---------------------------------------------------------------------------

type RequestHandler = Arc<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<serde_json::Value, RpcError>>
        + Send
        + Sync,
>;

type NotificationHandler = Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, ()> + Send + Sync>;

type StreamHandler =
    Arc<dyn Fn(serde_json::Value, StreamSender) -> BoxFuture<'static, ()> + Send + Sync>;

// ---------------------------------------------------------------------------
// Shared inner state — owned by the Peer, also accessible from spawned tasks
// ---------------------------------------------------------------------------

/// Arc'd inner of [`Peer`]. Holds every piece of state shared
/// between the peer's public handle, the dispatch driver, in-flight
/// handler futures, and the [`StreamSender`] / [`StreamReceiver`]
/// wrappers — so a Peer clone is just an `Arc::clone`.
pub(crate) struct PeerInner {
    ids: RequestIdGen,
    pub(crate) pending: Mutex<HashMap<Id, oneshot::Sender<Result<serde_json::Value, RpcError>>>>,
    pub(crate) streams: Mutex<HashMap<Id, mpsc::UnboundedSender<StreamFrame>>>,
    request_handlers: Mutex<HashMap<String, RequestHandler>>,
    notification_handlers: Mutex<HashMap<String, NotificationHandler>>,
    stream_handlers: Mutex<HashMap<String, StreamHandler>>,
    pub(crate) outbound: mpsc::UnboundedSender<String>,
    /// Number of frames queued in `outbound` that haven't been
    /// drained by the wire writer yet. Bumped by [`send_frame`] /
    /// [`crate::stream`]'s send paths; decremented by
    /// [`forward_outbound`]. Surfaces in [`Peer::metrics`] so
    /// operators can spot a slow / dead client (TCP send blocked)
    /// before the unbounded queue eats memory.
    pub(crate) outbound_depth: AtomicUsize,
    /// In-flight handler futures pushed onto the dispatch loop's
    /// `FuturesUnordered`. Bumped when [`on_inbound_text`] pushes
    /// a new handler; decremented when the handler completes (via
    /// [`InflightGuard`]'s `Drop`). Surfaces in [`Peer::metrics`].
    pub(crate) inflight_handlers: AtomicUsize,
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
    /// `sink`); it resolves when either half ends (transport
    /// closes, peer is dropped, etc.).
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
        let (out_tx, out_rx) = mpsc::unbounded();
        let inner = Arc::new(PeerInner {
            ids: RequestIdGen::new(),
            pending: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            request_handlers: Mutex::new(HashMap::new()),
            notification_handlers: Mutex::new(HashMap::new()),
            stream_handlers: Mutex::new(HashMap::new()),
            outbound: out_tx,
            outbound_depth: AtomicUsize::new(0),
            inflight_handlers: AtomicUsize::new(0),
        });
        let peer = Self {
            inner: inner.clone(),
        };
        let driver = async move {
            let writer = Box::pin(forward_outbound(out_rx, inner.clone(), sink));
            let reader = Box::pin(run_inbound(inner, stream));
            // Exit when either side ends; the other future is dropped.
            let _ = futures::future::select(writer, reader).await;
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
        Metrics {
            pending_responses: self.inner.pending.lock().unwrap().len(),
            active_streams: self.inner.streams.lock().unwrap().len(),
            outbound_depth: self.inner.outbound_depth.load(Ordering::Acquire),
            inflight_handlers: self.inner.inflight_handlers.load(Ordering::Acquire),
        }
    }

    // -----------------------------------------------------------------------
    // Outgoing: call / notify / call_stream
    // -----------------------------------------------------------------------

    /// Issue a request and await the typed response.
    pub async fn call<P, R>(&self, method: &str, params: &P) -> Result<R, Error>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let id = self.inner.ids.next_id();
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().unwrap().insert(id.clone(), tx);

        let req =
            p::request(id.clone(), method, params).map_err(|e| Error::Params(e.to_string()))?;
        send_frame(&self.inner, Frame::Request(req))?;

        match rx.await {
            Ok(Ok(value)) => Ok(serde_json::from_value(value)?),
            Ok(Err(rpc_err)) => Err(Error::Rpc(rpc_err)),
            Err(_) => {
                self.inner.pending.lock().unwrap().remove(&id);
                Err(Error::Abandoned)
            }
        }
    }

    /// Fire-and-forget one-way notification — no `id`, no reply.
    pub fn notify<P: Serialize>(&self, method: &str, params: &P) -> Result<(), Error> {
        let n = p::notification(method, params).map_err(|e| Error::Params(e.to_string()))?;
        send_frame(&self.inner, Frame::Notification(n))
    }

    /// Issue a streaming request. The returned [`StreamReceiver`]
    /// implements [`futures::Stream<Item = Result<R, Error>>`].
    /// Dropping the receiver sends a `stream:cancel` upstream.
    pub fn call_stream<P, R>(&self, method: &str, params: &P) -> Result<StreamReceiver<R>, Error>
    where
        P: Serialize,
        R: DeserializeOwned + Unpin,
    {
        let id = self.inner.ids.next_id();
        let (tx, rx) = mpsc::unbounded();
        self.inner.streams.lock().unwrap().insert(id.clone(), tx);

        let req =
            p::request(id.clone(), method, params).map_err(|e| Error::Params(e.to_string()))?;
        send_frame(&self.inner, Frame::Request(req))?;

        Ok(StreamReceiver::new(id, rx, self.inner.clone()))
    }

    // -----------------------------------------------------------------------
    // Inbound: register handlers
    // -----------------------------------------------------------------------

    /// Register a handler for incoming requests on `method`.
    pub fn on_request<P, R, F, Fut>(&self, method: impl Into<String>, f: F)
    where
        P: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(P) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<R, RpcError>> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: RequestHandler = Arc::new(move |params: serde_json::Value| {
            let f = f.clone();
            Box::pin(async move {
                let p: P = serde_json::from_value(params).map_err(|e| RpcError {
                    code: ERR_INVALID_PARAMS,
                    message: e.to_string(),
                    data: None,
                })?;
                let r = f(p).await?;
                serde_json::to_value(r).map_err(|e| RpcError {
                    code: ERR_INTERNAL,
                    message: e.to_string(),
                    data: None,
                })
            })
        });
        self.inner
            .request_handlers
            .lock()
            .unwrap()
            .insert(method.into(), h);
    }

    /// Register a handler for incoming notifications on `method`.
    /// Notifications produce no reply; errors inside the handler
    /// are not surfaced over the wire.
    pub fn on_notification<P, F, Fut>(&self, method: impl Into<String>, f: F)
    where
        P: DeserializeOwned + Send + 'static,
        F: Fn(P) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: NotificationHandler = Arc::new(move |params: serde_json::Value| {
            let f = f.clone();
            Box::pin(async move {
                let Ok(p) = serde_json::from_value::<P>(params) else {
                    return;
                };
                f(p).await;
            })
        });
        self.inner
            .notification_handlers
            .lock()
            .unwrap()
            .insert(method.into(), h);
    }

    /// Register a streaming-request handler for `method`. The
    /// closure receives the deserialised params plus a
    /// [`StreamSender`] for pushing items / closing / erroring.
    pub fn on_stream_request<P, F, Fut>(&self, method: impl Into<String>, f: F)
    where
        P: DeserializeOwned + Send + 'static,
        F: Fn(P, StreamSender) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let f = Arc::new(f);
        let h: StreamHandler = Arc::new(move |params: serde_json::Value, sender: StreamSender| {
            let f = f.clone();
            Box::pin(async move {
                let Ok(p) = serde_json::from_value::<P>(params) else {
                    let _ = sender.error(ERR_INVALID_PARAMS, "invalid params for stream request");
                    return;
                };
                f(p, sender).await;
            })
        });
        self.inner
            .stream_handlers
            .lock()
            .unwrap()
            .insert(method.into(), h);
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
/// Returns when `inbound` ends — at which point all pending
/// requests are abandoned and all stream receivers see EOF.
async fn run_inbound<S, E>(inner: Arc<PeerInner>, inbound: S)
where
    S: Stream<Item = Result<String, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut inbound = inbound.fuse();
    let mut handler_tasks: FuturesUnordered<BoxFuture<'static, Option<Response>>> =
        FuturesUnordered::new();

    loop {
        futures::select! {
            // New inbound frame
            item = inbound.next() => match item {
                Some(Ok(text)) => on_inbound_text(&inner, &text, &mut handler_tasks),
                Some(Err(_)) | None => break,
            },
            // A handler finished — send its response back if it produced one
            done = handler_tasks.select_next_some() => {
                if let Some(response) = done {
                    let _ = send_frame(&inner, Frame::Response(response));
                }
            }
        }
    }

    // Drain any still-running handlers so their responses get sent.
    while let Some(done) = handler_tasks.next().await {
        if let Some(response) = done {
            let _ = send_frame(&inner, Frame::Response(response));
        }
    }

    // Connection closed — wake every blocked caller.
    let pending: Vec<_> = inner.pending.lock().unwrap().drain().collect();
    for (_id, tx) in pending {
        let _ = tx.send(Err(RpcError {
            code: ERR_INTERNAL,
            message: "connection closed".into(),
            data: None,
        }));
    }
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
            let _ = send_frame(inner, Frame::Response(parse_err_response));
            return;
        }
    };
    match p::classify(frame) {
        InboundKind::Response { id, outcome } => {
            if let Some(id) = id
                && let Some(tx) = inner.pending.lock().unwrap().remove(&id)
            {
                let _ = tx.send(outcome);
            }
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
            let inner = inner.clone();
            let guard = InflightGuard::new(inner.clone());
            handler_tasks.push(Box::pin(async move {
                let _g = guard;
                process_notification(inner, notif).await;
                None
            }));
        }
        InboundKind::Stream(sf) => {
            let tx_opt = inner.streams.lock().unwrap().get(sf.id()).cloned();
            if let Some(tx) = tx_opt {
                let _ = tx.unbounded_send(sf);
            }
        }
        // InboundKind is #[non_exhaustive]; forward-compat for
        // any future variants the underlying crate may add.
        _ => {}
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
    let id = req.id.clone();
    let params_value = params_into_value(req.params);

    // Try streaming handler first — if present, it gets a StreamSender
    // and produces no Response (the stream:* frames carry the reply).
    let stream_handler = inner
        .stream_handlers
        .lock()
        .unwrap()
        .get(&req.method)
        .cloned();
    if let Some(handler) = stream_handler {
        let sender = StreamSender::new(id, inner.clone());
        handler(params_value, sender).await;
        return None;
    }

    // Unary request handler — returns Some(Response).
    let unary_handler = inner
        .request_handlers
        .lock()
        .unwrap()
        .get(&req.method)
        .cloned();
    let response = match unary_handler {
        Some(handler) => match handler(params_value).await {
            Ok(value) => p::response_ok_value(id, value),
            Err(rpc_err) => p::response_err(Some(id), rpc_err.code, rpc_err.message),
        },
        None => p::response_err(
            Some(id),
            ERR_METHOD_NOT_FOUND,
            format!("method not found: {}", req.method),
        ),
    };
    Some(response)
}

async fn process_notification(inner: Arc<PeerInner>, notif: Notification) {
    let handler = inner
        .notification_handlers
        .lock()
        .unwrap()
        .get(&notif.method)
        .cloned();
    if let Some(handler) = handler {
        let params_value = params_into_value(notif.params);
        handler(params_value).await;
    }
    // No reply for notifications, even if no handler is registered.
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Serialize a frame and enqueue it on `inner.outbound`. Bumps
/// `inner.outbound_depth` so [`Peer::metrics`] reflects the
/// queued frame; [`forward_outbound`] decrements after the wire
/// write succeeds. On a closed-channel error the bump is
/// rolled back so the counter stays accurate.
pub(crate) fn send_frame(inner: &PeerInner, frame: Frame) -> Result<(), Error> {
    let text = serde_json::to_string(&frame)?;
    inner.outbound_depth.fetch_add(1, Ordering::AcqRel);
    inner.outbound.unbounded_send(text).map_err(|_| {
        inner.outbound_depth.fetch_sub(1, Ordering::AcqRel);
        Error::Closed
    })
}

/// Drain the outbound text-frame receiver into the user-supplied
/// transport sink. Exits when the receiver is exhausted (all Peers
/// dropped) or the sink errors / closes. Decrements
/// `inner.outbound_depth` per frame written so the counter mirrors
/// the real queue depth.
async fn forward_outbound<Si, SiE>(
    mut rx: mpsc::UnboundedReceiver<String>,
    inner: Arc<PeerInner>,
    mut sink: Si,
) where
    Si: Sink<String, Error = SiE> + Unpin,
    SiE: std::fmt::Display,
{
    while let Some(text) = rx.next().await {
        inner.outbound_depth.fetch_sub(1, Ordering::AcqRel);
        if sink.send(text).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}

fn params_into_value(params: Option<Params>) -> serde_json::Value {
    params
        .map(|p| p.into_value())
        .unwrap_or(serde_json::Value::Null)
}
