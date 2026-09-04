//! Stateful [`Peer`] built on the pure helpers in
//! [`crate::peer`] / [`crate::wire`]. Opt in via the `runtime`
//! feature.
//!
//! Where the pure layer gives you parsing, classification, and
//! frame builders, this module adds the connection-level state:
//!
//! - **Pending-request map** — outgoing `Request`s waiting on a
//!   matching `Response`, resolved through `futures::channel::oneshot`.
//! - **Handler registry** — incoming `Request` / `Notification` /
//!   streaming-request handlers, dispatched to async closures.
//! - **Stream registry** — active inbound streams (this peer is the
//!   receiver) keyed by request id, fed via
//!   `futures::channel::mpsc`.
//! - **Outbound scheduler** — a fair writer (see [`outbound`](self)'s
//!   internals): one priority control queue (responses, requests,
//!   notifications) plus per-stream queues drained round-robin into
//!   the caller-supplied transport sink, so one busy stream can't
//!   head-of-line-block control traffic or its siblings.
//!
//! Construction binds the peer to its transport: [`Peer::new`]
//! takes a `Sink<String>` plus a `Stream<Item = Result<String, _>>`
//! and returns both the peer handle and a *driver future* the
//! caller awaits or spawns. The driver runs the dispatch loop
//! (parse + classify + route via
//! [`InboundKind`](crate::peer::InboundKind)) and the outbound
//! drain side-by-side; it resolves when the transport ends —
//! flushing queued responses first on a clean read-side close.
//!
//! ### Cancel-on-drop streams
//!
//! Returned [`StreamReceiver`]s send a reserved cancel notification
//! ([`STREAM_CANCEL_OP`]) and remove themselves from the registry when
//! dropped; the producing peer intercepts it and closes that stream's
//! outbound queue, so the handler's next send fails. A handler can also
//! await [`StreamSender::cancelled`] to stop expensive upstream work
//! the moment the consumer leaves, without waiting for its next send.
//! The consumer gets gRPC-flavoured "drop the handle and the server
//! stops producing" semantics for free.
//!
//! ### Runtime-agnostic
//!
//! No tokio dep, no spawn primitive. The module uses
//! `futures::channel` + `FuturesUnordered` so the driver is a
//! single `Future` the user runs on whatever executor they prefer
//! — tokio, async-std, smol, `wasm-bindgen-futures`,
//! `futures::executor::block_on`, …
//!
//! Wasm-friendly: compiles to `wasm32-unknown-unknown` out of the
//! box (no `tokio::spawn`, no thread-local runtime handle).

mod error;
mod outbound;
mod peer;
mod stream;

pub use error::Error;
pub use peer::{Metrics, Peer};
pub use stream::{StreamItem, StreamReceiver, StreamSender};

use futures::stream::StreamExt;

/// Reserved notification op for stream cancellation. A dropped
/// [`StreamReceiver`] emits this (carrying the stream id) so the
/// producing peer closes that stream's outbound queue. The `$`-prefix
/// namespaces it away from application op names; the dispatch loop
/// intercepts it before user notification handlers, so registering a
/// handler for this op has no effect.
pub(crate) const STREAM_CANCEL_OP: &str = "$peerline/stream.cancel";

/// Wire two peers together in-process via two `mpsc::unbounded`
/// pairs (A → B and B → A). Returns both peers plus a driver
/// future the caller spawns on their runtime — while the driver is
/// alive, frames written on either peer feed the other peer's
/// dispatch loop.
///
/// Useful for tests, in-process plugin architectures, or anywhere
/// you'd otherwise pipe a peerline connection through an external
/// transport.
///
/// ```ignore
/// let (a, b, driver) = peerline::runtime::loopback();
/// tokio::spawn(driver);
///
/// b.on_request("ping", |_: serde_json::Value| async {
///     Ok::<_, peerline::wire::RpcError>("pong")
/// });
/// let resp: String = a.call("ping", &serde_json::json!([])).await?;
/// assert_eq!(resp, "pong");
/// ```
pub fn loopback() -> (Peer, Peer, impl std::future::Future<Output = ()>) {
    let (a_to_b_tx, a_to_b_rx) = futures::channel::mpsc::unbounded::<String>();
    let (b_to_a_tx, b_to_a_rx) = futures::channel::mpsc::unbounded::<String>();

    // a sends via a_to_b_tx → b reads from a_to_b_rx; symmetric for b.
    let a_inbound = b_to_a_rx.map(Ok::<_, std::convert::Infallible>);
    let b_inbound = a_to_b_rx.map(Ok::<_, std::convert::Infallible>);

    let (peer_a, driver_a) = Peer::new(a_to_b_tx, a_inbound);
    let (peer_b, driver_b) = Peer::new(b_to_a_tx, b_inbound);

    let driver = async move {
        futures::future::join(driver_a, driver_b).await;
    };

    (peer_a, peer_b, driver)
}
