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
//! - **Outbound channel** — a single `mpsc::UnboundedSender<String>`
//!   into which every helper writes serialized frames; an internal
//!   drain task pipes the matching receiver into the caller-supplied
//!   transport sink.
//!
//! Construction binds the peer to its transport: [`Peer::new`]
//! takes a `Sink<String>` plus a `Stream<Item = Result<String, _>>`
//! and returns both the peer handle and a *driver future* the
//! caller awaits or spawns. The driver runs the dispatch loop
//! (parse + classify + route via
//! [`InboundKind`](crate::peer::InboundKind)) and the outbound
//! drain side-by-side; it resolves when either half ends.
//!
//! ### Cancel-on-drop streams
//!
//! Returned [`StreamReceiver`]s send a `stream:cancel` frame and
//! remove themselves from the registry when dropped. The consumer
//! gets gRPC-flavoured "drop the handle and the server stops
//! producing" semantics for free.
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
mod peer;
mod stream;

pub use error::Error;
pub use peer::{Metrics, Peer};
pub use stream::{StreamItem, StreamReceiver, StreamSender};

use futures::stream::StreamExt;

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
