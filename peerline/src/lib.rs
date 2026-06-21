//! peerline — peer-symmetric JSON framing toolkit.
//!
//! peerline is a bidirectional messaging protocol where either
//! endpoint can initiate any frame at any time. There are no
//! "client" and "server" roles at the protocol level; both peers
//! use the same API to send requests, send notifications, reply,
//! and open streams.
//!
//! ### Wire envelopes
//!
//! Every frame on the wire is one of four envelopes:
//!
//! - [`wire::Request`] — a call that expects a reply.
//! - [`wire::Response`] — the reply, modelled as a `Result`-shaped
//!   enum so success/error mutual exclusion is enforced by the type
//!   system rather than runtime checks.
//! - [`wire::Notification`] — a one-way call with no reply.
//! - [`wire::StreamFrame`] — a streaming-lifecycle frame
//!   (`Open` / `Item` / `Close` / `Error` / `Cancel`) correlated to
//!   the originating Request by `id`.
//!
//! All four are unified under [`wire::Frame`]. Both peers can send
//! any variant; streams are bidi-capable with independent per-side
//! half-close semantics.
//!
//! ### Wire-level interop with JSON-RPC 2.0
//!
//! The Request, Response, and Notification envelopes are
//! wire-compatible with JSON-RPC 2.0: peerline emits the
//! `"jsonrpc": "2.0"` marker on outbound frames so existing
//! JSON-RPC tooling can speak the basic three envelopes without
//! modification. The marker is accepted as optional on inbound
//! frames, so peers that omit it (some MCP transports, app-server
//! style protocols) decode cleanly.
//!
//! Streaming and pubsub are peerline-native and have no JSON-RPC
//! counterpart.
//!
//! ### Modules
//!
//! - [`wire`] — frame envelope types, standard error code
//!   constants, and the [`wire::validate_version`] helper.
//!   Single-frame only: packing multiple frames in one wire
//!   message is a transport-layer concern.
//! - [`peer`] — peer-symmetric parsing + dispatch primitives.
//!   [`peer::parse_frame`] turns a text frame into a [`wire::Frame`];
//!   [`peer::classify`] dispatches it as [`peer::InboundKind`]
//!   (this peer's pending Response, an incoming Request to handle,
//!   an incoming Notification, or a Stream frame). Outgoing
//!   builders ([`peer::request`], [`peer::notification`],
//!   [`peer::response_ok`], [`peer::response_err`], stream phase
//!   builders) and [`peer::RequestIdGen`] cover the send-side.
//! - [`pubsub`] — pubsub-subscription layer. Subscribe RPCs return
//!   a [`pubsub::SubscriptionAck`]; the pushing peer sends
//!   [`wire::Notification`]s with method `"event"` / `"end"`; the
//!   receiving peer cancels with an `unsubscribe` request. Layered
//!   on top of the core without modifying it — pass any
//!   [`peer::InboundKind::IncomingNotification`] through
//!   [`pubsub::classify`] to recognise pubsub messages. The same
//!   convention is used by Ethereum's `eth_subscribe`, jsonrpsee,
//!   and many other ecosystems.
//!
//! ### Streaming
//!
//! Streaming is a first-class peerline feature, modelled as a
//! phase-per-variant enum so each variant carries exactly the
//! fields its lifecycle stage requires (the type system enforces
//! the `data` ⇔ `Item` and `error` ⇔ `Error` invariants — no
//! runtime validation needed). Variants: `Open` (optional ack),
//! `Item` (data element), `Close` (graceful half-close), `Error`
//! (abnormal half-close), `Cancel` (full-close). All correlate to
//! the originating request by `id`. Builders for each phase live
//! in [`peer`] (`stream_open`, `stream_item`, `stream_close`,
//! `stream_error`, `stream_cancel`); inbound stream frames
//! classify as [`peer::InboundKind::Stream`].

#![warn(missing_docs)]

pub mod peer;
pub mod pubsub;
pub mod wire;
