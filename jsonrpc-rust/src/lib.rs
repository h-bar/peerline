//! Transport-agnostic JSON-RPC 2.0 toolkit, peer-symmetric core +
//! pubsub extension.
//!
//! ### Spec-aligned core
//!
//! Everything below is defined by the
//! [JSON-RPC 2.0 spec](https://www.jsonrpc.org/specification). Where
//! the spec allows a range of values, we pick the strictest
//! spec-compatible interpretation (typed `Id`, typed `Params`,
//! `Response` is a `Result`-shaped enum enforcing result/error
//! mutual exclusion, `deny_unknown_fields` on envelopes).
//!
//! - [`wire`] — envelope types (`Request`, `Response`,
//!   `Notification`, `RpcError`, `Id`, `Params`, `Frame`), standard
//!   error code constants, and the [`wire::validate_version`]
//!   helper. Single-frame only — spec §6 batching is a
//!   transport-layer concern, not a wire shape.
//! - [`peer`] — peer-symmetric parsing + dispatch primitives.
//!   [`peer::parse_frame`] turns a text frame into a [`wire::Frame`]
//!   (the `jsonrpc` version field is optional — see its doc);
//!   [`peer::classify`] dispatches it as
//!   [`peer::InboundKind`] (this peer's pending Response, an
//!   incoming Request to handle, or an incoming Notification).
//!   Outgoing builders ([`peer::request`], [`peer::notification`],
//!   [`peer::response_ok`], [`peer::response_err`]) and
//!   [`peer::RequestIdGen`] cover the send-side. Replaces the old
//!   `server` + `client` split — both endpoints of a connection
//!   use the same API.
//!
//! ### Pubsub extension (not in spec)
//!
//! - [`pubsub`] — the de-facto JSON-RPC pubsub-subscription
//!   convention (Ethereum's `eth_subscribe`, jsonrpsee, …):
//!   subscribe RPCs return a [`pubsub::SubscriptionAck`]; the
//!   pushing peer sends [`wire::Notification`]s with method
//!   `"event"` / `"end"`; the receiving peer cancels with an
//!   `unsubscribe` request. Layered on top of the core without
//!   modifying it — pass any
//!   [`peer::InboundKind::IncomingNotification`] through
//!   [`pubsub::classify`] to recognise pubsub messages.
//!
//! ### Streaming extension (proposed 2.1)
//!
//! First-class streaming as a hypothetical JSON-RPC 2.1 extension.
//! Adds one wire envelope, [`wire::StreamFrame`], modelled as a
//! phase-per-variant enum so each variant carries exactly the
//! fields its lifecycle stage requires (the type system enforces
//! the `data` ⇔ `Item` and `error` ⇔ `Error` invariants — no
//! runtime validation needed). Variants: `Open` (optional ack),
//! `Item` (data element), `Close` (graceful half-close), `Error`
//! (abnormal half-close), `Cancel` (full-close). All correlate to
//! the originating request by `id`.
//!
//! Both peers can send any phase — streams are bidi-capable with
//! independent per-side half-close semantics. Builders for each
//! phase live in [`peer`] (`stream_open`, `stream_item`,
//! `stream_close`, `stream_error`, `stream_cancel`); inbound stream
//! frames classify as [`peer::InboundKind::Stream`]. See
//! `workbench/jsonrpc-rust-streaming.md` for the full design.

#![warn(missing_docs)]

pub mod peer;
pub mod pubsub;
pub mod wire;
