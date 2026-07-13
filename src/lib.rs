//! peerline — peer-symmetric bidirectional RPC toolkit.
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
//! All four are unified under [`wire::Frame`] via a two-level tagged
//! enum: the outer `ver` tag selects the wire version (today only
//! `"1"`) and the inner `kind` tag selects the envelope shape
//! (`req` / `resp` / `notif` / `stream`). Both peers can send any
//! variant; streams are bidi-capable with independent per-side
//! half-close semantics.
//!
//! ### Wire format
//!
//! ```jsonc
//! // request
//! {"ver":"1", "kind":"req",   "id":7, "op":"foo", "args":{"x":1}}
//! // success response
//! {"ver":"1", "kind":"resp",  "id":7, "data":42}
//! // error response (id may be null for parse-error replies)
//! {"ver":"1", "kind":"resp",  "id":7, "err":{"code":-32603, "msg":"bad"}}
//! // notification (no id)
//! {"ver":"1", "kind":"notif",         "op":"event", "args":{...}}
//! // stream item
//! {"ver":"1", "kind":"stream","id":7, "seq":1, "data":{...}}
//! ```
//!
//! Wire field names are kept ≤ 4 chars — `op` (operation), `args`
//! (arguments), `data` (result / stream item), `err` (error), `seq`
//! (stream sequence), `msg` (error message). Most Rust field names
//! match the wire name directly; `#[serde(rename)]` bridges the few
//! that differ.
//!
//! Adding a new wire version (`ver: "2"`) is purely additive — define
//! `wire::v2::Content` and add a `V2(v2::Content)` variant to
//! [`wire::Frame`]; v1 code is untouched.
//!
//! ### Modules
//!
//! - [`wire`] — frame envelope types, version-tag dispatch, standard
//!   error code constants. Single-frame only: packing multiple frames
//!   in one wire message is a transport-layer concern.
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
//!   on top of the core — pass any
//!   [`peer::InboundKind::IncomingNotification`] through
//!   [`pubsub::classify`] to recognise pubsub messages.
//! - `runtime` *(optional, enable with `runtime` feature)* —
//!   stateful [`Peer`](runtime::Peer) on top of the pure helpers:
//!   pending-request map, handler registry, stream registry,
//!   outbound channel. Runtime-agnostic, built on
//!   `futures::channel`; works on tokio, async-std, smol, and wasm.
//!
//! ### Streaming
//!
//! Streaming is a first-class peerline feature, modelled as a
//! phase-per-variant enum so each variant carries exactly the
//! fields its lifecycle stage requires (the type system enforces
//! the `d` ⇔ `Item` and `e` ⇔ `Error` invariants — no
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

/// Recommended maximum size, in bytes, of a single text frame carrying
/// one peerline [`wire::Frame`].
///
/// Byte-level framing is a transport concern (see the module docs), but
/// the size *ceiling* is shared policy: every transport that carries
/// peerline frames should pin its codec to this one value rather than
/// inheriting a per-library default, so they behave alike and a peer
/// cannot make any decoder buffer more than this before a frame
/// completes. 64 MiB matches tungstenite's default message ceiling.
pub const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// Stateful runtime [`Peer`](runtime::Peer) — call / notify /
/// stream / handler registry. Opt in via the `runtime` feature.
#[cfg(feature = "runtime")]
pub mod runtime;
