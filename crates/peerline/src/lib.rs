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
//! - [`wire::StreamFrame`] — one frame of a stream, correlated to
//!   the originating Request by `id`. A flat envelope: the lifecycle
//!   lives in `seq` (`>= 0` item, `-1` terminal), not in a variant
//!   tag.
//!
//! All four are unified under [`wire::Frame`] via a two-level tagged
//! enum: the outer `ver` tag selects the wire version (today only
//! `"1"`) and the inner `kind` tag selects the envelope shape
//! (`req` / `resp` / `notif` / `stream`). Both peers can send any
//! variant, and either peer can open a stream.
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
//!   [`peer::response_ok`], [`peer::response_err`], the
//!   `stream_item` / `stream_terminal*` family) and
//!   [`peer::RequestIdGen`] cover the send-side.
//! - [`pubsub`] — pubsub-subscription layer. Subscribe RPCs return
//!   a [`pubsub::SubscriptionAck`]; the pushing peer sends
//!   [`wire::Notification`]s on the reserved `$peerline/pubsub.*`
//!   ops ([`pubsub::EVENT_OP`] / [`pubsub::END_OP`]); the receiving
//!   peer cancels with a [`pubsub::UNSUBSCRIBE_OP`] request. Layered
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
//! Streaming is a first-class peerline feature, modelled as a single
//! flat [`wire::StreamFrame`] envelope — there is no per-phase
//! variant, and no `Open` / `Close` / `Cancel` frame. The lifecycle
//! is encoded in `seq`:
//!
//! - `seq >= 0` — a regular item. Items are 0-indexed and monotonic;
//!   the first (`seq = 0`) implicitly opens the stream, and gaps
//!   signal items the producer skipped.
//! - `seq == -1` — the terminal frame, ending the stream. It may
//!   bundle a final `data` payload and/or an `err`.
//!
//! A receiver treats any frame as terminal once `seq == -1` *or*
//! `err` is present. All frames correlate to the originating request
//! by `id`, and inbound stream frames classify as
//! [`peer::InboundKind::Stream`].
//!
//! Builders live in [`peer`]: [`peer::stream_item`],
//! [`peer::stream_terminal`], [`peer::stream_terminal_with_data`],
//! [`peer::stream_terminal_with_error`],
//! [`peer::stream_terminal_with_rpc_error`] and
//! [`peer::stream_terminal_with_error_data`]. Cancellation is not a
//! frame kind — the consumer sends a reserved notification, which the
//! `runtime` feature wires up for you (`runtime::StreamReceiver`).

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
