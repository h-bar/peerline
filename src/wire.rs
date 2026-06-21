//! peerline wire types — the envelopes that cross the wire.
//!
//! peerline defines four frame envelopes — [`Request`], [`Response`],
//! [`Notification`], [`StreamFrame`] — unified under [`Frame`].
//! Either peer can send any envelope at any time.
//!
//! ### Strict typing
//!
//! Where a field has multiple valid shapes, peerline picks the
//! strictest typed representation:
//!
//! - [`Request.id`](Request) is typed [`Id`] — `String` or `Number`
//!   only. Fractional numbers are rejected at parse time. Null id is
//!   modelled as `Option<Id>::None`.
//! - [`Request.params`](Request) is typed [`Params`] — only `Array`
//!   (positional) or `Object` (named) values. Scalars / booleans /
//!   `null` are rejected at parse time.
//! - [`Response`] is a `Result`-shaped enum: the success/error
//!   mutual exclusion is enforced at the type level — a single
//!   response can only be one or the other, never both, never
//!   neither.
//! - [`RpcError`] carries an optional `data` field for
//!   application-defined payloads.
//! - `Request` / `Response` / `Notification` / `StreamFrame` all
//!   carry `deny_unknown_fields`, so stray fields don't silently
//!   sneak through into the typed envelopes.
//!
//! ### Single-frame wire
//!
//! Packing multiple frames into one wire message (a JSON array of
//! frames, for example) is a transport-layer framing concern, not a
//! wire-type concern. Transports that batch split the wire array
//! into individual frame strings and call
//! [`crate::peer::parse_frame`] on each. See [`Frame`] for details.
//!
//! ### Symmetric
//!
//! Every envelope derives both [`serde::Serialize`] and
//! [`serde::Deserialize`], so the same struct definitions work on
//! both ends of the connection. Send and receive helpers live in
//! [`crate::peer`].
//!
//! ### JSON-RPC 2.0 interop
//!
//! [`Request`] / [`Response`] / [`Notification`] are wire-compatible
//! with JSON-RPC 2.0. peerline emits `"jsonrpc": "2.0"` on outbound
//! frames so vanilla JSON-RPC tooling decodes the basic three
//! envelopes without modification, and accepts the field as optional
//! on inbound frames so peers that omit it decode cleanly. The
//! [`Id`] / [`Params`] / [`Response`] constraints above happen to
//! match the JSON-RPC 2.0 wire shape; [`StreamFrame`] is
//! peerline-specific.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The protocol-version marker peerline emits on outbound frames.
/// Fixed at `"2.0"` for wire compatibility with JSON-RPC 2.0
/// tooling. Peers may omit the field on the wire — see
/// [`Request::jsonrpc`] for the receive-side behaviour.
pub const JSONRPC_VERSION: &str = "2.0";

/// Standard error code: invalid JSON was received.
pub const ERR_PARSE: i32 = -32700;
/// Standard error code: the JSON sent is not a valid Request
/// object.
pub const ERR_INVALID_REQUEST: i32 = -32600;
/// Standard error code: the method does not exist or is not
/// available.
pub const ERR_METHOD_NOT_FOUND: i32 = -32601;
/// Standard error code: invalid method parameter(s).
pub const ERR_INVALID_PARAMS: i32 = -32602;
/// Standard error code: internal protocol error.
pub const ERR_INTERNAL: i32 = -32603;

// ---------------------------------------------------------------------------
// Id
// ---------------------------------------------------------------------------

/// Request / response identifier — `String` or `Number`.
/// Fractional numbers are rejected at deserialize time (serde's
/// integer deserialization errors on floats), matching peerline's
/// "no fractional ids" rule.
///
/// A wire-level `null` id is modelled as `Option<Id>::None` rather
/// than a variant here — both [`Request.id`] and [`Response.id`] are
/// `Option<Id>` (`None` ⇒ JSON null on the wire). That keeps this
/// enum clean and lets [`HashMap`](std::collections::HashMap) key
/// directly on it for the pending-request registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Id {
    /// Integer id.
    Number(i64),
    /// String id.
    String(String),
}

impl Id {
    /// Extract the id as a non-negative integer, if this is
    /// `Id::Number(n)` with `n >= 0`. Convenience for callers that
    /// only ever issue `u64` ids via [`crate::peer::RequestIdGen`].
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Id::Number(n) if *n >= 0 => Some(*n as u64),
            _ => None,
        }
    }

    /// Extract the id as an `i64`, if this is `Id::Number`.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Id::Number(n) => Some(*n),
            _ => None,
        }
    }

    /// Extract the id as `&str`, if this is `Id::String`.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Id::String(s) => Some(s),
            _ => None,
        }
    }
}

impl From<u64> for Id {
    fn from(n: u64) -> Self {
        Id::Number(n as i64)
    }
}

impl From<i64> for Id {
    fn from(n: i64) -> Self {
        Id::Number(n)
    }
}

impl From<String> for Id {
    fn from(s: String) -> Self {
        Id::String(s)
    }
}

impl From<&str> for Id {
    fn from(s: &str) -> Self {
        Id::String(s.to_owned())
    }
}

// ---------------------------------------------------------------------------
// Params
// ---------------------------------------------------------------------------

/// Method parameters — `Array` (positional) or `Object` (named).
/// Scalars, booleans, and `null` are rejected at parse time. The
/// field is optional on [`Request`], so users typically wrap this
/// in `Option<Params>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Params {
    /// Positional parameters: a JSON array.
    Array(Vec<Value>),
    /// Named parameters: a JSON object.
    Object(Map<String, Value>),
}

impl Params {
    /// Convert into the underlying `serde_json::Value` for cases
    /// where a handler wants to deserialize into a typed param
    /// struct via `serde_json::from_value`.
    #[must_use]
    pub fn into_value(self) -> Value {
        match self {
            Params::Array(a) => Value::Array(a),
            Params::Object(o) => Value::Object(o),
        }
    }

    /// Borrow as a `serde_json::Value`-shaped reference. Useful for
    /// passing to typed deserialization without consuming the params.
    #[must_use]
    pub fn as_value(&self) -> Value {
        match self {
            Params::Array(a) => Value::Array(a.clone()),
            Params::Object(o) => Value::Object(o.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// Request / Response / Notification
// ---------------------------------------------------------------------------

/// A call from one peer that **expects a response**. `id` is
/// REQUIRED and non-null (`Number` or `String`). For one-way calls
/// that don't want a response, use [`Notification`] instead —
/// they're distinct Rust types so direction-typed dispatch is
/// enforced by the type system.
///
/// `#[serde(deny_unknown_fields)]` keeps the wire-shape strict: a
/// Response or Notification frame mistakenly sent in the request
/// slot can't masquerade as a Request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// `Some("2.0")` when peerline (or a JSON-RPC 2.0 peer) sent the
    /// frame; `None` when the peer omitted the field on the wire
    /// (codex app-server and some MCP transports do so). Not
    /// validated on parse; callers who want strict version
    /// enforcement can call [`validate_version`] themselves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// Caller-chosen request id — REQUIRED and non-null. A null id
    /// on the wire is treated as no-id (i.e. a [`Notification`]) by
    /// [`crate::peer::parse_frame`].
    pub id: Id,
    /// The method name being invoked.
    pub method: String,
    /// Method parameters — must be a JSON Array or Object when
    /// present. Absent for parameter-less calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

/// A reply to a [`Request`]. The `result` / `error` mutual
/// exclusion is enforced **at the type level**: this is a `Result`-
/// shaped enum, so a single response can only be one or the other,
/// never both, never neither.
///
/// On the wire the variants stay invisible (`#[serde(untagged)]`):
/// `Ok` serializes to `{"jsonrpc":...,"id":...,"result":...}`,
/// `Err` to `{"jsonrpc":...,"id":...,"error":...}`. Both inner
/// structs use `#[serde(deny_unknown_fields)]`, so a frame that has
/// both `result` and `error`, or has neither, or carries any other
/// unknown field, fails to deserialize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Response {
    /// Successful invocation — carries the `result` value.
    Ok(ResponseOk),
    /// Failed invocation — carries the `error` object.
    Err(ResponseErr),
}

/// Body of a successful [`Response`] — `jsonrpc`, `id`, `result`.
/// `deny_unknown_fields` rejects frames that also carry an `error`
/// field (which would violate the success/error mutual exclusion).
///
/// `id` is non-optional (`Id`, not `Option<Id>`): the only case
/// where a response carries `null` id is when the responder
/// couldn't recover the id from a malformed request — by definition
/// an *error*, not a success. A successful response always knows
/// the id it's replying to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseOk {
    /// `Some("2.0")` when peerline (or a JSON-RPC 2.0 peer) sent the
    /// frame; `None` when the peer omitted the field on the wire.
    /// See [`Request::jsonrpc`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// Echoes the request's `id`. Non-null — successful responses
    /// always know the id.
    pub id: Id,
    /// Success payload.
    pub result: Value,
}

/// Body of an error [`Response`] — `jsonrpc`, `id`, `error`.
/// `deny_unknown_fields` rejects frames that also carry a `result`
/// field (which would violate the success/error mutual exclusion).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseErr {
    /// `Some("2.0")` when peerline (or a JSON-RPC 2.0 peer) sent the
    /// frame; `None` when the peer omitted the field on the wire.
    /// See [`Request::jsonrpc`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// Echoes the request's `id`. `None` ⇒ JSON `null` on the wire
    /// (the only place null id is allowed: when the responder
    /// couldn't recover an id from a malformed request).
    pub id: Option<Id>,
    /// Error payload.
    pub error: RpcError,
}

impl Response {
    /// The response's `id` field, regardless of variant. Returns
    /// `Some` for [`Response::Ok`] (always non-null by type) and for
    /// [`Response::Err`] with a recovered id; `None` only for an
    /// `Err` response built for the malformed-request case (wire
    /// `id: null`).
    #[must_use]
    pub fn id(&self) -> Option<&Id> {
        match self {
            Response::Ok(r) => Some(&r.id),
            Response::Err(r) => r.id.as_ref(),
        }
    }

    /// The protocol-version field, regardless of variant. `None`
    /// when the peer omitted it on the wire.
    #[must_use]
    pub fn jsonrpc(&self) -> Option<&str> {
        match self {
            Response::Ok(r) => r.jsonrpc.as_deref(),
            Response::Err(r) => r.jsonrpc.as_deref(),
        }
    }

    /// `Some(&result)` iff this is an [`Response::Ok`].
    #[must_use]
    pub fn result(&self) -> Option<&Value> {
        match self {
            Response::Ok(r) => Some(&r.result),
            Response::Err(_) => None,
        }
    }

    /// `Some(&error)` iff this is an [`Response::Err`].
    #[must_use]
    pub fn error(&self) -> Option<&RpcError> {
        match self {
            Response::Ok(_) => None,
            Response::Err(r) => Some(&r.error),
        }
    }

    /// `true` iff this is an [`Response::Ok`].
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self, Response::Ok(_))
    }

    /// `true` iff this is an [`Response::Err`].
    #[must_use]
    pub fn is_err(&self) -> bool {
        matches!(self, Response::Err(_))
    }

    /// Consume the response into a Rust [`Result`]. Useful in the
    /// receive-side routing path where downstream code only cares
    /// about success vs failure, not the envelope around it.
    pub fn into_outcome(self) -> Result<Value, RpcError> {
        match self {
            Response::Ok(r) => Ok(r.result),
            Response::Err(r) => Err(r.error),
        }
    }
}

/// A one-way call without an `id` member — no reply expected, the
/// other peer MUST NOT send one.
///
/// A separate Rust type from [`Request`] (not a `Request` with
/// `id = None`): this means the type system enforces "Notifications
/// don't get responses" — handlers that consume a [`Notification`]
/// can't accidentally return a [`Response`] for it. Either peer can
/// send a notification at any time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notification {
    /// `Some("2.0")` when peerline (or a JSON-RPC 2.0 peer) sent the
    /// frame; `None` when the peer omitted the field on the wire.
    /// See [`Request::jsonrpc`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// The notification method name.
    pub method: String,
    /// Notification payload — must be a JSON Array or Object when
    /// present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

/// Error object — the body of [`Response::Err`]. The `data` field
/// is optional and carries arbitrary application-specific
/// information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    /// One of the [`ERR_PARSE`] / [`ERR_INVALID_REQUEST`] / …
    /// constants or an application-range value (`-32000` to
    /// `-32099` are reserved for application use).
    pub code: i32,
    /// Human-readable error message.
    pub message: String,
    /// Optional application-defined data — anything from a Sentry
    /// id to a structured validation failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// ---------------------------------------------------------------------------
// Frame — single-frame wire envelope, peer-symmetric
// ---------------------------------------------------------------------------

/// Anything that crosses the wire as one peerline frame, in either
/// direction. Both peers may send any variant — there is no
/// "client"-only or "server"-only frame.
///
/// **Batching is intentionally not modelled here.** Packing multiple
/// [`Frame`]s into one wire message (a JSON array of frames, for
/// example) is a transport-layer framing concern — the transport's
/// job, not the wire type's. Transports that batch split the array
/// into individual frame strings and call [`crate::peer::parse_frame`]
/// on each.
///
/// ### Untagged discriminator
///
/// `#[serde(untagged)]` tries variants in *declaration order* and
/// picks the first that succeeds. We list **most-restrictive first**
/// so a malformed frame can't accidentally land in a too-permissive
/// variant:
///
/// 1. `Stream` — must have `id` AND `stream`.
/// 2. `Response` — must have `id` AND (`result` xor `error`).
/// 3. `Request` — must have `id` AND `method`.
/// 4. `Notification` — must have `method`, must NOT have `id`.
///
/// `deny_unknown_fields` on every inner struct (`ResponseOk`,
/// `ResponseErr`, `Request`, `Notification`, `StreamFrame`) makes
/// the variants truly disjoint: e.g. an object with both
/// `id`+`method`+`result` is rejected by every variant
/// (`Response` doesn't allow `method`, `Request` doesn't allow
/// `result`), so it fails enum-level rather than silently landing
/// somewhere wrong.
///
/// `wire_disambiguation_*` integration tests pin this contract down.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Frame {
    /// A streaming-lifecycle frame (see [`StreamFrame`]).
    Stream(StreamFrame),
    /// A reply to a [`Request`] this peer (or the other peer) sent.
    Response(Response),
    /// A call from one peer expecting a reply.
    Request(Request),
    /// A one-way call (no reply expected).
    Notification(Notification),
}

// ---------------------------------------------------------------------------
// StreamFrame
// ---------------------------------------------------------------------------

/// One streaming-lifecycle frame, modelled as a phase-per-variant
/// enum so each variant carries exactly the fields its phase
/// requires. The type system enforces the `data` ⇔ `Item` and
/// `error` ⇔ `Error` invariants — there is no malformed
/// `StreamFrame` value to construct in Rust.
///
/// Streaming is peerline-specific — it has no JSON-RPC 2.0
/// counterpart. The `jsonrpc` field still says `"2.0"` on the wire
/// so peers that ignore unknown frames silently drop it without
/// erroring; peerline-aware peers recognise the `stream` field as
/// the streaming marker.
///
/// All frames correlate to the originating [`Request`] by `id`.
/// Both peers can send any variant — streams are bidi-capable with
/// independent per-side half-close semantics.
///
/// Wire shape uses `#[serde(tag = "stream")]` internally-tagged
/// representation: the `stream` field carries the variant name and
/// the other fields sit alongside it at the same level.
///
/// ```jsonc
/// // open — optional ack of streaming intent
/// {"jsonrpc":"2.0", "id":7, "stream":"open"}
///
/// // item — one stream element
/// {"jsonrpc":"2.0", "id":7, "stream":"item", "seq":1, "data":{...}}
///
/// // `seq` is a producer-assigned monotonic counter starting at 1.
/// // Non-consecutive seqs signal dropped items (sender's
/// // upstream lagged) — receivers recover out of band.
///
/// // close — graceful half-close (other side may keep sending)
/// {"jsonrpc":"2.0", "id":7, "stream":"close"}
///
/// // error — abnormal half-close
/// {"jsonrpc":"2.0", "id":7, "stream":"error", "error":{code,message,data?}}
///
/// // cancel — full-close, terminates both halves immediately
/// {"jsonrpc":"2.0", "id":7, "stream":"cancel"}
/// ```
///
/// `deny_unknown_fields` rejects stray fields per variant: e.g. a
/// `stream:open` frame carrying `data` fails to deserialize rather
/// than landing in `Item` by mistake. `id` MUST match an active
/// stream id — that's the receiver's stream registry's job, not the
/// wire type's.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stream", rename_all = "lowercase", deny_unknown_fields)]
pub enum StreamFrame {
    /// Optional acknowledgement that this RPC is a stream. The
    /// sender may skip `Open` and start directly with `Item` if it
    /// has data ready — `Open` is useful as an early ack when
    /// producing the first item is slow.
    Open {
        /// Protocol-version marker (`Some("2.0")` when emitted by
        /// peerline; `None` when the peer omitted the field on the
        /// wire). See [`Request::jsonrpc`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        jsonrpc: Option<String>,
        /// Correlates to the originating [`Request`].
        id: Id,
    },
    /// One element of the sender's outgoing half-stream.
    Item {
        /// Protocol-version marker (`Some("2.0")` when emitted by
        /// peerline; `None` when the peer omitted the field on the
        /// wire). See [`Request::jsonrpc`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        jsonrpc: Option<String>,
        /// Correlates to the originating [`Request`].
        id: Id,
        /// Producer-assigned monotonic sequence number for this
        /// item within the stream, starting at `1` for the first
        /// item the sender emits. The sender may advance `seq`
        /// past consecutive values without sending the intervening
        /// items (e.g. an upstream broadcast buffer overflowed
        /// because the receiver was lagging) — receivers detect
        /// dropped items by non-consecutive `seq`s and can recover
        /// out of band.
        seq: u64,
        /// Stream element payload — typed by the consumer.
        data: Value,
    },
    /// Graceful half-close: the sender will produce no more items.
    /// The other side may still send. Stream is `DONE` only when
    /// both halves are closed.
    Close {
        /// Protocol-version marker (`Some("2.0")` when emitted by
        /// peerline; `None` when the peer omitted the field on the
        /// wire). See [`Request::jsonrpc`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        jsonrpc: Option<String>,
        /// Correlates to the originating [`Request`].
        id: Id,
    },
    /// Abnormal half-close with `error` populated — the sender
    /// failed on its side.
    Error {
        /// Protocol-version marker (`Some("2.0")` when emitted by
        /// peerline; `None` when the peer omitted the field on the
        /// wire). See [`Request::jsonrpc`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        jsonrpc: Option<String>,
        /// Correlates to the originating [`Request`].
        id: Id,
        /// Stream-level error.
        error: RpcError,
    },
    /// Full-close: terminates the stream in both directions
    /// immediately and discards anything in-flight. Either peer
    /// may send.
    Cancel {
        /// Protocol-version marker (`Some("2.0")` when emitted by
        /// peerline; `None` when the peer omitted the field on the
        /// wire). See [`Request::jsonrpc`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        jsonrpc: Option<String>,
        /// Correlates to the originating [`Request`].
        id: Id,
    },
}

impl StreamFrame {
    /// The `id` field, regardless of variant. Convenience for the
    /// 90% case where you just want to route to the matching stream
    /// in the receiver's registry without destructuring.
    #[must_use]
    pub fn id(&self) -> &Id {
        match self {
            Self::Open { id, .. }
            | Self::Item { id, .. }
            | Self::Close { id, .. }
            | Self::Error { id, .. }
            | Self::Cancel { id, .. } => id,
        }
    }

    /// The protocol-version field, regardless of variant. `None`
    /// when the peer omitted it on the wire.
    #[must_use]
    pub fn jsonrpc(&self) -> Option<&str> {
        match self {
            Self::Open { jsonrpc, .. }
            | Self::Item { jsonrpc, .. }
            | Self::Close { jsonrpc, .. }
            | Self::Error { jsonrpc, .. }
            | Self::Cancel { jsonrpc, .. } => jsonrpc.as_deref(),
        }
    }
}

// ---------------------------------------------------------------------------
// Version validation — opt-in helper. The parse path treats the
// `jsonrpc` field as optional (defaulting to `"2.0"` when absent) for
// maximum interop; callers who want strict behaviour call this
// directly on the parsed frame.
// ---------------------------------------------------------------------------

/// Check that the protocol-version marker is exactly `"2.0"`.
///
/// Returns `Ok(())` on a match; otherwise an `Err` whose message is
/// suitable for the `message` field of a `-32600` InvalidRequest
/// response.
pub fn validate_version(jsonrpc: &str) -> Result<(), String> {
    if jsonrpc == JSONRPC_VERSION {
        Ok(())
    } else {
        Err(format!(
            "jsonrpc version must be {JSONRPC_VERSION:?}, got {jsonrpc:?}"
        ))
    }
}
