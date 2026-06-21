//! JSON-RPC 2.0 wire types — strictly spec-aligned.
//!
//! Everything in this module is defined by the
//! [2.0 spec](https://www.jsonrpc.org/specification). Extensions
//! (pubsub subscription envelopes, etc.) live in [`crate::pubsub`].
//!
//! Where the spec allows a range of values, this crate picks the
//! strictest spec-compatible interpretation:
//!
//! - [`Request.id`](Request) is typed [`Id`] — `String` or `Number`
//!   only. Fractional numbers are rejected at parse time. The third
//!   spec-allowed value, `Null`, is modelled as `Option<Id>::None`.
//! - [`Request.params`](Request) is typed [`Params`] — only `Array`
//!   (positional) or `Object` (named) values, as required by §4.2.
//! - [`Response`] is a `Result`-shaped enum: the `result` / `error`
//!   mutual exclusion of §5 is enforced at the type level.
//! - [`RpcError`] carries the optional `data` field from §5.
//! - Batching (spec §6) is intentionally not in the wire types —
//!   it's a transport-layer framing concern (multiple frames in one
//!   wire message). See [`Frame`] for details.
//! - `Request` / `Response` / `Notification` all carry
//!   `deny_unknown_fields`, so extension fields don't silently
//!   sneak through into the typed envelopes.
//!
//! Symmetric: every envelope derives both [`serde::Serialize`] and
//! [`serde::Deserialize`] so the same struct definitions work on
//! both ends of the connection. Direction-specific helpers live in
//! [`crate::server`] and [`crate::client`].

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The `jsonrpc` field every envelope carries when emitted by this
/// crate. Peers may omit the field on the wire — see
/// [`Request::jsonrpc`] for the receive-side behaviour.
pub const JSONRPC_VERSION: &str = "2.0";

/// Standard JSON-RPC 2.0 error code: invalid JSON was received.
pub const ERR_PARSE: i32 = -32700;
/// Standard JSON-RPC 2.0 error code: the JSON sent is not a valid
/// Request object.
pub const ERR_INVALID_REQUEST: i32 = -32600;
/// Standard JSON-RPC 2.0 error code: the method does not exist or is
/// not available.
pub const ERR_METHOD_NOT_FOUND: i32 = -32601;
/// Standard JSON-RPC 2.0 error code: invalid method parameter(s).
pub const ERR_INVALID_PARAMS: i32 = -32602;
/// Standard JSON-RPC 2.0 error code: internal JSON-RPC error.
pub const ERR_INTERNAL: i32 = -32603;

// ---------------------------------------------------------------------------
// Id — spec §4.1 / §5
// ---------------------------------------------------------------------------

/// JSON-RPC 2.0 identifier — `String` or `Number` per spec §4.1.
/// Fractional numbers are rejected at deserialize time (serde's
/// integer deserialization errors on floats), matching the spec's
/// "SHOULD NOT contain fractional parts" rule.
///
/// The spec's third allowed id value, `Null`, is modelled as
/// `Option<Id>::None` rather than a variant here — both [`Request.id`]
/// and [`Response.id`] are `Option<Id>` (`None` ⇒ JSON null on the
/// wire). That keeps this enum clean and lets [`HashMap`](std::collections::HashMap)
/// key directly on it for client-side pending-request registries.
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
    /// only ever issue `u64` ids via [`crate::client::RequestIdGen`].
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
// Params — spec §4.2 (Structured value: Array or Object)
// ---------------------------------------------------------------------------

/// JSON-RPC 2.0 method parameters — `Array` (positional) or `Object`
/// (named) per spec §4.2. Scalars, booleans, and `null` are rejected
/// at parse time. The field is optional on [`Request`], so users
/// typically wrap this in `Option<Params>`.
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
// Request / Response / Notification — spec §4 / §5
// ---------------------------------------------------------------------------

/// One JSON-RPC 2.0 request per spec §4 — a call from the client
/// that **expects a response**. `id` is REQUIRED and non-null
/// (`Number` or `String`). For one-way calls that don't want a
/// response, use [`Notification`] instead — they're distinct Rust
/// types so direction-typed dispatch is enforced by the type system.
///
/// `#[serde(deny_unknown_fields)]` keeps the wire-shape strict: a
/// Response or Notification frame mistakenly sent in the request
/// slot can't masquerade as a Request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// `Some("2.0")` per JSON-RPC §4 / §5; `None` when the peer
    /// omitted the field on the wire (codex app-server and some MCP
    /// transports do so). Not validated on parse; callers who want
    /// spec-strict behaviour can call [`validate_version`] themselves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// Caller-chosen request id — REQUIRED, non-null per spec §4.1.
    /// The spec discourages `null` here ("a value of Null is
    /// discouraged because this specification uses a value of Null
    /// for Responses with an unknown id"); our [`server::parse_inbound`](crate::server::parse_inbound)
    /// treats wire `id: null` as no-id (i.e. a [`Notification`]).
    pub id: Id,
    /// The method name being invoked.
    pub method: String,
    /// Method parameters — must be a JSON Array or Object when
    /// present. Absent for parameter-less calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

/// One JSON-RPC 2.0 response — what a server sends and a client
/// receives. The `result` / `error` mutual exclusion required by
/// spec §5 is enforced **at the type level**: this is a `Result`-
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
/// field (which would violate spec §5 mutual exclusion).
///
/// `id` is non-optional (`Id`, not `Option<Id>`): per spec §5, the
/// only case where a response carries `null` id is when the server
/// couldn't recover the id from a malformed request — by definition
/// an *error*, not a success. A successful response always knows
/// the id it's replying to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseOk {
    /// `Some("2.0")` per JSON-RPC §5; `None` when the peer omitted
    /// the field on the wire. See [`Request::jsonrpc`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// Echoes the request's `id`. Non-null per spec §5 — successful
    /// responses always know the id.
    pub id: Id,
    /// Success payload.
    pub result: Value,
}

/// Body of an error [`Response`] — `jsonrpc`, `id`, `error`.
/// `deny_unknown_fields` rejects frames that also carry a `result`
/// field (which would violate spec §5 mutual exclusion).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseErr {
    /// `Some("2.0")` per JSON-RPC §5; `None` when the peer omitted
    /// the field on the wire. See [`Request::jsonrpc`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// Echoes the request's `id`. `None` ⇒ JSON `null` on the wire
    /// (mandatory when the server couldn't recover an id from a
    /// malformed request per spec §5 — the only place null id is
    /// allowed).
    pub id: Option<Id>,
    /// Error payload.
    pub error: RpcError,
}

impl Response {
    /// The response's `id` field, regardless of variant. Returns
    /// `Some` for [`Response::Ok`] (always non-null by type) and for
    /// [`Response::Err`] with a recovered id; `None` only for an
    /// `Err` response constructed for the spec §5 parse-error case
    /// (wire `id: null`).
    #[must_use]
    pub fn id(&self) -> Option<&Id> {
        match self {
            Response::Ok(r) => Some(&r.id),
            Response::Err(r) => r.id.as_ref(),
        }
    }

    /// The `jsonrpc` version string, regardless of variant. `None`
    /// when the peer omitted the field on the wire.
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
    /// client-side routing path where downstream code only cares
    /// about success vs failure, not the envelope around it.
    pub fn into_outcome(self) -> Result<Value, RpcError> {
        match self {
            Response::Ok(r) => Ok(r.result),
            Response::Err(r) => Err(r.error),
        }
    }
}

/// One JSON-RPC 2.0 notification per spec §4.1 — a one-way call
/// without an `id` member. Signals that the client doesn't want a
/// response; the server MUST NOT send one.
///
/// **Direction is client → server only.** The spec describes
/// Notifications as a thing clients send; it does not define
/// server-initiated notifications. The server-push pattern used by
/// our pubsub extension uses [`crate::pubsub::Event`] instead — same
/// wire shape, but a distinct Rust type so direction confusion is
/// impossible.
///
/// A separate Rust type from [`Request`] (not a `Request` with
/// `id = None`): this means the type system enforces "Notifications
/// don't get responses" — handlers that consume a [`Notification`]
/// can't accidentally return a [`Response`] for it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notification {
    /// `Some("2.0")` per JSON-RPC §4.1; `None` when the peer omitted
    /// the field on the wire. See [`Request::jsonrpc`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jsonrpc: Option<String>,
    /// The notification method name.
    pub method: String,
    /// Notification payload — must be a JSON Array or Object when
    /// present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

/// JSON-RPC 2.0 error object per spec §5 — the body of
/// [`Response::Err`]. The `data` field is optional and carries
/// arbitrary application-specific information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    /// One of the [`ERR_PARSE`] / [`ERR_INVALID_REQUEST`] / … constants
    /// or an application-range value (`-32000` to `-32099` are
    /// reserved for application use).
    pub code: i32,
    /// Human-readable error message.
    pub message: String,
    /// Optional application-defined data — anything from a Sentry id
    /// to a structured validation failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// ---------------------------------------------------------------------------
// Frame — single-frame wire envelope, peer-symmetric
// ---------------------------------------------------------------------------

/// Anything that crosses the wire as one JSON-RPC 2.0 frame, in
/// either direction. JSON-RPC 2.0 is symmetric on the wire — the
/// spec's "Client" and "Server" labels apply to *roles in one RPC
/// call*, not to endpoints. Either peer may send any of these
/// frames.
///
/// **Batching is intentionally not modelled here.** Spec §6's batch
/// format (an array of frames in one message) is a transport-layer
/// framing concern — packing multiple [`Frame`]s into one wire
/// message and unpacking on receipt is the transport's job, not the
/// wire type's. Consumers that need batching split the wire message
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
/// 1. `Stream` — must have `id` AND `stream` (extension; not 2.0).
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
    /// A streaming-lifecycle frame (extension — see [`StreamFrame`]).
    Stream(StreamFrame),
    /// A reply to a [`Request`] this peer (or the other peer) sent.
    Response(Response),
    /// A call from one peer expecting a reply.
    Request(Request),
    /// A one-way call (no reply expected).
    Notification(Notification),
}

// ---------------------------------------------------------------------------
// StreamFrame — proposed JSON-RPC 2.1 streaming extension
// ---------------------------------------------------------------------------

/// One streaming-lifecycle frame, modelled as a phase-per-variant
/// enum so each variant carries exactly the fields its phase
/// requires. The type system enforces the `data` ⇔ `Item` and
/// `error` ⇔ `Error` invariants — there is no malformed
/// `StreamFrame` value to construct in Rust.
///
/// **Not part of JSON-RPC 2.0.** This is the proposed 2.1 streaming
/// extension. The `jsonrpc` field still says `"2.0"` on the wire
/// for backwards compatibility with peers that ignore unknown
/// frames; 2.1-aware peers recognise the `stream` field as the
/// streaming-extension marker.
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
        /// `Some("2.0")` per JSON-RPC §4 / §5; `None` when the peer
        /// omitted the field on the wire. See [`Request::jsonrpc`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        jsonrpc: Option<String>,
        /// Correlates to the originating [`Request`].
        id: Id,
    },
    /// One element of the sender's outgoing half-stream.
    Item {
        /// `Some("2.0")` per JSON-RPC §4 / §5; `None` when the peer
        /// omitted the field on the wire. See [`Request::jsonrpc`].
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
        /// `Some("2.0")` per JSON-RPC §4 / §5; `None` when the peer
        /// omitted the field on the wire. See [`Request::jsonrpc`].
        #[serde(default, skip_serializing_if = "Option::is_none")]
        jsonrpc: Option<String>,
        /// Correlates to the originating [`Request`].
        id: Id,
    },
    /// Abnormal half-close with `error` populated — the sender
    /// failed on its side.
    Error {
        /// `Some("2.0")` per JSON-RPC §4 / §5; `None` when the peer
        /// omitted the field on the wire. See [`Request::jsonrpc`].
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
        /// `Some("2.0")` per JSON-RPC §4 / §5; `None` when the peer
        /// omitted the field on the wire. See [`Request::jsonrpc`].
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

    /// The `jsonrpc` field, regardless of variant. `None` when the
    /// peer omitted it on the wire.
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
// maximum interop; callers who want spec-strict behaviour call this
// directly on the parsed frame.
// ---------------------------------------------------------------------------

/// Check that `jsonrpc` is exactly `"2.0"` per spec §4 / §5.
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
