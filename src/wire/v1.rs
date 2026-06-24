//! v1 wire envelope types.
//!
//! All v1 envelope types live here. They're keyed under [`Content`]'s
//! `kind` tag, which is itself routed by the parent
//! [`crate::wire::Frame`] version dispatch. Wire field names are
//! short — at most 4 chars (`op` / `args` / `data` / `err` / `seq` /
//! `msg`) — but readable; single-char tokens were too cryptic.
//! Where Rust and wire names match (`op`, `args`, `id`, `seq`,
//! `data`, `code`) no rename is needed; the rest use
//! `#[serde(rename = "…")]`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

// ---------------------------------------------------------------------------
// ErrorType — typed view over the integer `code` field on `RpcError`.
//
// The wire still carries an `i32` code (compact, freeform for app errors);
// Rust callers use the typed view via `RpcError::kind()` for exhaustive
// pattern-matching. peerline-internal protocol errors live in a reserved
// negative range so they don't collide with application codes.
// ---------------------------------------------------------------------------

/// Typed view over an [`RpcError`]'s `code` field. Library-internal
/// protocol errors map to named variants; any other integer maps to
/// [`ErrorType::Application`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    /// The frame couldn't be decoded (parse error, unknown version,
    /// unknown variant, missing/extra fields). Code `-32600`.
    InvalidRequest,
    /// No handler is registered for the requested method. Code `-32601`.
    MethodNotFound,
    /// Params failed to deserialize into the handler's expected type.
    /// Code `-32602`.
    InvalidParams,
    /// Internal protocol error (e.g. the connection's outbound side
    /// closed before a response arrived). Code `-32603`.
    Internal,
    /// Application-defined error code (anything not in the reserved
    /// peerline range).
    Application(i32),
}

impl From<i32> for ErrorType {
    fn from(code: i32) -> Self {
        match code {
            -32600 => ErrorType::InvalidRequest,
            -32601 => ErrorType::MethodNotFound,
            -32602 => ErrorType::InvalidParams,
            -32603 => ErrorType::Internal,
            n => ErrorType::Application(n),
        }
    }
}

impl From<ErrorType> for i32 {
    fn from(t: ErrorType) -> i32 {
        match t {
            ErrorType::InvalidRequest => -32600,
            ErrorType::MethodNotFound => -32601,
            ErrorType::InvalidParams => -32602,
            ErrorType::Internal => -32603,
            ErrorType::Application(n) => n,
        }
    }
}

// ---------------------------------------------------------------------------
// Id and Params aliases
// ---------------------------------------------------------------------------

/// Request / response identifier. Each peer allocates ids monotonically
/// via [`crate::peer::RequestIdGen`]; the wire carries them as JSON
/// numbers.
pub type Id = u64;

/// Method parameters. Always a JSON object — peerline doesn't support
/// positional-array params. Callers pass a typed struct that serializes
/// to an object, or a [`serde_json::Map`] directly.
pub type Params = Map<String, Value>;

// ---------------------------------------------------------------------------
// Content — the kind-dispatch enum, keyed on `t`
// ---------------------------------------------------------------------------

/// One v1 frame body, discriminated by the `kind` field on the wire.
/// Rust variant names match today's [`Request`] / [`Response`] /
/// [`Notification`] / [`StreamFrame`] envelopes so pattern-match
/// call sites don't churn; per-variant `#[serde(rename)]` gives the
/// compact wire tag values (`"req"`, `"resp"`, `"notif"`, `"stream"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Content {
    /// A call expecting a reply.
    #[serde(rename = "req")]
    Request(Request),
    /// A reply to a [`Request`].
    #[serde(rename = "resp")]
    Response(Response),
    /// A one-way call (no reply expected).
    #[serde(rename = "notif")]
    Notification(Notification),
    /// A streaming-lifecycle frame.
    #[serde(rename = "stream")]
    Stream(StreamFrame),
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// A call from one peer that **expects a response**. `id` is REQUIRED.
/// For one-way calls, use [`Notification`] instead — they're distinct
/// Rust types so direction-typed dispatch is enforced by the type
/// system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Caller-chosen request id — required.
    pub id: Id,
    /// The operation being invoked.
    pub op: String,
    /// Operation arguments — must be a JSON Object when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Params>,
}

// ---------------------------------------------------------------------------
// Response (Ok | Err — mutual exclusion enforced at the type level)
// ---------------------------------------------------------------------------

/// A reply to a [`Request`]. The result/error mutual exclusion is
/// enforced **at the type level**: this is a `Result`-shaped enum,
/// so a single response can only be one or the other.
///
/// `#[serde(untagged)]` keeps the variants invisible on the wire:
/// the presence of `data` vs `err` discriminates. Both inner structs use
/// `deny_unknown_fields` so a frame that has both — or neither — fails
/// to deserialize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Response {
    /// Successful invocation — carries the `data` (result) value.
    Ok(ResponseOk),
    /// Failed invocation — carries the `err` (error) object.
    Err(ResponseErr),
}

/// Body of a successful [`Response`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseOk {
    /// Echoes the request's `id`. Non-null — successful responses
    /// always know the id.
    pub id: Id,
    /// Success payload.
    #[serde(rename = "data")]
    pub result: Value,
}

/// Body of an error [`Response`]. `id` is `Option<Id>` — `None` ⇒
/// JSON `null` on the wire, used only when the responder couldn't
/// recover the request id from a malformed frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseErr {
    /// Echoes the request's `id`, or `null` if the request was so
    /// malformed the id couldn't be recovered.
    pub id: Option<Id>,
    /// Error payload.
    #[serde(rename = "err")]
    pub error: RpcError,
}

impl Response {
    /// The response's id, regardless of variant. `Some` for [`Response::Ok`]
    /// (always non-null by type) and for [`Response::Err`] with a recovered
    /// id; `None` only for an `Err` response built for the malformed-request
    /// case (wire `id: null`).
    #[must_use]
    pub fn id(&self) -> Option<&Id> {
        match self {
            Response::Ok(r) => Some(&r.id),
            Response::Err(r) => r.id.as_ref(),
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

    /// Consume the response into a Rust [`Result`].
    pub fn into_outcome(self) -> Result<Value, RpcError> {
        match self {
            Response::Ok(r) => Ok(r.result),
            Response::Err(r) => Err(r.error),
        }
    }
}

// ---------------------------------------------------------------------------
// Notification
// ---------------------------------------------------------------------------

/// A one-way call without an id — no reply expected, the other peer
/// MUST NOT send one. A separate Rust type from [`Request`] so the
/// type system enforces "Notifications don't get responses."
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notification {
    /// The operation name.
    pub op: String,
    /// Notification arguments — must be a JSON Object when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Params>,
}

// ---------------------------------------------------------------------------
// RpcError
// ---------------------------------------------------------------------------

/// Error object — body of [`Response::Err`] and the `error` field of
/// a terminal [`StreamFrame`]. Wire field names: `code` / `msg` /
/// `data` — kept ≤ 4 chars like the envelope fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    /// The raw integer code. Peerline-internal protocol errors use the
    /// reserved negative range (`-32600`..=`-32603`); applications are
    /// free to pick any other value. Prefer the typed view via
    /// [`Self::kind`] for pattern-matching.
    pub code: i32,
    /// Human-readable error message.
    #[serde(rename = "msg")]
    pub message: String,
    /// Optional application-defined data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    /// Typed view of the integer [`code`](Self::code) — exhaustive
    /// pattern-matching on protocol vs application errors.
    /// Equivalent to `ErrorType::from(self.code)`.
    #[must_use]
    pub fn error_type(&self) -> ErrorType {
        self.code.into()
    }
}

// ---------------------------------------------------------------------------
// StreamFrame — flat struct, lifecycle encoded in `seq`
// ---------------------------------------------------------------------------

/// One stream frame. Lifecycle is encoded in `seq`:
///
/// - `seq >= 0` — a regular stream item (the first, `seq = 0`,
///   implicitly opens the stream; items are 0-indexed and monotonic;
///   gaps signal dropped items).
/// - `seq == -1` — the **terminal** frame: no more data is coming from
///   the producer for this stream. May carry `data` (last item bundled
///   with the terminal marker) and/or `error` (stream ended in error).
///
/// All frames correlate to the originating [`Request`] by `id`.
/// Receivers treat any frame whose `seq == -1` *or* whose `error` is
/// present as terminal — once the receiver sees either, the stream is
/// over.
///
/// ```jsonc
/// // regular items (0-indexed)
/// {"ver":"1", "kind":"stream", "id":7, "seq":0,  "data":{...}}
/// {"ver":"1", "kind":"stream", "id":7, "seq":1,  "data":{...}}
/// // empty terminal — normal end with no final item
/// {"ver":"1", "kind":"stream", "id":7, "seq":-1}
/// // terminal carrying the last item
/// {"ver":"1", "kind":"stream", "id":7, "seq":-1, "data":{...}}
/// // terminal carrying an error (stream ended abnormally)
/// {"ver":"1", "kind":"stream", "id":7, "seq":-1, "err":{"code":-32000,"msg":"boom"}}
/// ```
///
/// There is no separate Open / Close / Cancel frame. The first
/// `seq=0` Item implicitly opens the stream. A `seq=-1` frame ends it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamFrame {
    /// Correlates to the originating [`Request`].
    pub id: Id,
    /// Producer-assigned sequence number. `>= 0` for regular items
    /// (the first item, `seq = 0`, implicitly opens the stream;
    /// subsequent items increment monotonically and gaps signal
    /// dropped items). `-1` marks the terminal frame. Values
    /// `<= -2` are reserved.
    pub seq: i64,
    /// Stream element payload — typed by the consumer. Present on
    /// regular items; optional on the terminal frame (a `seq=-1`
    /// frame may bundle the last data item).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// Optional error payload. Presence on any frame signals that the
    /// stream ended in error — receivers treat the frame as terminal
    /// regardless of `seq`.
    #[serde(rename = "err", default, skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl StreamFrame {
    /// The `id` field. Convenience accessor mirroring the other frame types.
    #[must_use]
    pub fn id(&self) -> &Id {
        &self.id
    }

    /// `true` if this frame ends the stream — either `seq == -1` or
    /// `error` is set.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.seq == -1 || self.error.is_some()
    }
}

/// The sentinel `seq` value that marks the terminal frame.
pub const STREAM_TERMINAL_SEQ: i64 = -1;
