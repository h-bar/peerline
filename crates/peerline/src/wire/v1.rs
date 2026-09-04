//! v1 wire envelope types.
//!
//! All v1 envelope types live here. The wire frame is a flat JSON
//! object tagged by `ver` (version) and `kind` (envelope shape). Rather
//! than let serde's derived internally-tagged / untagged enum machinery
//! drive the dispatch — which buffers the whole frame into an
//! intermediate representation on every parse and, fatally, cannot
//! capture payloads as [`serde_json::value::RawValue`] — the frame's
//! [`Serialize`] / [`Deserialize`] are hand-written (see [`crate::wire`]).
//! Deserialization funnels through the flat [`WireV1`] view, whose `data`
//! field is captured raw and only parsed at the typed boundary;
//! [`content_from_wire`] then validates per-`kind` and builds the typed
//! [`Content`]. (`args` is captured raw by that view too, but the same
//! mapping step materializes it into a [`Params`] map — the
//! envelope's declared type — so a request payload does pay a
//! `Value` round-trip that a response payload does not.)
//!
//! Wire field names are short — at most 4 chars (`op` / `args` / `data`
//! / `err` / `seq` / `msg`) — but readable.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
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
// RawJson — payload kept in raw serialized form
// ---------------------------------------------------------------------------

/// A JSON payload retained in its raw serialized form.
///
/// Envelope payload fields — `data` on [`StreamFrame`], `result` on
/// [`ResponseOk`] — hold one of these instead of a fully-parsed
/// [`serde_json::Value`]. On the send path the payload is serialized
/// straight to bytes once (no intermediate `Value` tree); on the receive
/// path [`crate::peer::parse_frame`] captures the raw slice without
/// walking it, deferring the single deserialize to the typed consumer via
/// [`Self::deserialize`]. This halves the serde work for large payloads.
///
/// On the wire a `RawJson` is indistinguishable from the equivalent
/// `Value` — it serializes and parses as the same JSON bytes.
#[derive(Debug, Clone)]
pub struct RawJson(Box<RawValue>);

impl RawJson {
    /// Serialize `value` directly into raw JSON in a single pass — no
    /// intermediate [`serde_json::Value`].
    pub fn from_serialize<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        serde_json::value::to_raw_value(value).map(RawJson)
    }

    /// Wrap an already-captured raw JSON slice.
    #[must_use]
    pub(crate) fn from_raw(raw: Box<RawValue>) -> Self {
        RawJson(raw)
    }

    /// The underlying raw JSON text.
    #[must_use]
    pub fn get(&self) -> &str {
        self.0.get()
    }

    /// Deserialize the raw JSON into `T` in a single pass.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.0.get())
    }

    /// Parse the payload into a navigable [`serde_json::Value`]. Prefer
    /// [`Self::deserialize`] for typed access; this is for callers that
    /// want a dynamic tree.
    pub fn to_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::from_str(self.0.get())
    }
}

impl Serialize for RawJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // `RawValue`'s own Serialize emits the captured bytes verbatim
        // under serde_json.
        self.0.serialize(serializer)
    }
}

impl PartialEq for RawJson {
    /// Semantic JSON equality: compares the parsed values, so whitespace
    /// / key-order differences between two encodings of the same payload
    /// don't matter. Falls back to raw-text equality if either side
    /// isn't valid JSON (never happens for well-formed frames).
    fn eq(&self, other: &Self) -> bool {
        match (self.to_value(), other.to_value()) {
            (Ok(a), Ok(b)) => a == b,
            _ => self.get() == other.get(),
        }
    }
}

impl PartialEq<Value> for RawJson {
    fn eq(&self, other: &Value) -> bool {
        matches!(self.to_value(), Ok(ref v) if v == other)
    }
}

// ---------------------------------------------------------------------------
// Content — the kind-dispatch enum (built by the frame's manual Deserialize)
// ---------------------------------------------------------------------------

/// One v1 frame body. On the wire it is discriminated by the `kind`
/// field; in Rust it's the product of [`crate::wire::Frame`]'s
/// hand-written (de)serialization, so this enum carries no serde
/// attributes of its own.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    /// A call expecting a reply.
    Request(Request),
    /// A reply to a [`Request`].
    Response(Response),
    /// A one-way call (no reply expected).
    Notification(Notification),
    /// A streaming-lifecycle frame.
    Stream(StreamFrame),
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// A call from one peer that **expects a response**. `id` is REQUIRED.
/// For one-way calls, use [`Notification`] instead — they're distinct
/// Rust types so direction-typed dispatch is enforced by the type
/// system.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    /// Caller-chosen request id — required.
    pub id: Id,
    /// The operation being invoked.
    pub op: String,
    /// Operation arguments — a JSON Object when present.
    pub args: Option<Params>,
}

// ---------------------------------------------------------------------------
// Response (Ok | Err — mutual exclusion enforced at the type level)
// ---------------------------------------------------------------------------

/// A reply to a [`Request`]. The result/error mutual exclusion is
/// enforced **at the type level**: this is a `Result`-shaped enum, so a
/// single response can only be one or the other. On the wire the
/// presence of `data` vs `err` discriminates; the frame deserializer
/// rejects a `resp` frame carrying both or neither.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    /// Successful invocation — carries the `data` (result) value.
    Ok(ResponseOk),
    /// Failed invocation — carries the `err` (error) object.
    Err(ResponseErr),
}

/// Body of a successful [`Response`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResponseOk {
    /// Echoes the request's `id`. Non-null — successful responses
    /// always know the id.
    pub id: Id,
    /// Success payload, kept raw (wire field `data`).
    pub result: RawJson,
}

/// Body of an error [`Response`]. `id` is `Option<Id>` — `None` ⇒
/// JSON `null` on the wire, used only when the responder couldn't
/// recover the request id from a malformed frame.
#[derive(Debug, Clone, PartialEq)]
pub struct ResponseErr {
    /// Echoes the request's `id`, or `null` if the request was so
    /// malformed the id couldn't be recovered.
    pub id: Option<Id>,
    /// Error payload (wire field `err`).
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
    pub fn result(&self) -> Option<&RawJson> {
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

    /// Consume the response into a Rust [`Result`]. The success payload
    /// is returned raw — deserialize it with [`RawJson::deserialize`].
    pub fn into_outcome(self) -> Result<RawJson, RpcError> {
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
#[derive(Debug, Clone, PartialEq)]
pub struct Notification {
    /// The operation name.
    pub op: String,
    /// Notification arguments — a JSON Object when present.
    pub args: Option<Params>,
}

// ---------------------------------------------------------------------------
// RpcError
// ---------------------------------------------------------------------------

/// Error object — body of [`Response::Err`] and the `error` field of
/// a terminal [`StreamFrame`]. Wire field names: `code` / `msg` /
/// `data` — kept ≤ 4 chars like the envelope fields. This one keeps its
/// derived serde impls: it is small, carries no large payload, and is
/// (de)serialized as a plain nested object (never a tagged-enum
/// discriminant), so `RawValue` capture doesn't apply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-export", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-export", ts(export))]
pub struct RpcError {
    /// The raw integer code. Peerline-internal protocol errors use the
    /// reserved negative range (`-32600`..=`-32603`); applications are
    /// free to pick any other value. Prefer the typed view via
    /// [`Self::error_type`] for pattern-matching.
    pub code: i32,
    /// Human-readable error message.
    #[serde(rename = "msg")]
    pub message: String,
    /// Optional application-defined data.
    //
    // `ts(optional)` is explicit and load-bearing: ts-rs's default
    // `Option<T>` mapping is `T | null` with the *key required*, which is
    // not the wire shape (`data` is omitted entirely when `None`). Don't
    // rely on `skip_serializing_if` translating on its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "ts-export", ts(optional))]
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
#[derive(Debug, Clone, PartialEq)]
pub struct StreamFrame {
    /// Correlates to the originating [`Request`].
    pub id: Id,
    /// Producer-assigned sequence number. `>= 0` for regular items
    /// (the first item, `seq = 0`, implicitly opens the stream;
    /// subsequent items increment monotonically and gaps signal
    /// dropped items). `-1` marks the terminal frame. Values
    /// `<= -2` are reserved.
    pub seq: i64,
    /// Stream element payload, kept raw — typed by the consumer.
    /// Present on regular items; optional on the terminal frame (a
    /// `seq=-1` frame may bundle the last data item).
    pub data: Option<RawJson>,
    /// Optional error payload. Presence on any frame signals that the
    /// stream ended in error — receivers treat the frame as terminal
    /// regardless of `seq`.
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

// ---------------------------------------------------------------------------
// WireV1 — the flat deserialize view + typed mapping
// ---------------------------------------------------------------------------

/// The four envelope shapes, keyed on the wire `kind` tag. A plain
/// externally-tagged unit enum — deserializes straight from the tag
/// string, rejecting anything else.
#[derive(Deserialize)]
enum WireKind {
    #[serde(rename = "req")]
    Request,
    #[serde(rename = "resp")]
    Response,
    #[serde(rename = "notif")]
    Notification,
    #[serde(rename = "stream")]
    Stream,
}

/// Flat wire view of a v1 frame: every field any envelope can carry,
/// with `args` / `data` captured raw. This is a plain struct (not a
/// tagged enum), so serde_json records the raw payload slices without
/// buffering the frame into an intermediate `Value`. [`content_from_wire`]
/// validates per-`kind` and moves the fields into the typed [`Content`].
///
/// `deny_unknown_fields` rejects stray keys; per-`kind` field
/// requirements (which fields are mandatory / forbidden for each shape)
/// are enforced in [`content_from_wire`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireV1 {
    ver: String,
    kind: WireKind,
    /// `None` ⇒ key absent; `Some(None)` ⇒ explicit `null`;
    /// `Some(Some(n))` ⇒ a number. The distinction matters: an error
    /// response may carry `id: null`, but absent-vs-null-vs-number is
    /// validated per kind.
    #[serde(default, deserialize_with = "double_option")]
    id: Option<Option<Id>>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    seq: Option<i64>,
    #[serde(default)]
    args: Option<Box<RawValue>>,
    // NB: `present_raw`, not a plain `Option<Box<RawValue>>` — a present
    // `"data": null` (e.g. a `call::<_, ()>` reply, whose payload
    // serializes to `null`) must stay `Some(RawValue("null"))`. A plain
    // `Option` collapses JSON `null` to `None`, which would make a valid
    // Ok response look like it carries neither `data` nor `err` and get
    // rejected — deadlocking the caller's waiter. `#[serde(default)]` still
    // yields `None` for an absent key.
    #[serde(default, deserialize_with = "present_raw")]
    data: Option<Box<RawValue>>,
    #[serde(default)]
    err: Option<RpcError>,
}

/// Capture a *present* field as raw JSON even when its value is `null`,
/// so `"data": null` becomes `Some(RawValue("null"))` instead of the
/// `None` a plain `Option<Box<RawValue>>` would produce. Paired with
/// `#[serde(default)]`, an *absent* key is still `None` — this only runs
/// when the key is present.
fn present_raw<'de, D>(deserializer: D) -> Result<Option<Box<RawValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

/// `deserialize_with` helper that distinguishes an absent key (serde's
/// `default` → `None`) from a present `null` (`Some(None)`).
fn double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::deserialize(deserializer)?))
}

/// Map a validated [`WireV1`] into the typed [`Content`], enforcing the
/// per-`kind` field contract. Returns a message on any violation
/// (unknown version, missing/forbidden field, non-object args, a `resp`
/// carrying both or neither of `data`/`err`); [`crate::wire::Frame`]'s
/// `Deserialize` turns it into a serde error.
pub(crate) fn content_from_wire(w: WireV1) -> Result<Content, String> {
    if w.ver != "1" {
        return Err(format!("unsupported wire version {:?}", w.ver));
    }
    match w.kind {
        WireKind::Request => {
            if w.seq.is_some() || w.data.is_some() || w.err.is_some() {
                return Err("request must not carry seq/data/err".to_owned());
            }
            let id =
                w.id.flatten()
                    .ok_or_else(|| "request requires a numeric id".to_owned())?;
            let op = w.op.ok_or_else(|| "request requires op".to_owned())?;
            let args = w.args.as_deref().map(raw_to_params).transpose()?;
            Ok(Content::Request(Request { id, op, args }))
        }
        WireKind::Notification => {
            if w.id.is_some() {
                return Err("notification must not carry id".to_owned());
            }
            if w.seq.is_some() || w.data.is_some() || w.err.is_some() {
                return Err("notification must not carry seq/data/err".to_owned());
            }
            let op = w.op.ok_or_else(|| "notification requires op".to_owned())?;
            let args = w.args.as_deref().map(raw_to_params).transpose()?;
            Ok(Content::Notification(Notification { op, args }))
        }
        WireKind::Response => {
            if w.op.is_some() || w.seq.is_some() || w.args.is_some() {
                return Err("response must not carry op/seq/args".to_owned());
            }
            match (w.data, w.err) {
                (Some(_), Some(_)) => Err("response carries both data and err".to_owned()),
                (None, None) => Err("response carries neither data nor err".to_owned()),
                (Some(data), None) => {
                    let id =
                        w.id.flatten()
                            .ok_or_else(|| "ok response requires a numeric id".to_owned())?;
                    Ok(Content::Response(Response::Ok(ResponseOk {
                        id,
                        result: RawJson::from_raw(data),
                    })))
                }
                (None, Some(err)) => {
                    // The `id` key must be present (numeric or explicit
                    // null); absent is rejected. `null` ⇒ id `None`.
                    let id =
                        w.id.ok_or_else(|| "error response requires an id field".to_owned())?;
                    Ok(Content::Response(Response::Err(ResponseErr {
                        id,
                        error: err,
                    })))
                }
            }
        }
        WireKind::Stream => {
            if w.op.is_some() || w.args.is_some() {
                return Err("stream frame must not carry op/args".to_owned());
            }
            let id =
                w.id.flatten()
                    .ok_or_else(|| "stream frame requires a numeric id".to_owned())?;
            let seq = w
                .seq
                .ok_or_else(|| "stream frame requires seq".to_owned())?;
            Ok(Content::Stream(StreamFrame {
                id,
                seq,
                data: w.data.map(RawJson::from_raw),
                error: w.err,
            }))
        }
    }
}

/// Parse a raw `args` payload into typed [`Params`], requiring a JSON
/// Object (peerline doesn't support positional params).
fn raw_to_params(raw: &RawValue) -> Result<Params, String> {
    match serde_json::from_str::<Value>(raw.get()) {
        Ok(Value::Object(o)) => Ok(o),
        Ok(_) => Err("args must be a JSON object".to_owned()),
        Err(e) => Err(e.to_string()),
    }
}
