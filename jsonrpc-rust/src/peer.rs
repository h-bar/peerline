//! Peer-symmetric JSON-RPC 2.0 helpers — replaces the old
//! direction-typed `server` + `client` modules.
//!
//! JSON-RPC 2.0 is symmetric on the wire: the spec's "Client" and
//! "Server" labels apply to roles in a single RPC call, not to
//! endpoints. Real bidirectional protocols on top of JSON-RPC (LSP,
//! DAP, MCP) lean into this — *either* endpoint can initiate a
//! request, send a notification, receive a response. This module
//! provides the building blocks for that symmetric model.
//!
//! ### What this module gives you
//!
//! - [`parse_frame`] — turn an inbound text
//!   frame into a [`Frame`]. Treats the `jsonrpc` version field as
//!   fully optional (`Option<String>`) so peers like the codex
//!   app-server, which omit it on the wire, decode cleanly. Callers
//!   who want spec-strict validation can call
//!   [`wire::validate_version`] on the parsed value themselves.
//! - [`classify`] — turn a parsed [`Frame`] into [`InboundKind`]
//!   for dispatch: this peer's pending Response, an incoming
//!   Request the peer needs to handle, or an incoming Notification.
//! - Builders for outgoing frames: [`request`], [`notification`],
//!   [`response_ok`], [`response_err`], plus value/with_data
//!   variants for direct-`Value` and `data`-carrying paths.
//! - [`RequestIdGen`] — local-unique id allocator for *this* peer's
//!   outgoing requests. Each peer numbers from 1 independently; the
//!   correlation between request and reply is "the id I just gave
//!   out matches the id on the inbound Response."
//!
//! All helpers are pure: no IO, no locks, no async. The
//! state-holding `Peer` *struct* (pending-requests map, handler
//! registry, mutex strategy, async runtime) is intentionally NOT
//! in this crate — that's runtime-dependent. Build it in your
//! transport layer with whatever async primitives you prefer.
//!
//! **Batching is intentionally not handled here.** Spec §6's
//! array-of-frames format is a transport-layer concern (one wire
//! message carrying many frames). Transports that support it split
//! the array into individual frame strings and call [`parse_frame`]
//! on each, collecting [`Response`]s into a reply array if needed.
//!
//! ### Data flow
//!
//! ```text
//! text frame ──► parse_frame ──► Frame
//!                                  │
//!                                  ▼
//!                              classify
//!                                  │
//!     ┌────────────────────────────┼────────────────────────────┐
//!     ▼                            ▼                            ▼
//! InboundKind::Response   InboundKind::IncomingRequest   InboundKind::IncomingNotification
//!  (resolve pending)       (dispatch handler, build      (dispatch handler,
//!                           Response, send it back)       no reply)
//! ```
//!
//! ### Example dispatch (the same shape on both ends)
//!
//! ```ignore
//! let frame = peer::parse_frame(&text)?;
//! match peer::classify(frame) {
//!     InboundKind::Response { id, outcome } => resolve_pending(id, outcome),
//!     InboundKind::IncomingRequest(req) => {
//!         let resp = my_handler(req).await;
//!         send(&serde_json::to_string(&resp)?).await;
//!     }
//!     InboundKind::IncomingNotification(n) => my_notif_handler(n).await,
//! }
//! ```

use crate::wire::{
    ERR_INVALID_REQUEST, ERR_PARSE, Frame, Id, JSONRPC_VERSION, Notification, Request, Response,
    ResponseErr, ResponseOk, RpcError, StreamFrame,
};
use serde::Serialize;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Inbound classification
// ---------------------------------------------------------------------------

/// What an inbound [`Frame`] resolves to from this peer's
/// perspective. The transport's read loop calls [`classify`] on
/// each parsed frame and dispatches based on the variant.
///
/// Pubsub-aware peers pipe [`InboundKind::IncomingNotification`]
/// through [`crate::pubsub::classify`] to recognise subscription
/// pushes; the core layer doesn't bake in that convention.
#[derive(Debug)]
#[non_exhaustive]
pub enum InboundKind {
    /// A reply to one of this peer's pending requests.
    Response {
        /// The request id this response matches. `None` ⇒ wire
        /// carried JSON `null` (parse-error reply per spec §5); the
        /// consumer typically can't match this to any pending caller
        /// and should log it.
        id: Option<Id>,
        /// `Ok(result)` on success, `Err(rpc_error)` on failure.
        outcome: Result<Value, RpcError>,
    },
    /// A request initiated by the other peer — handle it and send
    /// a [`Response`].
    IncomingRequest(Request),
    /// A notification initiated by the other peer — handle it
    /// silently (no reply per spec §4.1).
    IncomingNotification(Notification),
    /// A streaming-lifecycle frame (proposed 2.1 extension — see
    /// [`StreamFrame`]). Route by `frame.id` to the matching stream
    /// in this peer's stream registry, then act on `frame.stream`
    /// (open / item / close / error / cancel).
    Stream(StreamFrame),
}

/// Classify a parsed [`Frame`] for dispatch.
#[must_use]
pub fn classify(frame: Frame) -> InboundKind {
    match frame {
        Frame::Response(r) => {
            let id = r.id().cloned();
            InboundKind::Response {
                id,
                outcome: r.into_outcome(),
            }
        }
        Frame::Request(r) => InboundKind::IncomingRequest(r),
        Frame::Notification(n) => InboundKind::IncomingNotification(n),
        Frame::Stream(s) => InboundKind::Stream(s),
    }
}

// ---------------------------------------------------------------------------
// Inbound parsing
// ---------------------------------------------------------------------------

/// Parse one inbound text frame.
///
/// The `jsonrpc` version field is **fully optional** — peers that omit
/// it (codex app-server, some MCP transports) decode with the
/// `jsonrpc` field as `None`. Callers who want spec-strict version
/// enforcement can still call [`wire::validate_version`] on the
/// returned frame when the field is `Some`.
///
/// Returns:
/// - `Ok(Frame::Request | Notification | Response | Stream)` for a
///   single well-formed JSON object.
/// - `Err(response)` for invalid JSON, or a shape that doesn't match
///   any `Frame` variant (e.g. an array — batching is a transport
///   concern, see below). The returned [`Response`] is the
///   ready-to-serialize reply with `id: null` (per spec §5).
///
/// **Batching**: this parser handles one frame at a time. Transports
/// that support spec §6 batches split the wire array into individual
/// frame strings and call this once per element.
//
// `Response` is ~144 B (above clippy's 128 B `result_large_err`
// threshold), but the error path is one-shot — the caller serializes
// and sends immediately, so boxing here would just add an allocation.
#[allow(clippy::result_large_err)]
pub fn parse_frame(text: &str) -> Result<Frame, Response> {
    let value: Value = serde_json::from_str(text)
        .map_err(|e| response_err(None, ERR_PARSE, format!("parse error: {e}")))?;
    let normalized = normalize_null_id(value);
    let frame: Frame = serde_json::from_value(normalized)
        .map_err(|e| response_err(None, ERR_INVALID_REQUEST, format!("invalid frame: {e}")))?;
    Ok(frame)
}

/// Strip an `id: null` field off an inbound object so the entry is
/// dispatched as a [`Notification`] (no `id`) rather than failing
/// [`Request`] deserialization (which requires non-null `id`).
/// Matches spec §4.1's "Null id is discouraged on requests"
/// recommendation by treating it identically to a missing field.
fn normalize_null_id(value: Value) -> Value {
    if let Value::Object(mut obj) = value {
        if matches!(obj.get("id"), Some(Value::Null)) {
            obj.remove("id");
        }
        Value::Object(obj)
    } else {
        value
    }
}

// ---------------------------------------------------------------------------
// Outgoing builders — request side
// ---------------------------------------------------------------------------

/// Build a [`Request`] for `method` with `params`, tagged with the
/// caller-allocated `id` (any [`Id`] variant). Errors only if
/// `params` fails to serialize. `params` must serialize to a JSON
/// Array or Object per spec §4.2; scalars / booleans / `null` are
/// rejected.
pub fn request<T: Serialize>(
    id: impl Into<Id>,
    method: impl Into<String>,
    params: &T,
) -> Result<Request, serde_json::Error> {
    let value = serde_json::to_value(params)?;
    let typed = params_from_value(value).ok_or_else(|| {
        serde::ser::Error::custom("jsonrpc params must serialize to a JSON Array or Object")
    })?;
    Ok(Request {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
        method: method.into(),
        params: Some(typed),
    })
}

/// Build a parameter-less [`Request`] for `method`.
#[must_use]
pub fn request_no_params(id: impl Into<Id>, method: impl Into<String>) -> Request {
    Request {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
        method: method.into(),
        params: None,
    }
}

/// Build a [`Notification`] — a one-way call per spec §4.1 (no
/// `id`, no response expected). `params` must serialize to a JSON
/// Array or Object.
pub fn notification<T: Serialize>(
    method: impl Into<String>,
    params: &T,
) -> Result<Notification, serde_json::Error> {
    let value = serde_json::to_value(params)?;
    let typed = params_from_value(value).ok_or_else(|| {
        serde::ser::Error::custom("jsonrpc params must serialize to a JSON Array or Object")
    })?;
    Ok(Notification {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        method: method.into(),
        params: Some(typed),
    })
}

/// Build a parameter-less [`Notification`] for `method`.
#[must_use]
pub fn notification_no_params(method: impl Into<String>) -> Notification {
    Notification {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        method: method.into(),
        params: None,
    }
}

// ---------------------------------------------------------------------------
// Outgoing builders — response side
// ---------------------------------------------------------------------------

/// Build a success [`Response`] with the given `id` and serialized
/// `result`. Errors only if `result` fails to serialize. `id` is
/// non-optional per spec §5 — a successful response always knows
/// the request's id. For the parse-error null-id case, use
/// [`response_err`] / [`response_err_with_data`] with `None`.
pub fn response_ok<T: Serialize>(
    id: impl Into<Id>,
    result: &T,
) -> Result<Response, serde_json::Error> {
    Ok(Response::Ok(ResponseOk {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
        result: serde_json::to_value(result)?,
    }))
}

/// Build a success [`Response`] whose `result` is already a
/// `serde_json::Value`. Cannot fail, unlike [`response_ok`].
#[must_use]
pub fn response_ok_value(id: impl Into<Id>, result: Value) -> Response {
    Response::Ok(ResponseOk {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
        result,
    })
}

/// Build an error [`Response`] with the given `id`, JSON-RPC error
/// `code`, and human-readable `message`. Pass `None` for `id` on
/// parse-error / invalid-request replies where the server couldn't
/// recover the original id (the wire field becomes JSON `null` per
/// spec §5).
pub fn response_err(id: Option<Id>, code: i32, message: impl Into<String>) -> Response {
    Response::Err(ResponseErr {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id,
        error: RpcError {
            code,
            message: message.into(),
            data: None,
        },
    })
}

/// Build an error [`Response`] including the spec §5 optional
/// `data` payload.
pub fn response_err_with_data(
    id: Option<Id>,
    code: i32,
    message: impl Into<String>,
    data: Value,
) -> Response {
    Response::Err(ResponseErr {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id,
        error: RpcError {
            code,
            message: message.into(),
            data: Some(data),
        },
    })
}

// ---------------------------------------------------------------------------
// Request id allocator
// ---------------------------------------------------------------------------

/// Allocator for **this peer's** outgoing request ids. Each peer
/// maintains its own; correlation is "the id I issued" matches "the
/// id on the reply I receive." Backed by an [`AtomicU64`] so a
/// single instance can be shared across concurrent callers without
/// external locking.
#[derive(Debug)]
pub struct RequestIdGen {
    next: AtomicU64,
}

impl Default for RequestIdGen {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestIdGen {
    /// Fresh generator starting at id `1` (id `0` is reserved so
    /// `0` can be used as a sentinel in registry maps if desired).
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next raw `u64`. Use [`Self::next_id`] to get a
    /// typed [`Id`] directly.
    #[must_use]
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    /// Allocate the next id as a typed [`Id::Number`].
    #[must_use]
    pub fn next_id(&self) -> Id {
        Id::Number(self.next() as i64)
    }
}

// ---------------------------------------------------------------------------
// Streaming (proposed 2.1 extension)
//
// Builders for [`StreamFrame`] frames keyed by request id. Each peer
// holds its own per-stream state (item channels, half-close flags,
// cancel watchers) outside this library — these helpers only build
// the wire frames. See `workbench/jsonrpc-rust-streaming.md` for the
// extension's full design.
// ---------------------------------------------------------------------------

/// Build a `stream:open` frame — optional ack of streaming intent.
#[must_use]
pub fn stream_open(id: impl Into<Id>) -> StreamFrame {
    StreamFrame::Open {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
    }
}

/// Build a `stream:item` frame carrying one element of the sender's
/// outgoing stream. `seq` is the producer-assigned monotonic
/// sequence number (starts at `1` for the first item). Errors only
/// if `data` fails to serialize.
pub fn stream_item<T: Serialize>(
    id: impl Into<Id>,
    seq: u64,
    data: &T,
) -> Result<StreamFrame, serde_json::Error> {
    Ok(StreamFrame::Item {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
        seq,
        data: serde_json::to_value(data)?,
    })
}

/// Build a `stream:close` frame — graceful half-close, the sender
/// will produce no more items. The other side may still send;
/// stream is `DONE` only when both halves are closed.
#[must_use]
pub fn stream_close(id: impl Into<Id>) -> StreamFrame {
    StreamFrame::Close {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
    }
}

/// Build a `stream:error` frame — abnormal half-close, the sender
/// failed on its side.
#[must_use]
pub fn stream_error(id: impl Into<Id>, code: i32, message: impl Into<String>) -> StreamFrame {
    StreamFrame::Error {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
        error: RpcError {
            code,
            message: message.into(),
            data: None,
        },
    }
}

/// Build a `stream:error` frame including the spec §5 optional
/// `data` payload on the inner error.
#[must_use]
pub fn stream_error_with_data(
    id: impl Into<Id>,
    code: i32,
    message: impl Into<String>,
    data: Value,
) -> StreamFrame {
    StreamFrame::Error {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
        error: RpcError {
            code,
            message: message.into(),
            data: Some(data),
        },
    }
}

/// Build a `stream:cancel` frame — full-close, terminates the
/// stream in both directions immediately and discards anything
/// in-flight. Either peer may send.
#[must_use]
pub fn stream_cancel(id: impl Into<Id>) -> StreamFrame {
    StreamFrame::Cancel {
        jsonrpc: Some(JSONRPC_VERSION.to_string()),
        id: id.into(),
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Convert a `serde_json::Value` into the typed
/// [`crate::wire::Params`] expected by [`Request`] / [`Notification`].
/// Returns `None` for scalars / booleans / `null`, which the spec
/// forbids for `params`.
fn params_from_value(value: Value) -> Option<crate::wire::Params> {
    match value {
        Value::Object(o) => Some(crate::wire::Params::Object(o)),
        Value::Array(a) => Some(crate::wire::Params::Array(a)),
        _ => None,
    }
}
