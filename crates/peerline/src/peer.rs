//! peerline peer helpers — symmetric dispatch primitives.
//!
//! peerline is symmetric on the wire: there is no "client" or
//! "server" role at the protocol level. Both endpoints can
//! initiate requests, send notifications, receive responses, and
//! open streams. This module gives you the building blocks for
//! that symmetric model.
//!
//! ### What this module gives you
//!
//! - [`parse_frame`] — turn an inbound text frame into a [`Frame`]
//!   in one serde call. Version dispatch (`ver`) and kind dispatch
//!   (`kind`) happen inside the deserialize.
//! - [`classify`] — turn a parsed [`Frame`] into [`InboundKind`]
//!   for routing: this peer's pending Response, an incoming Request
//!   the peer needs to handle, an incoming Notification, or a
//!   Stream frame.
//! - Builders for outgoing frames: [`request`], [`notification`],
//!   [`response_ok`], [`response_err`], plus value/with_data
//!   variants and the stream builders ([`stream_item`] and the
//!   [`stream_terminal`] family).
//! - [`RequestIdGen`] — local-unique id allocator for *this* peer's
//!   outgoing requests.
//!
//! All helpers are pure: no IO, no locks, no async. The
//! state-holding `Peer` struct (pending-requests map, handler
//! registry, async runtime) lives in [`crate::runtime`].
//!
//! ### Data flow
//!
//! ```text
//! text frame ──► parse_frame ──► Frame::V1(Content::*)
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

use crate::wire::{
    Content, ErrorType, Frame, Id, Notification, Params, RawJson, Request, Response, ResponseErr,
    ResponseOk, RpcError, STREAM_TERMINAL_SEQ, StreamFrame,
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
#[derive(Debug)]
#[non_exhaustive]
pub enum InboundKind {
    /// A reply to one of this peer's pending requests.
    Response {
        /// The request id this response matches. `None` ⇒ wire
        /// carried JSON `null` (a parse-error reply for which the
        /// responder couldn't recover the id).
        id: Option<Id>,
        /// `Ok(result)` on success, `Err(rpc_error)` on failure. The
        /// success payload is raw — deserialize it with
        /// [`RawJson::deserialize`].
        outcome: Result<RawJson, RpcError>,
    },
    /// A request initiated by the other peer — handle it and send
    /// a [`Response`].
    IncomingRequest(Request),
    /// A notification initiated by the other peer — handle it
    /// silently, no reply.
    IncomingNotification(Notification),
    /// A streaming-lifecycle frame. Route by `frame.id()` to the
    /// matching stream in this peer's stream registry.
    Stream(StreamFrame),
}

/// Classify a parsed [`Frame`] for dispatch.
#[must_use]
pub fn classify(frame: Frame) -> InboundKind {
    match frame {
        Frame::V1(content) => match content {
            Content::Response(r) => {
                let id = r.id().copied();
                InboundKind::Response {
                    id,
                    outcome: r.into_outcome(),
                }
            }
            Content::Request(r) => InboundKind::IncomingRequest(r),
            Content::Notification(n) => InboundKind::IncomingNotification(n),
            Content::Stream(s) => InboundKind::Stream(s),
        },
    }
}

// ---------------------------------------------------------------------------
// Inbound parsing
// ---------------------------------------------------------------------------

/// Parse one inbound text frame.
///
/// Returns:
/// - `Ok(Frame)` for a single well-formed JSON frame.
/// - `Err(response)` for invalid JSON or any shape that doesn't
///   match an expected variant (wrong `ver`, unknown `kind`, missing
///   required field, etc.). The returned [`Response`] is the
///   ready-to-serialize error reply with `id: null`.
///
/// **Batching**: this parser handles one frame at a time. Transports
/// that pack multiple frames into one wire message split the array
/// into individual frame strings and call this once per element.
//
// `Response` is ~144 B (above clippy's 128 B `result_large_err`
// threshold), but the error path is one-shot — caller serializes
// and sends immediately, so boxing here would just add an allocation.
#[allow(clippy::result_large_err)]
pub fn parse_frame(text: &str) -> Result<Frame, Response> {
    serde_json::from_str(text).map_err(|e| {
        response_err(
            None,
            ErrorType::InvalidRequest,
            format!("invalid frame: {e}"),
        )
    })
}

// ---------------------------------------------------------------------------
// Outgoing builders — request side
// ---------------------------------------------------------------------------

/// Build a [`Request`] for `op` with `args`. Errors only if `args`
/// fails to serialize or doesn't serialize to a JSON Object.
pub fn request<T: Serialize>(
    id: impl Into<Id>,
    op: impl Into<String>,
    args: &T,
) -> Result<Request, serde_json::Error> {
    let value = serde_json::to_value(args)?;
    let typed = params_from_value(value).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom("args must serialize to a JSON Object")
    })?;
    Ok(Request {
        id: id.into(),
        op: op.into(),
        args: Some(typed),
    })
}

/// Build an argument-less [`Request`] for `op`.
#[must_use]
pub fn request_no_args(id: impl Into<Id>, op: impl Into<String>) -> Request {
    Request {
        id: id.into(),
        op: op.into(),
        args: None,
    }
}

/// Build a [`Notification`]. Errors only if `args` fails to
/// serialize or doesn't serialize to a JSON Object.
pub fn notification<T: Serialize>(
    op: impl Into<String>,
    args: &T,
) -> Result<Notification, serde_json::Error> {
    let value = serde_json::to_value(args)?;
    let typed = params_from_value(value).ok_or_else(|| {
        <serde_json::Error as serde::ser::Error>::custom("args must serialize to a JSON Object")
    })?;
    Ok(Notification {
        op: op.into(),
        args: Some(typed),
    })
}

/// Build an argument-less [`Notification`] for `op`.
#[must_use]
pub fn notification_no_args(op: impl Into<String>) -> Notification {
    Notification {
        op: op.into(),
        args: None,
    }
}

// ---------------------------------------------------------------------------
// Outgoing builders — response side
// ---------------------------------------------------------------------------

/// Build a success [`Response`] with `result`.
pub fn response_ok<T: Serialize>(
    id: impl Into<Id>,
    result: &T,
) -> Result<Response, serde_json::Error> {
    Ok(Response::Ok(ResponseOk {
        id: id.into(),
        result: RawJson::from_serialize(result)?,
    }))
}

/// Build a success [`Response`] whose `result` is already serialized
/// into a [`RawJson`]. Lets the runtime forward a handler's result
/// without a `Value` round-trip.
#[must_use]
pub fn response_ok_raw(id: impl Into<Id>, result: RawJson) -> Response {
    Response::Ok(ResponseOk {
        id: id.into(),
        result,
    })
}

/// Build an error [`Response`]. Pass `None` for `id` on parse-error
/// replies where the responder couldn't recover the original id.
pub fn response_err(id: Option<Id>, code: impl Into<i32>, message: impl Into<String>) -> Response {
    Response::Err(ResponseErr {
        id,
        error: RpcError {
            code: code.into(),
            message: message.into(),
            data: None,
        },
    })
}

/// Build an error [`Response`] from a full [`RpcError`] — unlike
/// [`response_err`], preserves the error's `data` payload. This is
/// what the runtime uses to relay a handler's error verbatim.
#[must_use]
pub fn response_err_rpc(id: Option<Id>, error: RpcError) -> Response {
    Response::Err(ResponseErr { id, error })
}

/// Build an error [`Response`] with an attached `data` payload.
pub fn response_err_with_data(
    id: Option<Id>,
    code: impl Into<i32>,
    message: impl Into<String>,
    data: Value,
) -> Response {
    Response::Err(ResponseErr {
        id,
        error: RpcError {
            code: code.into(),
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
/// id on the reply I receive."
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
    /// Fresh generator starting at id `1` (id `0` reserved as a
    /// sentinel for registry maps).
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next raw `u64`. Equivalent to [`Self::next_id`]
    /// since [`Id`] is `u64`; kept for source compatibility.
    #[must_use]
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    /// Allocate the next id.
    #[must_use]
    pub fn next_id(&self) -> Id {
        self.next()
    }
}

// ---------------------------------------------------------------------------
// Streaming builders
// ---------------------------------------------------------------------------

/// Build a stream item frame (non-terminal). `seq` is the
/// producer-assigned monotonic sequence number, starting at `0`
/// (the first item implicitly opens the stream).
/// Non-consecutive `seq`s between items signal drops the producer
/// advanced past (see [`crate::runtime::StreamSender::skip`]).
pub fn stream_item<T: Serialize>(
    id: impl Into<Id>,
    seq: u64,
    data: &T,
) -> Result<StreamFrame, serde_json::Error> {
    Ok(StreamFrame {
        id: id.into(),
        seq: seq as i64,
        data: Some(RawJson::from_serialize(data)?),
        error: None,
    })
}

/// Build a terminal frame (`seq = -1`) with no payload — normal
/// end-of-stream.
#[must_use]
pub fn stream_terminal(id: impl Into<Id>) -> StreamFrame {
    StreamFrame {
        id: id.into(),
        seq: STREAM_TERMINAL_SEQ,
        data: None,
        error: None,
    }
}

/// Build a terminal frame carrying the last item. Equivalent to
/// emitting one final item and the end-of-stream marker in a single
/// frame.
pub fn stream_terminal_with_data<T: Serialize>(
    id: impl Into<Id>,
    data: &T,
) -> Result<StreamFrame, serde_json::Error> {
    Ok(StreamFrame {
        id: id.into(),
        seq: STREAM_TERMINAL_SEQ,
        data: Some(RawJson::from_serialize(data)?),
        error: None,
    })
}

/// Build a terminal frame carrying an error — stream ended abnormally.
#[must_use]
pub fn stream_terminal_with_error(
    id: impl Into<Id>,
    code: impl Into<i32>,
    message: impl Into<String>,
) -> StreamFrame {
    StreamFrame {
        id: id.into(),
        seq: STREAM_TERMINAL_SEQ,
        data: None,
        error: Some(RpcError {
            code: code.into(),
            message: message.into(),
            data: None,
        }),
    }
}

/// Build a terminal frame carrying a full [`RpcError`] — unlike
/// [`stream_terminal_with_error`], preserves the error's `data`
/// payload. This is what the runtime uses to relay a stream
/// handler's error verbatim.
#[must_use]
pub fn stream_terminal_with_rpc_error(id: impl Into<Id>, error: RpcError) -> StreamFrame {
    StreamFrame {
        id: id.into(),
        seq: STREAM_TERMINAL_SEQ,
        data: None,
        error: Some(error),
    }
}

/// Build a terminal frame carrying an error with an attached `data`
/// payload on the inner [`RpcError`].
#[must_use]
pub fn stream_terminal_with_error_data(
    id: impl Into<Id>,
    code: impl Into<i32>,
    message: impl Into<String>,
    data: Value,
) -> StreamFrame {
    StreamFrame {
        id: id.into(),
        seq: STREAM_TERMINAL_SEQ,
        data: None,
        error: Some(RpcError {
            code: code.into(),
            message: message.into(),
            data: Some(data),
        }),
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Convert a [`serde_json::Value`] into the typed [`Params`] expected
/// by [`Request`] / [`Notification`]. Returns `None` for anything
/// that isn't a JSON Object.
fn params_from_value(value: Value) -> Option<Params> {
    match value {
        Value::Object(o) => Some(o),
        _ => None,
    }
}
