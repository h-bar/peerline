//! peerline wire types — version-tagged frame envelopes.
//!
//! Every frame on the wire is a [`Frame`], internally tagged by `ver`
//! (version). Today only v1 exists; future wire versions land as
//! additional [`Frame`] variants (`V2(v2::Content)` etc.) without
//! touching v1.
//!
//! ### Layering
//!
//! - [`Frame`] — outer version dispatch. Serde reads `ver` and routes
//!   to the matching per-version content enum.
//! - [`v1::Content`] — inner kind dispatch, keyed on `kind`
//!   (`"req"` / `"resp"` / `"notif"` / `"stream"`).
//! - [`v1::Request`] / [`v1::Response`] / [`v1::Notification`] /
//!   [`v1::StreamFrame`] — the envelope shapes themselves. Wire
//!   field names are ≤ 4 chars (`op` / `args` / `data` / `err` /
//!   `seq` / `msg`) but readable; Rust field names mostly match.
//!
//! Commonly used v1 types are re-exported at this module's root so
//! callers can write `peerline::wire::Request` rather than
//! `peerline::wire::v1::Request`.
//!
//! ### Wire format
//!
//! ```jsonc
//! // unary request
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

pub mod v1;

pub use v1::{
    Content, ErrorType, Id, Notification, Params, Request, Response, ResponseErr, ResponseOk,
    RpcError, STREAM_TERMINAL_SEQ, StreamFrame,
};

use serde::{Deserialize, Serialize};

/// Outer wire envelope — version-tagged. Today only v1 exists; future
/// wire versions land as additional variants (`V2(v2::Content)` etc.)
/// in this enum without touching v1.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "ver")]
pub enum Frame {
    /// v1 frame (the current and only version).
    #[serde(rename = "1")]
    V1(v1::Content),
}

impl Frame {
    /// The frame's `id`, regardless of variant — `None` for
    /// notifications and error responses with `id: null`, `Some`
    /// otherwise.
    #[must_use]
    pub fn id(&self) -> Option<&Id> {
        match self {
            Frame::V1(Content::Request(r)) => Some(&r.id),
            Frame::V1(Content::Response(r)) => r.id(),
            Frame::V1(Content::Notification(_)) => None,
            Frame::V1(Content::Stream(s)) => Some(s.id()),
        }
    }
}

// ---------------------------------------------------------------------------
// From impls — let callers write `req.into()` or `Frame::from(req)`
// instead of `Frame::V1(Content::Request(req))`.
// ---------------------------------------------------------------------------

impl From<Request> for Frame {
    fn from(r: Request) -> Self {
        Frame::V1(Content::Request(r))
    }
}

impl From<Response> for Frame {
    fn from(r: Response) -> Self {
        Frame::V1(Content::Response(r))
    }
}

impl From<Notification> for Frame {
    fn from(n: Notification) -> Self {
        Frame::V1(Content::Notification(n))
    }
}

impl From<StreamFrame> for Frame {
    fn from(s: StreamFrame) -> Self {
        Frame::V1(Content::Stream(s))
    }
}
