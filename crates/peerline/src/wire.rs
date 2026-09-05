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

// Flat wire-view mirrors for TypeScript export. Build-time only —
// enabled by the `ts-export` feature, never used at runtime. (Docs
// live in the module itself; a duplicate outer doc here would break
// its intra-doc links.)
#[cfg(feature = "ts-export")]
pub mod ts;

pub use v1::{
    Content, ErrorType, Id, Notification, Params, RawJson, Request, Response, ResponseErr,
    ResponseOk, RpcError, STREAM_TERMINAL_SEQ, StreamFrame,
};

use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Outer wire envelope — version-tagged. Today only v1 exists; future
/// wire versions land as additional variants (`V2(v2::Content)` etc.)
/// in this enum.
///
/// [`Serialize`] / [`Deserialize`] are hand-written rather than derived:
/// serde's internally-tagged (`ver` / `kind`) and untagged
/// (`Response`) machinery buffers the whole frame into an intermediate
/// representation on every parse and cannot capture payloads as
/// [`serde_json::value::RawValue`]. The manual impls dispatch on the
/// tags directly and funnel deserialization through the flat
/// `v1::WireV1` view, so the `data` payload is captured raw and parsed
/// once, at the typed boundary.
// `non_exhaustive` is what makes the documented promise true: adding a
// `V2(v2::Content)` variant must be purely additive, which an exhaustive
// public enum would turn into a semver break for matchers.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// v1 frame (the current and only version).
    V1(v1::Content),
}

impl Serialize for Frame {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Frame::V1(content) = self;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("ver", "1")?;
        match content {
            Content::Request(r) => {
                map.serialize_entry("kind", "req")?;
                map.serialize_entry("id", &r.id)?;
                map.serialize_entry("op", &r.op)?;
                if let Some(args) = &r.args {
                    map.serialize_entry("args", args)?;
                }
            }
            Content::Response(Response::Ok(r)) => {
                map.serialize_entry("kind", "resp")?;
                map.serialize_entry("id", &r.id)?;
                map.serialize_entry("data", &r.result)?;
            }
            Content::Response(Response::Err(r)) => {
                map.serialize_entry("kind", "resp")?;
                // Emitted even when `None` → `id: null` on the wire.
                map.serialize_entry("id", &r.id)?;
                map.serialize_entry("err", &r.error)?;
            }
            Content::Notification(n) => {
                map.serialize_entry("kind", "notif")?;
                map.serialize_entry("op", &n.op)?;
                if let Some(args) = &n.args {
                    map.serialize_entry("args", args)?;
                }
            }
            Content::Stream(s) => {
                map.serialize_entry("kind", "stream")?;
                map.serialize_entry("id", &s.id)?;
                map.serialize_entry("seq", &s.seq)?;
                if let Some(data) = &s.data {
                    map.serialize_entry("data", data)?;
                }
                if let Some(err) = &s.error {
                    map.serialize_entry("err", err)?;
                }
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Frame {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = v1::WireV1::deserialize(deserializer)?;
        let content = v1::content_from_wire(wire).map_err(serde::de::Error::custom)?;
        Ok(Frame::V1(content))
    }
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
