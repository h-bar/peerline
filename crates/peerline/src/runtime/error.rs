//! Error type for runtime-level Peer operations.

use crate::wire::RpcError;
use thiserror::Error;

/// Errors surfaced by [`super::Peer`] methods.
// `non_exhaustive`: the runtime may grow failure classes without a
// semver break.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    /// The peer returned an RPC error response.
    #[error("rpc error: {0:?}")]
    Rpc(RpcError),
    /// (De)serialization of params / result failed.
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    /// The connection's outbound side is closed — the peer can't
    /// send anything further. Also returned by `call` / `call_stream`
    /// issued after the connection ended; a request already in flight
    /// when it ended instead surfaces as [`Error::Rpc`] with code
    /// `-32603` and message "connection closed".
    #[error("connection closed")]
    Closed,
    /// A pending request was abandoned because the connection
    /// dropped before its reply arrived.
    #[error("request abandoned (connection dropped)")]
    Abandoned,
    /// A method that was supposed to return params-as-Object/Array
    /// got a scalar / null / bool. Reflected from
    /// [`crate::peer::request`].
    #[error("params: {0}")]
    Params(String),
}

impl From<RpcError> for Error {
    fn from(e: RpcError) -> Self {
        Error::Rpc(e)
    }
}

/// A frame the dispatch loop could not parse or could not route to a
/// waiting caller. None of these can be surfaced through a `call` /
/// `call_stream` return value — by definition there is no caller to
/// return them to — so without a [`Peer::on_protocol_error`] hook they
/// are invisible.
///
/// [`Peer::on_protocol_error`]: super::Peer::on_protocol_error
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ProtocolError {
    /// An inbound frame failed to parse. The peer has already replied
    /// with an `id: null` error response; this reports the local view.
    MalformedFrame {
        /// The parser's message.
        message: String,
    },
    /// A response arrived carrying `id: null` — the remote could not
    /// recover the id of the request it is answering, so this peer
    /// cannot match it to a pending call either. Whichever call it was
    /// for stays pending until the connection ends.
    UncorrelatedResponse {
        /// The error the remote reported, when the response carried one.
        error: Option<RpcError>,
    },
    /// A response or stream frame arrived for an id with no pending
    /// call and no active stream — a duplicate reply, a frame that
    /// raced a cancelled stream, or a remote using ids it never saw.
    /// Discarded.
    UnroutableFrame {
        /// The id the frame carried.
        id: crate::wire::Id,
    },
}
