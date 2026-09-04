//! Error type for runtime-level Peer operations.

use crate::wire::RpcError;
use thiserror::Error;

/// Errors surfaced by [`super::Peer`] methods.
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
