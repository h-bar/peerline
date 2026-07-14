//! Unix-domain-socket transport for peerline, for local tooling.
//!
//! Each frame is a single JSON object terminated by a newline
//! ([`tokio_util::codec::LinesCodec`]). The transport is symmetric — the
//! same wire framing on both ends:
//!
//! - [`serve`] (accept side) binds a listener and drives one
//!   [`peerline::runtime::Peer`] per accepted connection, configured by a
//!   caller-supplied handler closure.
//! - [`connect`] (dial side) connects to an existing socket and returns
//!   one `(Peer, driver)` for the caller to configure and drive.
//!
//! peerline is peer-symmetric, so the bind/dial choice only decides which
//! end is addressable — once connected, either peer may issue any frame.

#![forbid(unsafe_code)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::StreamExt;
use peerline::runtime::Peer;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, info, warn};

/// A type-erased per-connection peer initializer — the `on_peer` closure
/// after boxing, so a routing table can hold heterogeneous services.
pub type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;

/// Bind to `path` and serve peerline over a Unix domain socket forever,
/// driving one [`peerline::runtime::Peer`] per accepted connection —
/// configured by `on_peer` (register your handlers there). The socket
/// file is unlinked first if it already exists, then restricted to
/// owner-only access (`0600` on unix) so only the same user can connect.
pub async fn serve<F>(path: impl AsRef<Path>, on_peer: F) -> Result<(), String>
where
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    serve_one(path.as_ref().to_path_buf(), Arc::new(on_peer)).await
}

/// Serve several peerline services over a Unix domain socket, **mounted by
/// socket path**: each `(path, handler)` gets its own listener, so a
/// client reaches one service by connecting to that socket. Unlike WS
/// (paths on one port) or iroh (ALPNs on one endpoint), a UDS mount is a
/// distinct socket file — the filesystem is the namespace. All listeners
/// run concurrently; the call returns if any one fails to bind or accept.
pub async fn serve_mounted(mounts: Vec<(PathBuf, PeerHandler)>) -> Result<(), String> {
    let listeners = mounts.into_iter().map(|(path, handler)| serve_one(path, handler));
    futures::future::try_join_all(listeners).await.map(|_| ())
}

/// One UDS listener: unlink + bind `path`, tighten perms, then accept
/// forever, driving `handler` per connection. The shared core behind both
/// [`serve`] and [`serve_mounted`].
async fn serve_one(path: PathBuf, handler: PeerHandler) -> Result<(), String> {
    match tokio::fs::remove_file(&path).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        other => other.map_err(|e| format!("uds unlink {}: {e}", path.display()))?,
    }
    let listener = UnixListener::bind(&path).map_err(|e| format!("uds bind: {e}"))?;
    // The socket is created under the process umask, which commonly leaves
    // it group/world-connectable. Tighten it to owner-only so access
    // control doesn't silently depend on the caller's umask.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(&path, perms)
            .await
            .map_err(|e| format!("uds chmod {}: {e}", path.display()))?;
    }
    info!(socket = %path.display(), "peerline-uds listening");

    loop {
        // A per-accept error (e.g. the process is momentarily out of file
        // descriptors) must not tear the whole listener down — log it and
        // keep serving, matching the iroh/ws acceptors' resilience.
        let (stream, _addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(error = %e, "peerline-uds accept failed");
                continue;
            }
        };
        let handler = handler.clone();
        tokio::spawn(async move { serve_conn(stream, handler).await });
    }
}

/// Connect to a peerline endpoint at `path` (the dial side of [`serve`])
/// and return one `(Peer, driver)`. Register handlers on the peer, then
/// drive `driver` to run the session; it resolves when the socket closes.
///
/// The framing is identical to [`serve`], so either end may bind or dial.
pub async fn connect(path: impl AsRef<Path>) -> Result<(Peer, BoxFuture<'static, ()>), String> {
    let path = path.as_ref();
    let stream = UnixStream::connect(path)
        .await
        .map_err(|e| format!("uds connect {}: {e}", path.display()))?;
    debug!(socket = %path.display(), "peerline-uds dialed");
    let (peer, driver) = peer_over(stream);
    Ok((peer, Box::pin(driver)))
}

/// Drive one accepted connection: newline-delimited frames into a
/// [`Peer`], run the handler set, then drive until it ends.
async fn serve_conn(stream: UnixStream, handler: PeerHandler) {
    debug!("peerline-uds connection opened");
    let (peer, driver) = peer_over(stream);
    handler(&peer);
    driver.await;
    debug!("peerline-uds connection closed");
}

/// Wrap a connected [`UnixStream`] as a peerline `(Peer, driver)` over the
/// shared newline framing — the one place both [`serve`] and [`connect`]
/// build the codec, so the two ends can't drift.
fn peer_over(stream: UnixStream) -> (Peer, impl Future<Output = ()>) {
    // Pin the line ceiling to the shared [`peerline::MAX_FRAME_LEN`]
    // rather than `LinesCodec`'s unbounded default, so a single JSON-RPC
    // frame is capped at the same size as the other transports.
    let framed = Framed::new(stream, LinesCodec::new_with_max_length(peerline::MAX_FRAME_LEN));
    let (sink, stream) = framed.split();
    Peer::new(Box::pin(sink), Box::pin(stream))
}
