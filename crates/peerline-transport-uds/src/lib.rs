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
use std::path::Path;

use futures::future::BoxFuture;
use futures::stream::StreamExt;
use peerline::runtime::Peer;
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, info, warn};

/// Bind to `path` and serve peerline over a Unix domain socket forever,
/// driving one [`peerline::runtime::Peer`] per accepted connection —
/// configured by `on_peer` (register your handlers there). The socket
/// file is unlinked first if it already exists, then restricted to
/// owner-only access (`0600` on unix) so only the same user can connect.
pub async fn serve<F>(path: impl AsRef<Path>, on_peer: F) -> Result<(), String>
where
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    let path = path.as_ref();
    match tokio::fs::remove_file(path).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        other => other.map_err(|e| format!("uds unlink {}: {e}", path.display()))?,
    }
    let listener = UnixListener::bind(path).map_err(|e| format!("uds bind: {e}"))?;
    // The socket is created under the process umask, which commonly leaves
    // it group/world-connectable. Tighten it to owner-only so access
    // control doesn't silently depend on the caller's umask.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(path, perms)
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
        let on_peer = on_peer.clone();
        tokio::spawn(async move { serve_conn(stream, on_peer).await });
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
/// [`Peer`], run the handler set (`on_peer`), then drive until it ends.
async fn serve_conn<F>(stream: UnixStream, on_peer: F)
where
    F: Fn(&Peer),
{
    debug!("peerline-uds connection opened");
    let (peer, driver) = peer_over(stream);
    on_peer(&peer);
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
