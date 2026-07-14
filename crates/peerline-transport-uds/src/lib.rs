//! Unix-domain-socket transport for peerline, for local tooling.
//!
//! Each frame is a single JSON object terminated by a newline
//! ([`tokio_util::codec::LinesCodec`]). [`serve`] binds a listener and
//! drives one [`peerline::runtime::Peer`] per accepted connection,
//! configured by a caller-supplied handler closure.

#![forbid(unsafe_code)]

use std::path::Path;

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

/// Drive one accepted connection: newline-delimited frames into a
/// [`Peer`], run the handler set (`on_peer`), then drive until it ends.
async fn serve_conn<F>(stream: UnixStream, on_peer: F)
where
    F: Fn(&Peer),
{
    debug!("peerline-uds connection opened");
    // Pin the line ceiling to the shared [`peerline::MAX_FRAME_LEN`]
    // rather than `LinesCodec`'s unbounded default, so a single JSON-RPC
    // frame is capped at the same size as the other transports.
    let framed = Framed::new(stream, LinesCodec::new_with_max_length(peerline::MAX_FRAME_LEN));
    let (sink, stream) = framed.split();
    let (peer, driver) = Peer::new(Box::pin(sink), Box::pin(stream));
    on_peer(&peer);
    driver.await;
    debug!("peerline-uds connection closed");
}
