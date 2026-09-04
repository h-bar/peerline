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

use std::ffi::OsString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
/// configured by `on_peer` (register your handlers there). The socket is
/// published owner-only (`0600` on unix), so only the same user can
/// connect; see [`serve_mounted`] for exactly what that guarantees, and
/// prefer a directory only this user can write.
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
///
/// Each socket is published owner-only (`0600` on unix): it is bound and
/// `chmod`ed under a staging name in the same directory and then renamed
/// into place. So the **advertised path** is never reachable while still
/// carrying the process umask's permissions, and an existing socket there
/// is replaced atomically rather than unlinked-then-rebound.
///
/// The staging name does exist briefly under the umask. That window is
/// only reachable by another user if the umask leaves the socket
/// group/world-writable, and the socket should anyway live in a directory
/// only this user can write — put it there and the question does not
/// arise.
pub async fn serve_mounted(mounts: Vec<(PathBuf, PeerHandler)>) -> Result<(), String> {
    let listeners = mounts
        .into_iter()
        .map(|(path, handler)| serve_one(path, handler));
    futures::future::try_join_all(listeners).await.map(|_| ())
}

/// Serial number for staging paths, so two listeners in one process
/// can't pick the same temporary name.
static STAGING_SEQ: AtomicU64 = AtomicU64::new(0);

/// A sibling of `path` to bind before publishing: same directory (so the
/// rename that follows is within one filesystem and therefore atomic),
/// name distinct per process and per listener.
///
/// This lengthens the path by ~12 bytes, which matters only for targets
/// already within that much of the platform's `sun_path` limit (104
/// bytes on macOS, 108 on Linux) — those now fail to bind, with an error
/// naming both paths.
fn staging_path(path: &Path) -> PathBuf {
    let seq = STAGING_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut name: OsString = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.{seq}.tmp", std::process::id()));
    path.with_file_name(name)
}

/// Bind a listener that is owner-only from the instant it is reachable
/// at `path`.
///
/// The obvious sequence — bind `path`, then `chmod` it — has two
/// problems. The socket is published under the process umask and
/// tightened a moment later, so anyone who connects inside that window
/// is already in; and unlinking a stale `path` before binding leaves a
/// gap another process can bind. Doing the work out of sight fixes both
/// *for the advertised path*: bind a staging name in the same directory,
/// `chmod` it there, and `rename` it onto `path`. Rename is atomic and
/// replaces whatever was there, so `path` is never absent and never
/// exists in a permissive state.
///
/// What this does not erase is the umask window itself — it moves it to
/// the staging name, which is predictable (`<name>.<pid>.<n>.tmp`). A
/// process watching the directory could connect there in the gap between
/// bind and `chmod`. Reaching it needs a umask that leaves the socket
/// group- or world-*writable* (`connect(2)` requires write permission on
/// the socket file, on both Linux and macOS), which is exactly the case
/// the `chmod` exists for — `0002` qualifies, not just `0`. So the
/// residual exposure is a brief race under a permissive umask rather than
/// a socket left connectable for the life of the service.
///
/// Because the name is predictable, the two checks above are what keep it
/// from being worse than a race: an entry we cannot remove is a hard
/// error, and an entry that survives `bind` as a symlink is refused. Both
/// matter in a sticky shared directory like `/tmp`, where another user
/// can create names we cannot delete, and where `bind` following a
/// trailing symlink would otherwise put the live socket somewhere they
/// control.
///
/// Closing the umask window completely would need the bind to happen
/// under a private `0700` directory or a scoped umask, neither of which
/// this crate can do without `unsafe` or a new dependency. A caller that
/// wants the stronger guarantee should place the socket in a directory
/// only it can write.
async fn bind_owner_only(path: &Path) -> Result<UnixListener, String> {
    bind_via_staging(path, &staging_path(path)).await
}

/// The body of [`bind_owner_only`], with the staging name supplied rather
/// than allocated. Split out so a test can plant a squatter at a known
/// name: [`staging_path`] bumps a shared counter, so a test that called it
/// to *predict* the next name would consume the very value it predicted.
async fn bind_via_staging(path: &Path, staging: &Path) -> Result<UnixListener, String> {
    // Clear the staging name, and **fail if we cannot**. Tolerating the
    // error here would be a security regression: the staging name is
    // predictable, `bind` follows a trailing symlink, and in a sticky
    // shared directory (`/tmp`) another user's entry cannot be unlinked.
    // A symlink pre-created there would silently redirect the socket we
    // are about to create into a directory the attacker controls. The
    // sequence this replaced took the same line on the target path.
    match tokio::fs::remove_file(staging).await {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        other => other.map_err(|e| format!("uds clear staging {}: {e}", staging.display()))?,
    }

    let listener = UnixListener::bind(staging).map_err(|e| {
        format!(
            "uds bind {} (staging for {}): {e}",
            staging.display(),
            path.display()
        )
    })?;

    ensure_staging_is_not_a_symlink(staging).await?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        if let Err(e) = tokio::fs::set_permissions(staging, perms).await {
            let _ = tokio::fs::remove_file(staging).await;
            return Err(format!(
                "uds chmod {} (staging for {}): {e}",
                staging.display(),
                path.display()
            ));
        }
    }

    if let Err(e) = tokio::fs::rename(staging, path).await {
        let _ = tokio::fs::remove_file(staging).await;
        return Err(format!(
            "uds publish {} -> {}: {e}",
            staging.display(),
            path.display()
        ));
    }
    Ok(listener)
}

/// Closes the race the pre-bind removal cannot: the name could have been
/// created in between. `bind` follows a trailing symlink, so a staging
/// path that is *still* a symlink after binding means the socket was
/// created somewhere else — refuse rather than `chmod` and `rename`
/// through it. Past this point the staging entry is a socket we own,
/// which a sticky directory stops anyone else replacing.
async fn ensure_staging_is_not_a_symlink(staging: &Path) -> Result<(), String> {
    match tokio::fs::symlink_metadata(staging).await {
        Ok(m) if !m.file_type().is_symlink() => Ok(()),
        Ok(_) => Err(format!(
            "uds staging {} is a symlink; refusing to publish through it",
            staging.display()
        )),
        Err(e) => Err(format!("uds stat staging {}: {e}", staging.display())),
    }
}

/// One UDS listener: publish `path` owner-only, then accept forever,
/// driving `handler` per connection. The shared core behind both
/// [`serve`] and [`serve_mounted`].
async fn serve_one(path: PathBuf, handler: PeerHandler) -> Result<(), String> {
    let listener = bind_owner_only(&path).await?;
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
    let framed = Framed::new(
        stream,
        LinesCodec::new_with_max_length(peerline::MAX_FRAME_LEN),
    );
    let (sink, stream) = framed.split();
    Peer::new(Box::pin(sink), Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A unique socket path under the temp dir, short enough to stay
    /// well inside `sun_path` even after the staging suffix.
    fn temp_socket(tag: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "pl-{tag}-{}-{}.sock",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// The published socket must be owner-only. The staging + rename
    /// dance exists so this holds from the instant the path is
    /// reachable, not merely by the time anyone looks — a bind-then-
    /// chmod would pass this assertion while still having exposed the
    /// socket under the process umask in between.
    #[tokio::test]
    async fn published_socket_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_socket("perms");
        let listener = bind_owner_only(&path).await.expect("bind");

        let mode = std::fs::metadata(&path).expect("stat").permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "socket mode was {:o}", mode & 0o777);

        drop(listener);
        let _ = std::fs::remove_file(&path);
    }

    /// The staging name is predictable, so a hostile process sharing the
    /// directory can pre-create it. In a sticky directory like `/tmp` we
    /// cannot remove another user's entry — and tolerating that failure
    /// would let `bind` follow their symlink and put the live socket
    /// wherever they pointed. Refuse instead.
    ///
    /// The unremovable entry is simulated with a directory we make
    /// unwritable, which fails `unlink` the same way the sticky bit does
    /// for someone else's file.
    #[tokio::test]
    async fn refuses_when_the_staging_name_cannot_be_cleared() {
        use std::os::unix::fs::PermissionsExt;

        let dir = temp_socket("squat-dir");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("app.sock");
        let staging = dir.join("app.sock.squatted.tmp");
        std::os::unix::fs::symlink(dir.join("captured.sock"), &staging).expect("plant symlink");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).expect("chmod dir");

        let restore = |dir: &PathBuf| {
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
            let _ = std::fs::remove_dir_all(dir);
        };
        // Running as root, or on a filesystem that ignores directory
        // permissions, the premise does not hold and there is nothing to
        // assert.
        if std::fs::remove_file(&staging).is_ok() {
            restore(&dir);
            return;
        }

        let err = bind_via_staging(&path, &staging)
            .await
            .expect_err("an unremovable staging name must be refused");
        assert!(err.contains("clear staging"), "unexpected error: {err}");
        assert!(!path.exists(), "the target must not be published");
        restore(&dir);
    }

    /// The second guard, for a squatter that appears *after* the removal
    /// and before the bind. `bind` follows a trailing symlink, so the
    /// staging entry surviving as a symlink means the socket landed
    /// somewhere else. Exercised directly: reproducing the interleaving
    /// end-to-end needs a second user and a sticky directory.
    #[tokio::test]
    async fn refuses_staging_that_survives_the_bind_as_a_symlink() {
        let staging = temp_socket("symlink-guard");
        std::os::unix::fs::symlink(temp_socket("symlink-target"), &staging).expect("plant");

        let err = ensure_staging_is_not_a_symlink(&staging)
            .await
            .expect_err("a symlinked staging entry must be refused");
        assert!(err.contains("symlink"), "unexpected error: {err}");

        // A real socket there is accepted.
        let _ = std::fs::remove_file(&staging);
        let listener = UnixListener::bind(&staging).expect("bind");
        assert!(ensure_staging_is_not_a_symlink(&staging).await.is_ok());
        drop(listener);
        let _ = std::fs::remove_file(&staging);
    }

    /// Publishing is a rename onto the target, so an existing socket at
    /// that path is replaced and stays continuously reachable. (This
    /// shows the replacement works; the *atomicity* of the swap is not
    /// something a test can observe from outside.)
    #[tokio::test]
    async fn publishing_replaces_an_existing_socket() {
        let path = temp_socket("replace");
        let first = bind_owner_only(&path).await.expect("first bind");
        let second = bind_owner_only(&path).await.expect("second bind");

        // The path now resolves to the second listener: connecting
        // succeeds and the first listener never sees it.
        UnixStream::connect(&path).await.expect("connect");
        tokio::time::timeout(Duration::from_secs(5), second.accept())
            .await
            .expect("the republished listener should accept, not time out")
            .expect("accept should succeed");

        drop(first);
        let _ = std::fs::remove_file(&path);
    }
}
