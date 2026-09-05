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
use peerline::runtime::{Peer, Policy};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{debug, info, warn};

/// A type-erased per-connection peer initializer — the closure that
/// registers a service's handlers, after boxing. Defined in
/// [`peerline::runtime`], re-exported here so transport users need only
/// this crate.
pub use peerline::runtime::PeerHandler;

// ---------------------------------------------------------------------------
// UdsAccept — the connection's facts
// ---------------------------------------------------------------------------

/// Credentials of the process on the other end, as the kernel reports
/// them. Handed to an [`Acceptor`] to screen the connection.
///
/// Unforgeable: the peer cannot claim someone else's uid. This is the one
/// peerline transport that authenticates a *user*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdsAccept {
    uid: u32,
    gid: u32,
    pid: Option<i32>,
}

impl UdsAccept {
    /// Effective user id of the connecting process, at connect time.
    #[must_use]
    pub fn uid(&self) -> u32 {
        self.uid
    }

    /// Effective group id of the connecting process.
    #[must_use]
    pub fn gid(&self) -> u32 {
        self.gid
    }

    /// Process id, where the platform reports one.
    ///
    /// Useful for logging and correlation, **not** for authorization: pids
    /// are reused, so anything resolved from one (an executable path, a
    /// cgroup) is a time-of-check/time-of-use trap. Decide on
    /// [`uid`](Self::uid).
    #[must_use]
    pub fn pid(&self) -> Option<i32> {
        self.pid
    }
}

/// Screens one connection: given its [`UdsAccept`] credentials, either
/// admit it — returning the initializer to run on the resulting peer — or
/// refuse with a reason.
///
/// A refusal drops the stream and logs `reason`, before a `Peer` exists
/// and before one frame is dispatched.
///
/// The socket file is published `0600`, and both Linux and macOS require
/// write permission on it to `connect(2)`, so the kernel already limits
/// this to the same user. Credentials are what let a policy say something
/// *more* than that — and are defence in depth if the socket ends up
/// somewhere with looser permissions than intended.
pub type Acceptor = Arc<dyn Fn(&UdsAccept) -> Result<PeerHandler, String> + Send + Sync + 'static>;

// ---------------------------------------------------------------------------
// UdsPolicy — the common decisions, as a value
// ---------------------------------------------------------------------------

/// A reusable admission policy — a conjunction of checks over
/// [`UdsAccept`], backed by [`peerline::runtime::Policy`] so `custom`,
/// `and`, `check`, and `acceptor` behave identically across transports.
/// A value rather than a closure, so a mount table can carry it.
#[derive(Clone)]
pub struct UdsPolicy(Policy<UdsAccept>);

impl Default for UdsPolicy {
    fn default() -> Self {
        Self::same_user()
    }
}

impl UdsPolicy {
    /// Admit connections from this process's own effective uid, and from
    /// root — which the `0600` socket admits regardless, so refusing it
    /// would be theatre that only breaks administrative tooling.
    ///
    /// The default, and the same set the socket's permissions already
    /// allow, so this changes nothing on a correctly published socket and
    /// catches the case where it was not.
    #[must_use]
    pub fn same_user() -> Self {
        Self(Policy::custom(|accept: &UdsAccept| {
            let me = rustix::process::geteuid().as_raw();
            if accept.uid() == me || accept.uid() == 0 {
                Ok(())
            } else {
                Err(format!("uid {} is not {me} or root", accept.uid()))
            }
        }))
    }

    /// Admit connections from any of `uids`.
    #[must_use]
    pub fn users<I: IntoIterator<Item = u32>>(uids: I) -> Self {
        let uids: Vec<u32> = uids.into_iter().collect();
        Self(Policy::custom(move |accept: &UdsAccept| {
            if uids.contains(&accept.uid()) {
                Ok(())
            } else {
                Err(format!("uid {} not permitted", accept.uid()))
            }
        }))
    }

    /// Admit every connection whose credentials the kernel can report,
    /// relying on the socket's file permissions alone. (Screening reads
    /// peer credentials before any policy runs, and a connection whose
    /// credentials cannot be read is refused rather than served blind —
    /// which essentially cannot happen for a connected stream on the
    /// supported platforms.)
    #[must_use]
    pub fn allow_any() -> Self {
        Self(Policy::allow_any())
    }

    /// A policy that is only `f`.
    #[must_use]
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(&UdsAccept) -> Result<(), String> + Send + Sync + 'static,
    {
        Self(Policy::custom(f))
    }

    /// Additionally require `f` to accept. Composes: every `and` is kept
    /// and all must pass — e.g. `UdsPolicy::same_user().and(...)` keeps
    /// the euid rule *and* enforces the extra check.
    #[must_use]
    pub fn and<F>(self, f: F) -> Self
    where
        F: Fn(&UdsAccept) -> Result<(), String> + Send + Sync + 'static,
    {
        Self(self.0.and(f))
    }

    /// Apply the policy. `Err(reason)` refuses the connection.
    pub fn check(&self, accept: &UdsAccept) -> Result<(), String> {
        self.0.check(accept)
    }

    /// Pair this policy with a peer initializer, giving the closure
    /// [`serve`] wants. Wrap in [`Arc`] for a [`serve_mounted`] table.
    pub fn acceptor<F>(
        self,
        on_peer: F,
    ) -> impl Fn(&UdsAccept) -> Result<PeerHandler, String> + Send + Sync + 'static
    where
        F: Fn(&Peer) + Send + Sync + 'static,
    {
        self.0.acceptor(on_peer)
    }
}

/// Bind to `path` and serve peerline over a Unix domain socket forever,
/// driving one [`peerline::runtime::Peer`] per admitted connection,
/// screening each with `acceptor`. The socket is published owner-only
/// (`0600` on unix), so only the same user can connect; see
/// [`serve_mounted`] for exactly what that guarantees, and prefer a
/// directory only this user can write.
///
/// ```ignore
/// serve("/run/app.sock", UdsPolicy::same_user().acceptor(|peer| {
///     peer.on_request("ping", |_: ()| async { Ok::<_, RpcError>("pong") });
/// }))
/// .await
/// ```
pub async fn serve<F>(path: impl AsRef<Path>, acceptor: F) -> Result<(), String>
where
    F: Fn(&UdsAccept) -> Result<PeerHandler, String> + Send + Sync + 'static,
{
    serve_one(path.as_ref().to_path_buf(), Arc::new(acceptor)).await
}

/// Serve several peerline services over a Unix domain socket, **mounted by
/// socket path**: each `(path, handler)` gets its own listener, so a
/// client reaches one service by connecting to that socket. Unlike WS
/// (paths on one port) or iroh (ALPNs on one endpoint), a UDS mount is a
/// distinct socket file — the filesystem is the namespace. All listeners
/// run concurrently; the call returns if any one fails to bind. Per-accept
/// errors do not tear a listener down: they are logged and retried after a
/// short pause.
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
pub async fn serve_mounted(mounts: Vec<(PathBuf, Acceptor)>) -> Result<(), String> {
    let listeners = mounts
        .into_iter()
        .map(|(path, acceptor)| serve_one(path, acceptor));
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
/// under a private `0700` directory (creatable safely via rustix's `fs`
/// API — a mode-explicit `mkdir`, an `O_NOFOLLOW` ownership check, then
/// rename out) — machinery this crate deliberately doesn't carry for a
/// window that is only reachable under a permissive umask. A caller that
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

/// Best-effort removal of staging leftovers from previous runs that died
/// between `bind` and `rename`: siblings of `path` named
/// `<name>.<pid>.<seq>.tmp` whose pid no longer names a live process.
/// Nothing else ever removes these — a fresh run computes a different
/// pid/seq name — so without the sweep a crash-looping daemon accumulates
/// dead staging sockets forever. Entries whose pid is alive are left
/// alone: they may belong to a concurrent publisher of this same target
/// (or another listener in this process). Failures are ignored — the
/// sweep is hygiene, and the staging name this bind actually uses is
/// cleared separately with a hard error.
async fn sweep_stale_staging(path: &Path) {
    let (Some(dir), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str())) else {
        return;
    };
    let prefix = format!("{name}.");
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let file_name = entry.file_name();
        let Some(stem) = file_name
            .to_str()
            .and_then(|f| f.strip_prefix(&prefix))
            .and_then(|rest| rest.strip_suffix(".tmp"))
        else {
            continue;
        };
        // The stem must be exactly `<pid>.<seq>` — anything else is not
        // ours to judge.
        let mut parts = stem.split('.');
        let (Some(pid), Some(seq), None) = (parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        if seq.parse::<u64>().is_err() {
            continue;
        }
        let Ok(pid) = pid.parse::<i32>() else {
            continue;
        };
        if pid_is_alive(pid) {
            continue;
        }
        let _ = tokio::fs::remove_file(entry.path()).await;
    }
}

/// `kill(pid, 0)` via rustix. Only ESRCH proves the pid is free; success
/// or EPERM means some process holds it, so its staging entries are
/// treated as live.
fn pid_is_alive(pid: i32) -> bool {
    let Some(pid) = rustix::process::Pid::from_raw(pid) else {
        // Zero / negative — not a pid the staging scheme writes.
        return true;
    };
    !matches!(
        rustix::process::test_kill_process(pid),
        Err(rustix::io::Errno::SRCH)
    )
}

/// One UDS listener: publish `path` owner-only, then accept forever,
/// screening each connection with `acceptor`. The shared core behind both
/// [`serve`] and [`serve_mounted`].
async fn serve_one(path: PathBuf, acceptor: Acceptor) -> Result<(), String> {
    sweep_stale_staging(&path).await;
    let listener = bind_owner_only(&path).await?;
    info!(socket = %path.display(), "peerline-uds listening");

    loop {
        // A per-accept error (e.g. the process is momentarily out of file
        // descriptors) must not tear the whole listener down — log it and
        // keep serving, matching the iroh/ws acceptors' resilience. The
        // pause is for the sticky case: under EMFILE the pending
        // connection keeps the listener readable and `accept` fails again
        // immediately, so an unpaced retry pegs a core and floods the log.
        let (stream, _addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(error = %e, "peerline-uds accept failed");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
        };
        // Screening happens in the connection's own task, not here: an
        // acceptor is caller-supplied, and one that takes its time must
        // not hold up every other pending accept on this socket.
        let acceptor = acceptor.clone();
        let socket = path.clone();
        tokio::spawn(async move { screen_and_serve(stream, socket, &acceptor).await });
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

/// Screen one accepted connection, then serve it if admitted.
async fn screen_and_serve(stream: UnixStream, socket: PathBuf, acceptor: &Acceptor) {
    // Credentials are the whole point of screening here, so a socket that
    // cannot report them is refused rather than served blind.
    let accept = match peer_credentials(&stream) {
        Ok(accept) => accept,
        Err(e) => {
            warn!(socket = %socket.display(), error = %e,
                  "peerline-uds: no peer credentials; refusing");
            return;
        }
    };
    match acceptor(&accept) {
        Ok(init) => serve_conn(stream, init).await,
        Err(reason) => warn!(
            socket = %socket.display(),
            uid = accept.uid(),
            pid = ?accept.pid(),
            %reason,
            "peerline-uds connection refused"
        ),
    }
}

/// The connecting process's credentials, as the kernel reports them.
fn peer_credentials(stream: &UnixStream) -> Result<UdsAccept, std::io::Error> {
    let cred = stream.peer_cred()?;
    Ok(UdsAccept {
        uid: cred.uid(),
        gid: cred.gid(),
        pid: cred.pid(),
    })
}

/// Drive one admitted connection: newline-delimited frames into a
/// [`Peer`], run the handler set, then drive until it ends.
async fn serve_conn(stream: UnixStream, init: PeerHandler) {
    debug!("peerline-uds connection opened");
    let (peer, driver) = peer_over(stream);
    init(&peer);
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

    /// The sweep removes only what it can prove stale: a staging entry
    /// whose pid is dead. Entries with a live pid (possibly a concurrent
    /// publisher) and names that don't match the `<pid>.<seq>.tmp` shape
    /// are left alone.
    #[tokio::test]
    async fn sweep_removes_only_dead_pid_staging_entries() {
        let dir = temp_socket("sweep-dir");
        std::fs::create_dir_all(&dir).expect("mkdir");
        let target = dir.join("app.sock");

        // A pid that provably no longer runs: spawn something short-lived
        // and reap it.
        let dead_pid = {
            let mut child = std::process::Command::new("/usr/bin/true")
                .spawn()
                .expect("spawn");
            let pid = child.id();
            child.wait().expect("wait");
            pid
        };

        let stale = dir.join(format!("app.sock.{dead_pid}.0.tmp"));
        let live = dir.join(format!("app.sock.{}.1.tmp", std::process::id()));
        let unrelated = dir.join("app.sock.notapid.2.tmp");
        for f in [&stale, &live, &unrelated] {
            std::fs::write(f, b"").expect("plant entry");
        }

        sweep_stale_staging(&target).await;

        assert!(!stale.exists(), "dead-pid entry must be swept");
        assert!(live.exists(), "live-pid entry must survive");
        assert!(unrelated.exists(), "non-matching name must survive");
        let _ = std::fs::remove_dir_all(&dir);
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

    fn accept_as(uid: u32) -> UdsAccept {
        UdsAccept {
            uid,
            gid: 20,
            pid: Some(1),
        }
    }

    /// `same_user` admits this process and root, and nobody else.
    #[test]
    fn same_user_admits_self_and_root_only() {
        let me = rustix::process::geteuid().as_raw();
        let p = UdsPolicy::same_user();
        assert!(p.check(&accept_as(me)).is_ok());
        assert!(p.check(&accept_as(0)).is_ok());
        let err = p
            .check(&accept_as(me.wrapping_add(1)))
            .expect_err("another uid must be refused");
        assert!(err.contains("not"), "unexpected: {err}");
    }

    #[test]
    fn users_policy_is_an_allowlist() {
        let p = UdsPolicy::users([501, 502]);
        assert!(p.check(&accept_as(501)).is_ok());
        assert!(p.check(&accept_as(503)).is_err());
        assert!(UdsPolicy::allow_any().check(&accept_as(999)).is_ok());
    }

    /// `and` composes with the named rules — the point of the shared
    /// policy machinery: `same_user()` gains an extra conjunct without
    /// re-implementing the euid-or-root rule.
    #[test]
    fn policy_and_composes_with_named_rules() {
        let me = rustix::process::geteuid().as_raw();
        let p = UdsPolicy::same_user().and(|a: &UdsAccept| {
            if a.pid().is_some() {
                Ok(())
            } else {
                Err("no pid".into())
            }
        });
        let with_pid = UdsAccept {
            uid: me,
            gid: 0,
            pid: Some(1),
        };
        let without_pid = UdsAccept {
            uid: me,
            gid: 0,
            pid: None,
        };
        assert!(p.check(&with_pid).is_ok());
        assert_eq!(p.check(&without_pid).unwrap_err(), "no pid");
        // The named rule is still enforced alongside the conjunct.
        let other_user = UdsAccept {
            uid: me.wrapping_add(1),
            gid: 0,
            pid: Some(1),
        };
        assert!(p.check(&other_user).is_err());
    }

    /// A refused connection reaches no op: the client's call fails rather
    /// than being answered.
    #[tokio::test]
    async fn refused_connection_dispatches_no_frames() {
        let path = temp_socket("refuse");
        let serve_path = path.clone();
        let server = tokio::spawn(async move {
            serve(&serve_path, |accept: &UdsAccept| {
                assert_ne!(accept.uid(), u32::MAX, "credentials should be readable");
                Err::<PeerHandler, _>("not today".to_string())
            })
            .await
        });

        let mut connected = None;
        for _ in 0..200 {
            if let Ok(c) = connect(&path).await {
                connected = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (peer, driver) = connected.expect("server should accept a connection");
        tokio::spawn(driver);
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            peer.call::<_, String>("ping", &serde_json::json!({})),
        )
        .await
        .expect("call must not hang")
        .expect_err("a refused connection must not answer");
        // Specifically *closed*, not merely an error: a regression that
        // served the connection with no handlers registered would answer
        // `MethodNotFound`, which is also an `Err`.
        assert!(
            err.to_string().contains("closed"),
            "expected the connection to be dropped, got: {err}"
        );

        server.abort();
        let _ = std::fs::remove_file(&path);
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
