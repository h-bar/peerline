//! Unified multi-service host for peerline.
//!
//! Every peerline service is just "a function that registers handlers on a
//! [`Peer`]". This crate turns that into a composable unit — the
//! [`Service`] trait — and a [`Host`] that mounts many services behind one
//! transport stack, so a fleet of services runs from **one binary** instead
//! of one binary each.
//!
//! Each service is **mounted at an address you configure** ([`Mount`]), per
//! transport, so a connection reaches exactly one service and their ops
//! never collide:
//!
//! - WebSocket — a path on the shared port (`/` for a lone service, `/foo`
//!   when several share a port).
//! - Unix socket — a socket file (the filesystem is the namespace).
//! - iroh — an ALPN on the shared endpoint (one ticket, dialer picks the
//!   ALPN).
//!
//! Addresses are explicit rather than derived from the name, so migrating a
//! single-service daemon keeps its exact wire addresses (WS `/`, its socket
//! path), while a multi-service host gives each a distinct mount.
//!
//! ```ignore
//! let mut host = Host::new()
//!     .mount(Sh::new(state), Mount::new().ws("/").uds(cfg.socket));
//! if let Some(addr) = cfg.ws_bind { host = host.ws_bind(addr); }
//! host.run().await?;   // binds transports, serves until SIGINT
//! ```
//!
//! The host owns the boilerplate every service used to hand-roll: binding
//! each transport, requiring at least one active mount, iroh key
//! persistence + ticket emission, and graceful shutdown.

#![forbid(unsafe_code)]

use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::BoxFuture;
use peerline::runtime::Peer;
use tokio::sync::oneshot;
use tracing::info;

/// The boxed per-connection peer initializer handed to each transport.
type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;

/// A transport's forever-running accept loop.
type Serve = BoxFuture<'static, Result<(), String>>;

/// A composable peerline service: a name (for logging) plus the handler
/// registrations that run on each freshly-connected peer.
///
/// Migrating an existing service is mechanical: its old
/// `register_handlers(peer, state)` body becomes [`Service::register`], and
/// its state lives in the implementing struct.
pub trait Service: Send + Sync + 'static {
    /// Stable name, used in logs. The wire address comes from [`Mount`],
    /// not from here.
    fn name(&self) -> &'static str;

    /// Register this service's request / notification / stream handlers on
    /// a freshly-connected peer. Called once per connection.
    fn register(&self, peer: &Peer);
}

/// Where one service is mounted, per transport. A `None` field means the
/// service is not exposed on that transport. Build with the fluent
/// setters; every address is explicit.
#[derive(Default, Clone)]
pub struct Mount {
    /// WebSocket path on the shared port, e.g. `/` or `/foo`. Only active
    /// if the host also has a [`Host::ws_bind`] address.
    pub ws_path: Option<String>,
    /// Unix domain socket file. Its parent directory is created if missing.
    pub uds_path: Option<PathBuf>,
    /// iroh ALPN on the shared endpoint. Only active if the host also has a
    /// [`Host::iroh_endpoint`].
    pub iroh_alpn: Option<Vec<u8>>,
}

impl Mount {
    /// An empty mount — not on any transport until a setter is called.
    pub fn new() -> Self {
        Self::default()
    }

    /// Serve this service at `path` on the shared WebSocket port.
    #[must_use]
    pub fn ws(mut self, path: impl Into<String>) -> Self {
        self.ws_path = Some(path.into());
        self
    }

    /// Serve this service on the Unix socket file `path`.
    #[must_use]
    pub fn uds(mut self, path: impl Into<PathBuf>) -> Self {
        self.uds_path = Some(path.into());
        self
    }

    /// Serve this service under `alpn` on the shared iroh endpoint.
    #[must_use]
    pub fn iroh(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.iroh_alpn = Some(alpn.into());
        self
    }
}

/// Whether/where the host registers with a `peerline-manager`.
#[derive(Default)]
enum ReportMode {
    /// Register with the default manager (`PEERLINE_MANAGER_SOCK` or the
    /// canonical `/tmp/peerline-manager.sock`). The default.
    #[default]
    Auto,
    /// Register with a specific manager socket.
    To(PathBuf),
    /// Don't register at all.
    Off,
}

/// The default manager socket a host reports to — the same
/// `PEERLINE_MANAGER_SOCK` (+ default) the manager itself binds.
fn default_manager_sock() -> PathBuf {
    std::env::var_os("PEERLINE_MANAGER_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/peerline-manager.sock"))
}

/// Builder for a unified multi-service peerline host.
#[derive(Default)]
pub struct Host {
    services: Vec<(Arc<dyn Service>, Mount)>,
    ws_bind: Option<SocketAddr>,
    iroh_key: Option<Option<PathBuf>>,
    iroh_relays: Vec<String>,
    report: ReportMode,
}

impl Host {
    /// Start an empty host.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mount a service at the given address(es).
    #[must_use]
    pub fn mount(mut self, svc: impl Service, mount: Mount) -> Self {
        self.services.push((Arc::new(svc), mount));
        self
    }

    /// Bind the shared WebSocket port. Services with a [`Mount::ws`] path
    /// are served here; without this, WS mounts are inactive.
    #[must_use]
    pub fn ws_bind(mut self, addr: SocketAddr) -> Self {
        self.ws_bind = Some(addr);
        self
    }

    /// Enable the shared iroh endpoint, persisting its identity at
    /// `key_path` (`None` ⇒ ephemeral). Services with a [`Mount::iroh`]
    /// ALPN are served here.
    #[must_use]
    pub fn iroh_endpoint(mut self, key_path: Option<PathBuf>) -> Self {
        self.iroh_key = Some(key_path);
        self
    }

    /// Add a custom iroh relay (repeatable). Both ends of a link must share
    /// the same relay; a self-hosted relay is what makes the iroh transport
    /// reachable where n0's public relays are distant or unreliable and direct
    /// connectivity is impossible. No relays ⇒ n0's defaults. Only takes effect
    /// with an [`Host::iroh_endpoint`]; the URL is validated at [`Host::run`].
    #[must_use]
    pub fn iroh_relay(mut self, url: impl Into<String>) -> Self {
        self.iroh_relays.push(url.into());
        self
    }

    /// Register this host's services + dial coordinates with a
    /// `peerline-manager` at a **specific** socket, overriding the default.
    ///
    /// By default a host already reports to the default manager
    /// (`PEERLINE_MANAGER_SOCK` or `/tmp/peerline-manager.sock`); call this
    /// only to target a different one, or [`Host::no_report`] to opt out.
    ///
    /// Once every transport binds (including the iroh ticket, if any), the
    /// host registers and heartbeats, holding the connection for the process
    /// lifetime; the manager expires it on TTL after it exits. Harmless if no
    /// manager is running (it retries). Requires the `uds` feature.
    #[must_use]
    pub fn report_to(mut self, manager_sock: impl Into<PathBuf>) -> Self {
        self.report = ReportMode::To(manager_sock.into());
        self
    }

    /// Opt out of registering with a manager. By default a host reports to
    /// the default manager socket; use this for standalone services (or
    /// tests) that shouldn't touch a manager.
    #[must_use]
    pub fn no_report(mut self) -> Self {
        self.report = ReportMode::Off;
        self
    }

    /// Bind every configured transport and serve until Ctrl-C (SIGINT). See
    /// [`Host::run_until`] to supply a custom shutdown signal.
    pub async fn run(self) -> Result<(), String> {
        self.run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    /// Bind every configured transport and serve until `shutdown` resolves
    /// or a transport fails.
    ///
    /// Errors if no service is registered, no mount is active on any
    /// configured transport, or two services collide on one address.
    pub async fn run_until(self, shutdown: impl Future<Output = ()>) -> Result<(), String> {
        if self.services.is_empty() {
            return Err("peerline-host: no services registered".into());
        }

        // One boxed initializer per service, reused across transports.
        let prepared: Vec<(&'static str, Mount, PeerHandler)> = self
            .services
            .iter()
            .map(|(svc, mount)| {
                let s = svc.clone();
                let handler: PeerHandler = Arc::new(move |peer: &Peer| s.register(peer));
                (svc.name(), mount.clone(), handler)
            })
            .collect();

        let mut serves: Vec<Serve> = Vec::new();
        // Receives the iroh ticket once the endpoint binds, so the manager
        // report can include it. Only wired when we're going to report.
        let will_report = !matches!(self.report, ReportMode::Off);
        let mut ticket_rx: Option<oneshot::Receiver<String>> = None;

        // WebSocket — paths on the shared port.
        if let Some(addr) = self.ws_bind {
            let mut mounts = Vec::new();
            for (name, mount, handler) in &prepared {
                if let Some(path) = &mount.ws_path {
                    // Explicit dial coordinates so a caller sees how to reach us.
                    info!(service = name, url = %format!("ws://{addr}{path}"),
                          "peerline-host: reachable (ws)");
                    mounts.push((path.clone(), handler.clone()));
                }
            }
            reject_dupes("ws path", mounts.iter().map(|(p, _)| p.clone()))?;
            if !mounts.is_empty() {
                serves.push(ws_serve(addr, mounts)?);
            }
        }

        // Unix sockets — one file per service.
        {
            let mut mounts = Vec::new();
            for (name, mount, handler) in &prepared {
                if let Some(path) = &mount.uds_path {
                    if let Some(dir) = path.parent() {
                        std::fs::create_dir_all(dir).map_err(|e| {
                            format!("peerline-host: uds dir {}: {e}", dir.display())
                        })?;
                    }
                    info!(service = name, socket = %path.display(),
                          "peerline-host: reachable (uds)");
                    mounts.push((path.clone(), handler.clone()));
                }
            }
            reject_dupes(
                "uds socket",
                mounts.iter().map(|(p, _)| p.display().to_string()),
            )?;
            if !mounts.is_empty() {
                serves.push(uds_serve(mounts)?);
            }
        }

        // iroh — ALPNs on the shared endpoint. The ticket isn't known until
        // the endpoint binds, so per-service dial info is printed from the
        // bind callback (see `iroh_serve`); collect (name, alpn) for it.
        if let Some(key_path) = &self.iroh_key {
            let mut mounts = Vec::new();
            let mut dials: Vec<(String, String)> = Vec::new();
            for (name, mount, handler) in &prepared {
                if let Some(alpn) = &mount.iroh_alpn {
                    dials.push((name.to_string(), String::from_utf8_lossy(alpn).into_owned()));
                    mounts.push((alpn.clone(), handler.clone()));
                }
            }
            reject_dupes("iroh alpn", mounts.iter().map(|(a, _)| a.clone()))?;
            if !mounts.is_empty() {
                // When reporting, capture the ticket (async, known at bind).
                let ticket_tx = if will_report {
                    let (tx, rx) = oneshot::channel();
                    ticket_rx = Some(rx);
                    Some(tx)
                } else {
                    None
                };
                serves.push(iroh_serve(
                    key_path.clone(),
                    self.iroh_relays.clone(),
                    mounts,
                    dials,
                    ticket_tx,
                )?);
            }
        }

        if serves.is_empty() {
            return Err("peerline-host: no active mounts (configure ws_bind / iroh_endpoint \
                        and give each service a Mount)"
                .into());
        }

        // Register with a manager unless opted out. Default: the canonical
        // manager socket. The report loop is driven inside the `select!`
        // below (never resolving), so it — and thus the manager connection
        // that keeps us registered — is dropped when the host shuts down,
        // which deregisters us.
        let report_sock = match &self.report {
            ReportMode::Off => None,
            ReportMode::To(path) => Some(path.clone()),
            ReportMode::Auto => Some(default_manager_sock()),
        };
        #[cfg(not(feature = "uds"))]
        if matches!(self.report, ReportMode::To(_)) {
            info!("peerline-host: report_to needs the `uds` feature; not registering");
        }
        let report =
            build_report_future(report_sock, &prepared, self.ws_bind, self.iroh_key.is_some(), ticket_rx)?;

        info!(services = self.services.len(), transports = serves.len(), "peerline-host starting");

        // Run every transport until one errors or shutdown fires. Dropping
        // the serve futures (and the report loop) on shutdown tears the
        // listeners and the manager registration down.
        tokio::select! {
            r = futures::future::try_join_all(serves) => r.map(|_| ()),
            _ = report => Ok(()), // never resolves; here only to be driven
            _ = shutdown => {
                info!("peerline-host: shutdown signal received, stopping");
                Ok(())
            }
        }
    }
}

// Transport dispatch, gated by feature. Each `*_serve` returns the accept
// loop when the feature is on, or an error when a mount targets a transport
// the binary wasn't built with — the public API is identical either way.

#[cfg(feature = "ws")]
fn ws_serve(addr: SocketAddr, mounts: Vec<(String, PeerHandler)>) -> Result<Serve, String> {
    Ok(Box::pin(peerline_transport_ws::serve_mounted(addr, mounts)))
}
#[cfg(not(feature = "ws"))]
fn ws_serve(_: SocketAddr, _: Vec<(String, PeerHandler)>) -> Result<Serve, String> {
    Err("peerline-host: WS mounts configured but the `ws` feature is disabled".into())
}

#[cfg(feature = "uds")]
fn uds_serve(mounts: Vec<(PathBuf, PeerHandler)>) -> Result<Serve, String> {
    Ok(Box::pin(peerline_transport_uds::serve_mounted(mounts)))
}
#[cfg(not(feature = "uds"))]
fn uds_serve(_: Vec<(PathBuf, PeerHandler)>) -> Result<Serve, String> {
    Err("peerline-host: UDS mounts configured but the `uds` feature is disabled".into())
}

#[cfg(feature = "iroh")]
fn iroh_serve(
    key_path: Option<PathBuf>,
    relays: Vec<String>,
    mounts: Vec<(Vec<u8>, PeerHandler)>,
    dials: Vec<(String, String)>,
    ticket_tx: Option<oneshot::Sender<String>>,
) -> Result<Serve, String> {
    let key = peerline_transport_iroh::load_or_create_secret_key(key_path.as_deref())
        .map_err(|e| format!("peerline-host: iroh key: {e}"))?;
    let config = peerline_transport_iroh::IrohConfig::from_relay_urls(relays)
        .map_err(|e| format!("peerline-host: iroh relay: {e}"))?;
    let serve = peerline_transport_iroh::serve_mounted(
        key,
        config,
        // The one ticket addresses the shared endpoint; each service is
        // reached by dialing it with that service's ALPN. Print both, and
        // hand the ticket to the manager report if one is waiting.
        move |ticket| {
            for (name, alpn) in &dials {
                info!(service = %name, ticket, alpn = %alpn,
                      "peerline-host: reachable (iroh — dial ticket with this alpn)");
            }
            if let Some(tx) = ticket_tx {
                let _ = tx.send(ticket.to_string());
            }
        },
        mounts,
    );
    // iroh is best-effort: an endpoint that never becomes remotely reachable
    // (no relay) fails ONLY the iroh transport, not the whole host — the other
    // transports keep serving. Log the failure, then park so this slot never
    // resolves and completes `try_join_all` (which would tear the rest down).
    // Dropping the serve future here also drops `ticket_tx`, so the manager
    // report proceeds without an iroh ticket instead of blocking on it.
    Ok(Box::pin(async move {
        if let Err(e) = serve.await {
            tracing::error!(error = %e, "peerline-host: iroh transport failed; \
                                         continuing without it");
        }
        std::future::pending::<()>().await;
        Ok(())
    }))
}
#[cfg(not(feature = "iroh"))]
fn iroh_serve(
    _: Option<PathBuf>,
    _: Vec<String>,
    _: Vec<(Vec<u8>, PeerHandler)>,
    _: Vec<(String, String)>,
    _: Option<oneshot::Sender<String>>,
) -> Result<Serve, String> {
    Err("peerline-host: iroh mounts configured but the `iroh` feature is disabled".into())
}

// --- manager auto-registration (`report_to`) -------------------------------

/// Build this host's endpoint report from its mounts and return the
/// registration loop future. Gated on `uds` (the manager is dialed over uds).
#[cfg(feature = "uds")]
fn build_report(
    manager_sock: PathBuf,
    prepared: &[(&'static str, Mount, PeerHandler)],
    ws_bind: Option<SocketAddr>,
    iroh_enabled: bool,
    ticket_rx: Option<oneshot::Receiver<String>>,
) -> Result<BoxFuture<'static, ()>, String> {
    use peerline_manager_protocol::Endpoints;

    let mut endpoints: std::collections::BTreeMap<String, Endpoints> =
        std::collections::BTreeMap::new();
    let mut iroh_dials: Vec<(String, String)> = Vec::new();
    for (name, mount, _) in prepared {
        let e = endpoints.entry(name.to_string()).or_default();
        if let (Some(addr), Some(path)) = (ws_bind, &mount.ws_path) {
            e.ws.push(format!("ws://{addr}{path}"));
        }
        if let Some(path) = &mount.uds_path {
            e.uds.push(path.display().to_string());
        }
        if iroh_enabled && let Some(alpn) = &mount.iroh_alpn {
            iroh_dials.push((name.to_string(), String::from_utf8_lossy(alpn).into_owned()));
        }
    }

    let host = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "peerline-host".into());

    Ok(Box::pin(report_loop(manager_sock, host, std::process::id(), endpoints, iroh_dials, ticket_rx)))
}

/// Resolve the report target: `Some` ⇒ the registration loop, `None` ⇒ a
/// no-op. Split on `uds` (the manager is dialed over uds); without it,
/// reporting is a no-op.
#[cfg(feature = "uds")]
fn build_report_future(
    report_sock: Option<PathBuf>,
    prepared: &[(&'static str, Mount, PeerHandler)],
    ws_bind: Option<SocketAddr>,
    iroh_enabled: bool,
    ticket_rx: Option<oneshot::Receiver<String>>,
) -> Result<BoxFuture<'static, ()>, String> {
    match report_sock {
        Some(sock) => build_report(sock, prepared, ws_bind, iroh_enabled, ticket_rx),
        None => Ok(Box::pin(std::future::pending())),
    }
}

#[cfg(not(feature = "uds"))]
fn build_report_future(
    _report_sock: Option<PathBuf>,
    _prepared: &[(&'static str, Mount, PeerHandler)],
    _ws_bind: Option<SocketAddr>,
    _iroh_enabled: bool,
    _ticket_rx: Option<oneshot::Receiver<String>>,
) -> Result<BoxFuture<'static, ()>, String> {
    Ok(Box::pin(std::future::pending()))
}

/// Fill in the iroh ticket (once bound), then keep the host registered with
/// the manager for as long as the process lives, reconnecting on failure.
#[cfg(feature = "uds")]
async fn report_loop(
    manager_sock: PathBuf,
    host: String,
    pid: u32,
    mut endpoints: std::collections::BTreeMap<String, peerline_manager_protocol::Endpoints>,
    iroh_dials: Vec<(String, String)>,
    ticket_rx: Option<oneshot::Receiver<String>>,
) {
    use peerline_manager_protocol::{IrohDial, Registration, ServiceEntry};

    // The iroh ticket isn't known until the endpoint binds.
    if let Some(rx) = ticket_rx
        && let Ok(ticket) = rx.await
    {
        for (name, alpn) in iroh_dials {
            endpoints.entry(name).or_default().iroh.push(IrohDial { ticket: ticket.clone(), alpn });
        }
    }

    let services: Vec<ServiceEntry> = endpoints
        .into_iter()
        .map(|(service, endpoints)| ServiceEntry { service, endpoints })
        .collect();
    let registration = Registration { host, pid, services };

    loop {
        if let Err(e) = report_once(&manager_sock, &registration).await {
            tracing::debug!(error = %e, manager = %manager_sock.display(),
                            "peerline-host: manager report failed; will retry");
        }
        // The connection ended (or never opened) — back off and re-register.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// How often the host heartbeats the manager. Well under the manager's TTL.
#[cfg(feature = "uds")]
const HEARTBEAT: std::time::Duration = std::time::Duration::from_millis(500);

/// One registration session: dial the manager, register, then heartbeat for
/// as long as the connection lives. Returns when the connection ends (manager
/// gone / restarted) so [`report_loop`] can reconnect. Dropping this future
/// (host shutdown) drops the connection; the manager then expires us on TTL.
#[cfg(feature = "uds")]
async fn report_once(
    manager_sock: &std::path::Path,
    registration: &peerline_manager_protocol::Registration,
) -> Result<(), String> {
    use peerline_manager_protocol::{Heartbeat, METHOD_HEARTBEAT, METHOD_REGISTER, RegisterAck};

    // Connect, retrying briefly (~1s) so a socket that's about to appear — a
    // service racing the manager at startup, or our own on self-registration
    // — is picked up promptly instead of after a full backoff.
    let (peer, driver) = 'connect: {
        let mut last = String::new();
        for _ in 0..20u32 {
            match peerline_transport_uds::connect(manager_sock).await {
                Ok(pair) => break 'connect pair,
                Err(e) => {
                    last = e;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
        return Err(last);
    };

    let work = async {
        let ack: RegisterAck =
            peer.call(METHOD_REGISTER, registration).await.map_err(|e| e.to_string())?;
        info!(id = ack.id, manager = %manager_sock.display(),
              "peerline-host: registered with manager");
        loop {
            tokio::time::sleep(HEARTBEAT).await;
            // A failed enqueue means the peer is gone; the driver will also
            // resolve, so either arm ends this session and triggers a retry.
            if peer.notify(METHOD_HEARTBEAT, &Heartbeat { id: ack.id }).is_err() {
                break;
            }
        }
        Ok::<(), String>(())
    };

    // Drive the connection and the register+heartbeat work together, both
    // owned here — so host shutdown (dropping this future) closes the wire.
    tokio::select! {
        _ = driver => {}
        r = work => r?,
    }
    Ok(())
}

/// Reject two services sharing one address on a transport — that would let
/// one silently shadow the other (and axum panics on a duplicate path).
fn reject_dupes<T: std::hash::Hash + Eq + std::fmt::Debug>(
    kind: &str,
    keys: impl Iterator<Item = T>,
) -> Result<(), String> {
    let mut seen = HashSet::new();
    for key in keys {
        if !seen.insert(format!("{key:?}")) {
            return Err(format!("peerline-host: two services share {kind} {key:?}"));
        }
    }
    Ok(())
}
