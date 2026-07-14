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

/// Builder for a unified multi-service peerline host.
#[derive(Default)]
pub struct Host {
    services: Vec<(Arc<dyn Service>, Mount)>,
    ws_bind: Option<SocketAddr>,
    iroh_key: Option<Option<PathBuf>>,
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
                serves.push(iroh_serve(key_path.clone(), mounts, dials)?);
            }
        }

        if serves.is_empty() {
            return Err("peerline-host: no active mounts (configure ws_bind / iroh_endpoint \
                        and give each service a Mount)"
                .into());
        }

        info!(services = self.services.len(), transports = serves.len(), "peerline-host starting");

        // Run every transport until one errors or shutdown fires. Dropping
        // the serve futures on shutdown tears the listeners down.
        tokio::select! {
            r = futures::future::try_join_all(serves) => r.map(|_| ()),
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
    mounts: Vec<(Vec<u8>, PeerHandler)>,
    dials: Vec<(String, String)>,
) -> Result<Serve, String> {
    let key = peerline_transport_iroh::load_or_create_secret_key(key_path.as_deref())
        .map_err(|e| format!("peerline-host: iroh key: {e}"))?;
    Ok(Box::pin(peerline_transport_iroh::serve_mounted(
        key,
        // The one ticket addresses the shared endpoint; each service is
        // reached by dialing it with that service's ALPN. Print both.
        move |ticket| {
            for (name, alpn) in &dials {
                info!(service = %name, ticket, alpn = %alpn,
                      "peerline-host: reachable (iroh — dial ticket with this alpn)");
            }
        },
        mounts,
    )))
}
#[cfg(not(feature = "iroh"))]
fn iroh_serve(
    _: Option<PathBuf>,
    _: Vec<(Vec<u8>, PeerHandler)>,
    _: Vec<(String, String)>,
) -> Result<Serve, String> {
    Err("peerline-host: iroh mounts configured but the `iroh` feature is disabled".into())
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
