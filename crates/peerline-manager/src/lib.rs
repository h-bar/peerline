//! The peerline service manager — a live registry that is itself a peerline
//! [`Service`].
//!
//! Mount [`Manager`] on a [`peerline_host::Host`] (typically over a local
//! Unix socket) and every peerline client can talk to it over peerline:
//!
//! - Hosts call [`METHOD_REGISTER`] (a stream) to report their services and
//!   dial coordinates, holding the stream open for the process lifetime.
//! - Clients call [`METHOD_LIST`] / [`METHOD_LOOKUP`] to discover who's live
//!   and how to reach them.
//!
//! **Liveness is heartbeat + TTL.** A host registers (unary), then sends
//! [`METHOD_HEARTBEAT`] periodically; any registration the manager hasn't
//! heard from within [`TTL`] is expired lazily on the next `list`/`lookup`.
//! (peerline drains in-flight handlers on connection close, so a parked
//! "hold the stream open" handler can't detect a hard crash — hence the
//! heartbeat.)

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use peerline::runtime::Peer;
use peerline::wire::RpcError;
use peerline_host::Service;
use peerline_manager_protocol::{
    Heartbeat, Listing, LookupArgs, LookupResult, METHOD_HEARTBEAT, METHOD_LIST, METHOD_LOOKUP,
    METHOD_REGISTER, RegisterAck, RegisteredHost, Registration,
};
use tracing::info;

/// A registration expires this long after its last heartbeat. Comfortably
/// larger than the host's heartbeat interval so a late heartbeat (tokio
/// timers can stretch under load) doesn't drop a live host.
pub const TTL: Duration = Duration::from_secs(2);

struct Entry {
    host: RegisteredHost,
    last_seen: Instant,
}

#[derive(Default)]
struct Registry {
    next_id: u64,
    entries: HashMap<u64, Entry>,
}

impl Registry {
    /// Drop registrations not seen within [`TTL`] (lazy reaping — no
    /// background task).
    fn prune(&mut self, now: Instant) {
        self.entries.retain(|id, e| {
            let live = now.duration_since(e.last_seen) < TTL;
            if !live {
                info!(id, host = %e.host.host, "manager: host expired (no heartbeat)");
            }
            live
        });
    }
}

/// The service manager. Cheap to clone (shares one registry); mount it on a
/// host and, optionally, read the registry directly with [`Manager::hosts`].
#[derive(Clone, Default)]
pub struct Manager {
    registry: Arc<Mutex<Registry>>,
}

impl Manager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of the live registered hosts (for embedding/monitoring).
    pub fn hosts(&self) -> Vec<RegisteredHost> {
        let mut g = lock(&self.registry);
        g.prune(Instant::now());
        g.entries.values().map(|e| e.host.clone()).collect()
    }
}

fn lock(m: &Mutex<Registry>) -> MutexGuard<'_, Registry> {
    // Every holder mutates with non-panicking ops, so a poisoned guard still
    // wraps consistent data — recover it and stay panic-free.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Service for Manager {
    fn name(&self) -> &'static str {
        "manager"
    }

    fn register(&self, peer: &Peer) {
        // register — unary. Insert with a fresh last-seen and return the id.
        let reg = self.registry.clone();
        peer.on_request(METHOD_REGISTER, move |r: Registration| {
            let reg = reg.clone();
            async move {
                let mut g = lock(&reg);
                let id = g.next_id;
                g.next_id += 1;
                info!(id, host = %r.host, pid = r.pid, "manager: host registered");
                g.entries.insert(
                    id,
                    Entry {
                        host: RegisteredHost { id, host: r.host, pid: r.pid, services: r.services },
                        last_seen: Instant::now(),
                    },
                );
                Ok::<_, RpcError>(RegisterAck { id })
            }
        });

        // heartbeat — notification. Refresh last-seen.
        let reg = self.registry.clone();
        peer.on_notification(METHOD_HEARTBEAT, move |h: Heartbeat| {
            let reg = reg.clone();
            async move {
                if let Some(e) = lock(&reg).entries.get_mut(&h.id) {
                    e.last_seen = Instant::now();
                }
            }
        });

        let reg = self.registry.clone();
        peer.on_request(METHOD_LIST, move |_: serde_json::Value| {
            let reg = reg.clone();
            async move {
                let mut g = lock(&reg);
                g.prune(Instant::now());
                Ok::<_, RpcError>(Listing { hosts: g.entries.values().map(|e| e.host.clone()).collect() })
            }
        });

        let reg = self.registry.clone();
        peer.on_request(METHOD_LOOKUP, move |a: LookupArgs| {
            let reg = reg.clone();
            async move {
                let mut g = lock(&reg);
                g.prune(Instant::now());
                let endpoints = g
                    .entries
                    .values()
                    .flat_map(|e| {
                        e.host
                            .services
                            .iter()
                            .filter(|s| s.service == a.service)
                            .map(|s| s.endpoints.clone())
                    })
                    .collect();
                Ok::<_, RpcError>(LookupResult { endpoints })
            }
        });
    }
}
