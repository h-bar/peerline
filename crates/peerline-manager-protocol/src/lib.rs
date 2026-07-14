//! Wire schema for the peerline service manager.
//!
//! A **host** reports the services it runs and how to dial each of them; the
//! **manager** keeps that live and answers `list` / `lookup`. Pure JSON
//! shapes — no transport, no runtime. Method constants match the ops the
//! manager registers on its peer.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// Register a host's services. A unary request returning [`RegisterAck`]
/// with the assigned id. Liveness is by heartbeat + TTL: the host then
/// sends [`METHOD_HEARTBEAT`] periodically, and the manager expires any
/// registration it hasn't heard from within its TTL.
pub const METHOD_REGISTER: &str = "manager.register";
/// Keep a registration alive. A **notification** carrying [`Heartbeat`];
/// the manager refreshes that host's last-seen time.
pub const METHOD_HEARTBEAT: &str = "manager.heartbeat";
/// List every registered host and its services. → [`Listing`].
pub const METHOD_LIST: &str = "manager.list";
/// Find every dialable endpoint for a named service. → [`LookupResult`].
pub const METHOD_LOOKUP: &str = "manager.lookup";

/// One iroh dial coordinate: the shared endpoint ticket + this service's ALPN.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IrohDial {
    pub ticket: String,
    pub alpn: String,
}

/// How a single service can be reached, across every transport it's mounted on.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Endpoints {
    /// `ws://host:port/path` URLs.
    #[serde(default)]
    pub ws: Vec<String>,
    /// Unix domain socket paths.
    #[serde(default)]
    pub uds: Vec<String>,
    /// iroh ticket + ALPN pairs.
    #[serde(default)]
    pub iroh: Vec<IrohDial>,
}

/// A named service and where to reach it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceEntry {
    pub service: String,
    pub endpoints: Endpoints,
}

/// What a host sends to [`METHOD_REGISTER`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Registration {
    /// A human label for the host/instance (e.g. binary name).
    pub host: String,
    pub pid: u32,
    pub services: Vec<ServiceEntry>,
}

/// Reply to [`METHOD_REGISTER`] — the assigned id, echoed in heartbeats.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterAck {
    pub id: u64,
}

/// Payload of a [`METHOD_HEARTBEAT`] notification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    pub id: u64,
}

/// A registered host as the manager holds it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisteredHost {
    pub id: u64,
    pub host: String,
    pub pid: u32,
    pub services: Vec<ServiceEntry>,
}

/// Result of [`METHOD_LIST`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Listing {
    pub hosts: Vec<RegisteredHost>,
}

/// Args for [`METHOD_LOOKUP`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LookupArgs {
    pub service: String,
}

/// Result of [`METHOD_LOOKUP`] — every host's endpoints for that service.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LookupResult {
    pub endpoints: Vec<Endpoints>,
}
