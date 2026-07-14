//! `peerline-manager` daemon: runs the [`Manager`] service on a host.
//!
//! Env:
//! - `PEERLINE_MANAGER_SOCK=<path>` — Unix socket to listen on
//!   (default `/tmp/peerline-manager.sock`).
//! - `PEERLINE_MANAGER_WS=<addr:port>` — also expose it over WebSocket.
//! - `RUST_LOG=<spec>` — tracing filter (default `info`).

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;

use peerline_host::{Host, Mount};
use peerline_manager::Manager;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let sock = std::env::var_os("PEERLINE_MANAGER_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/peerline-manager.sock"));

    let mut mount = Mount::new().uds(sock);
    let mut host = Host::new();
    if let Some(addr) = std::env::var("PEERLINE_MANAGER_WS").ok().and_then(|s| s.parse::<SocketAddr>().ok())
    {
        mount = mount.ws("/");
        host = host.ws_bind(addr);
    }

    host.mount(Manager::new(), mount).run().await
}
