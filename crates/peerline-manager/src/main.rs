//! `peerline-manager` daemon: runs the [`Manager`] service on a host, and
//! optionally serves the web dashboard (`web/index.html`).
//!
//! Env:
//! - `PEERLINE_MANAGER_SOCK=<path>` — Unix socket to listen on
//!   (default `/tmp/peerline-manager.sock`).
//! - `PEERLINE_MANAGER_WS=<addr:port>` — also expose the RPC over WebSocket
//!   (required for the web dashboard and browser clients).
//! - `PEERLINE_MANAGER_UI=<addr:port>` — serve the web dashboard here (it
//!   connects back to the `PEERLINE_MANAGER_WS` port).
//! - `RUST_LOG=<spec>` — tracing filter (default `info`).

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;

use peerline_host::{Host, Mount};
use peerline_manager::Manager;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn env_addr(key: &str) -> Option<SocketAddr> {
    std::env::var(key).ok().and_then(|s| s.parse().ok())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let sock = std::env::var_os("PEERLINE_MANAGER_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/peerline-manager.sock"));

    let ws_addr = env_addr("PEERLINE_MANAGER_WS");
    let mut mount = Mount::new().uds(sock);
    let mut host = Host::new();
    if let Some(addr) = ws_addr {
        mount = mount.ws("/");
        host = host.ws_bind(addr);
    }

    // Serve the web dashboard, if configured. It talks peerline over the WS
    // RPC port, so the page is templated with that port.
    if let Some(ui_addr) = env_addr("PEERLINE_MANAGER_UI") {
        let ws_port = ws_addr.map(|a| a.port().to_string()).unwrap_or_default();
        let page = include_str!("../web/index.html").replace("{{WS_PORT}}", &ws_port);
        if ws_addr.is_none() {
            tracing::warn!("PEERLINE_MANAGER_UI set without PEERLINE_MANAGER_WS — the dashboard has no RPC port to reach");
        }
        tokio::spawn(serve_ui(ui_addr, page));
    }

    host.mount(Manager::new(), mount).run().await
}

/// Tiny static server for the dashboard page (one route, `GET /`).
async fn serve_ui(addr: SocketAddr, page: String) {
    use axum::response::Html;
    use axum::routing::get;
    use axum::Router;

    let app = Router::new().route(
        "/",
        get(move || {
            let page = page.clone();
            async move { Html(page) }
        }),
    );
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            info!(%addr, "peerline-manager: web dashboard at http://{addr}/");
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(error = %e, "peerline-manager: web dashboard stopped");
            }
        }
        Err(e) => tracing::error!(%addr, error = %e, "peerline-manager: web dashboard bind failed"),
    }
}
