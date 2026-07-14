//! `peerline-manager` daemon: runs the [`Manager`] service on a host and
//! serves the web dashboard (`web/index.html`).
//!
//! Zero config works out of the box — it listens on a Unix socket, exposes
//! the RPC over WebSocket on `127.0.0.1:6466`, and serves the dashboard on
//! <http://127.0.0.1:6467/>. Override or disable any of it via env:
//!
//! - `PEERLINE_MANAGER_SOCK=<path>` — Unix socket (default
//!   `/tmp/peerline-manager.sock`).
//! - `PEERLINE_MANAGER_WS=<addr:port>` — WebSocket RPC bind (default
//!   `127.0.0.1:6466`; `off` to disable). Needed by browser clients.
//! - `PEERLINE_MANAGER_UI=<addr:port>` — web dashboard bind (default
//!   `127.0.0.1:6467`; `off` to disable). Connects back to the WS port.
//! - `RUST_LOG=<spec>` — tracing filter (default `info`).

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;

use peerline_host::{Host, Mount};
use peerline_manager::Manager;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Canonical defaults — chosen so the manager + dashboard just work with no
/// env config, on loopback only (override to `0.0.0.0:…` to expose on a LAN).
const DEFAULT_SOCK: &str = "/tmp/peerline-manager.sock";
const DEFAULT_WS: &str = "127.0.0.1:6466";
const DEFAULT_UI: &str = "127.0.0.1:6467";

/// Resolve an optional listen address: unset ⇒ the canonical default (on);
/// `off` / `none` / empty ⇒ disabled; otherwise the parsed address.
fn resolve_addr(key: &str, default: &str) -> Option<SocketAddr> {
    let raw = std::env::var(key).unwrap_or_else(|_| default.to_string());
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("off") || raw.eq_ignore_ascii_case("none") {
        return None;
    }
    match raw.parse() {
        Ok(addr) => Some(addr),
        Err(_) => {
            tracing::warn!(key, value = raw, "peerline-manager: invalid address, disabling");
            None
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let sock = std::env::var_os("PEERLINE_MANAGER_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SOCK));

    let ws_addr = resolve_addr("PEERLINE_MANAGER_WS", DEFAULT_WS);
    let ui_addr = resolve_addr("PEERLINE_MANAGER_UI", DEFAULT_UI);

    let mut mount = Mount::new().uds(sock);
    let mut host = Host::new();
    if let Some(addr) = ws_addr {
        mount = mount.ws("/");
        host = host.ws_bind(addr);
    }

    // Serve the web dashboard. It talks peerline over the WS RPC port, so the
    // page is templated with that port.
    if let Some(ui_addr) = ui_addr {
        let ws_port = ws_addr.map(|a| a.port().to_string()).unwrap_or_default();
        let page = include_str!("../web/index.html").replace("{{WS_PORT}}", &ws_port);
        if ws_addr.is_none() {
            tracing::warn!("web dashboard enabled but WS is off — the page has no RPC port to reach (set PEERLINE_MANAGER_WS)");
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
