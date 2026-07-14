//! End-to-end: a host with `report_to` auto-registers with the manager and
//! is deregistered when it shuts down.

use std::time::Duration;

use peerline::runtime::Peer;
use peerline_host::{Host, Mount, Service};
use peerline_manager::Manager;
use peerline_manager_protocol::{Listing, METHOD_LIST};
use serde_json::json;

/// A service with no handlers — we only care that it gets reported.
struct Dummy;
impl Service for Dummy {
    fn name(&self) -> &'static str {
        "dummy"
    }
    fn register(&self, _peer: &Peer) {}
}

async fn connect(sock: &std::path::Path) -> Peer {
    for _ in 0..300 {
        if let Ok((peer, driver)) = peerline_transport_uds::connect(sock).await {
            tokio::spawn(driver);
            return peer;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("socket never came up: {}", sock.display());
}

async fn list(peer: &Peer) -> Listing {
    peer.call(METHOD_LIST, &json!({})).await.expect("list")
}

#[tokio::test]
async fn host_auto_registers_and_deregisters() {
    let pid = std::process::id();
    let mgr_sock = std::env::temp_dir().join(format!("peerline-mgr-report-{pid}.sock"));
    let host_sock = std::env::temp_dir().join(format!("peerline-dummy-{pid}.sock"));

    // Manager.
    let m = mgr_sock.clone();
    tokio::spawn(async move {
        let _ = Host::new().mount(Manager::new(), Mount::new().uds(m)).no_report().run().await;
    });

    // A reporting host with a controllable shutdown.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (hs, ms) = (host_sock.clone(), mgr_sock.clone());
    tokio::spawn(async move {
        let _ = Host::new()
            .mount(Dummy, Mount::new().uds(hs))
            .report_to(ms)
            .run_until(async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let mon = connect(&mgr_sock).await;

    // The host should appear, reporting its uds endpoint.
    let mut registered = false;
    for _ in 0..400 {
        let listing = list(&mon).await;
        if let Some(host) = listing.hosts.first() {
            let svc = &host.services[0];
            assert_eq!(svc.service, "dummy");
            assert_eq!(svc.endpoints.uds, vec![host_sock.display().to_string()]);
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(registered, "host did not auto-register with the manager");

    // Shutting the host down should deregister it (connection-based liveness).
    let _ = shutdown_tx.send(());
    let mut gone = false;
    for _ in 0..400 {
        if list(&mon).await.hosts.is_empty() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(gone, "host was not deregistered after shutdown");

    let _ = tokio::fs::remove_file(&mgr_sock).await;
    let _ = tokio::fs::remove_file(&host_sock).await;
}
