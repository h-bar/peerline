//! End-to-end: two `Service`s registered on one `Host`, served over WS,
//! each reached at its own mount path and answering in isolation.

use std::time::Duration;

use peerline::runtime::Peer;
use peerline::wire::RpcError;
use peerline_host::{Host, Mount, Service};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EchoArgs {
    s: String,
}

/// A service whose `echo` tags its reply with the service name, so the
/// test can prove which mount answered.
struct Echo {
    name: &'static str,
}

impl Service for Echo {
    fn name(&self) -> &'static str {
        self.name
    }
    fn register(&self, peer: &Peer) {
        let tag = self.name;
        peer.on_request("echo", move |a: EchoArgs| async move {
            Ok::<_, RpcError>(format!("{tag}:{}", a.s))
        });
    }
}

async fn dial(url: &str) -> (Peer, futures::future::BoxFuture<'static, ()>) {
    let mut attempt = 0u32;
    loop {
        match peerline_transport_ws::connect(url).await {
            Ok(pair) => break pair,
            Err(_) if attempt < 100 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("connect {url}: {e}"),
        }
    }
}

#[tokio::test]
async fn host_mounts_two_services_by_path() {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("probe bind");
    let addr = probe.local_addr().expect("local_addr");
    drop(probe);

    let host = Host::new()
        .mount(Echo { name: "alpha" }, Mount::new().ws("/alpha"))
        .mount(Echo { name: "beta" }, Mount::new().ws("/beta"))
        .ws_bind(addr);
    tokio::spawn(async move {
        let _ = host.run().await;
    });

    let (alpha, d1) = dial(&format!("ws://{addr}/alpha")).await;
    tokio::spawn(d1);
    let (beta, d2) = dial(&format!("ws://{addr}/beta")).await;
    tokio::spawn(d2);

    let a: String = alpha.call("echo", &EchoArgs { s: "hi".into() }).await.expect("alpha");
    let b: String = beta.call("echo", &EchoArgs { s: "hi".into() }).await.expect("beta");
    assert_eq!(a, "alpha:hi");
    assert_eq!(b, "beta:hi");
}

#[tokio::test]
async fn run_rejects_empty_dupe_and_no_mount() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

    // No services.
    let err = Host::new().ws_bind(addr).run().await.unwrap_err();
    assert!(err.contains("no services"), "{err}");

    // Two services colliding on one WS path.
    let err = Host::new()
        .mount(Echo { name: "a" }, Mount::new().ws("/same"))
        .mount(Echo { name: "b" }, Mount::new().ws("/same"))
        .ws_bind(addr)
        .run()
        .await
        .unwrap_err();
    assert!(err.contains("share ws path"), "{err}");

    // A service exists but no transport is active (mount requests WS, host
    // never binds a WS port).
    let err = Host::new()
        .mount(Echo { name: "x" }, Mount::new().ws("/x"))
        .run()
        .await
        .unwrap_err();
    assert!(err.contains("no active mounts"), "{err}");
}
