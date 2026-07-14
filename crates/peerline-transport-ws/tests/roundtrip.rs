//! End-to-end check that [`serve`] (accept, axum) and [`connect`] (dial,
//! tokio-tungstenite) speak the same wire: an in-process server registers
//! an `echo` handler, a dialed client calls it over loopback TCP, and the
//! reply round-trips through the full `Peer` path.

use std::sync::Arc;
use std::time::Duration;

use peerline::runtime::Peer;
use peerline::wire::RpcError;
use peerline_transport_ws::{connect, serve, serve_mounted};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EchoArgs {
    s: String,
}

// Dial with a bounded retry until the server has bound the port.
async fn dial_retry(url: &str) -> (Peer, futures::future::BoxFuture<'static, ()>) {
    let mut attempt = 0u32;
    loop {
        match connect(url).await {
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
async fn serve_and_dial_roundtrip() {
    // Discover a free loopback port, then hand it to `serve` (which binds
    // it itself). The drop→rebind window is tiny and local.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("probe bind");
    let addr = probe.local_addr().expect("local_addr");
    drop(probe);

    let server = tokio::spawn(async move {
        serve(addr, |peer| {
            peer.on_request("echo", |a: EchoArgs| async move { Ok::<_, RpcError>(a.s) });
        })
        .await
        .expect("serve");
    });

    let (peer, driver) = dial_retry(&format!("ws://{addr}")).await;
    tokio::spawn(driver);

    let reply: String = peer
        .call("echo", &EchoArgs { s: "hi".into() })
        .await
        .expect("call");
    assert_eq!(reply, "hi");

    server.abort();
}

/// Two services mounted by path on one port: each path routes to its own
/// peer, so their `echo` handlers stay isolated (no op collision).
#[tokio::test]
async fn mounted_paths_route_to_distinct_services() {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("probe bind");
    let addr = probe.local_addr().expect("local_addr");
    drop(probe);

    // Each mount tags its reply so we can tell which service answered.
    let mount = |tag: &'static str| -> peerline_transport_ws::PeerHandler {
        Arc::new(move |peer: &Peer| {
            peer.on_request("echo", move |a: EchoArgs| async move {
                Ok::<_, RpcError>(format!("{tag}:{}", a.s))
            });
        })
    };
    let server = tokio::spawn(async move {
        serve_mounted(addr, vec![("/a".into(), mount("A")), ("/b".into(), mount("B"))])
            .await
            .expect("serve_mounted");
    });

    let (peer_a, driver_a) = dial_retry(&format!("ws://{addr}/a")).await;
    tokio::spawn(driver_a);
    let (peer_b, driver_b) = dial_retry(&format!("ws://{addr}/b")).await;
    tokio::spawn(driver_b);

    let a: String = peer_a.call("echo", &EchoArgs { s: "hi".into() }).await.expect("call a");
    let b: String = peer_b.call("echo", &EchoArgs { s: "hi".into() }).await.expect("call b");
    assert_eq!(a, "A:hi");
    assert_eq!(b, "B:hi");

    server.abort();
}
