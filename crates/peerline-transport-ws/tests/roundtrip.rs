//! End-to-end check that [`serve`] (accept, axum) and [`connect`] (dial,
//! tokio-tungstenite) speak the same wire: an in-process server registers
//! an `echo` handler, a dialed client calls it over loopback TCP, and the
//! reply round-trips through the full `Peer` path.

use std::time::Duration;

use peerline::wire::RpcError;
use peerline_transport_ws::{connect, serve};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EchoArgs {
    s: String,
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

    // Dial with a bounded retry until the server has bound the port.
    let url = format!("ws://{addr}");
    let (peer, driver) = {
        let mut attempt = 0u32;
        loop {
            match connect(&url).await {
                Ok(pair) => break pair,
                Err(_) if attempt < 100 => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => panic!("connect: {e}"),
            }
        }
    };
    tokio::spawn(driver);

    let reply: String = peer
        .call("echo", &EchoArgs { s: "hi".into() })
        .await
        .expect("call");
    assert_eq!(reply, "hi");

    server.abort();
}
