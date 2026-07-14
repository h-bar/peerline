//! End-to-end check that [`serve`] (accept) and [`connect`] (dial) speak
//! the same wire: an in-process server registers an `echo` handler, a
//! dialed client calls it, and the reply round-trips. Exercises the full
//! `Peer` path over a real Unix domain socket (no network).

use std::sync::Arc;
use std::time::Duration;

use peerline::runtime::Peer;
use peerline::wire::RpcError;
use peerline_transport_uds::{PeerHandler, connect, serve, serve_mounted};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EchoArgs {
    s: String,
}

// Dial with a bounded retry: `serve` unlinks + binds asynchronously, so
// the socket may not exist for the first attempt(s).
async fn dial_retry(path: &std::path::Path) -> (Peer, futures::future::BoxFuture<'static, ()>) {
    let mut attempt = 0u32;
    loop {
        match connect(path).await {
            Ok(pair) => break pair,
            Err(_) if attempt < 100 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("connect {}: {e}", path.display()),
        }
    }
}

#[tokio::test]
async fn serve_and_dial_roundtrip() {
    // A per-process socket path under the temp dir (unlinked by `serve`).
    let path = std::env::temp_dir().join(format!("peerline-uds-{}.sock", std::process::id()));

    let server_path = path.clone();
    let server = tokio::spawn(async move {
        serve(&server_path, |peer| {
            peer.on_request("echo", |a: EchoArgs| async move { Ok::<_, RpcError>(a.s) });
        })
        .await
        .expect("serve");
    });

    let (peer, driver) = dial_retry(&path).await;
    tokio::spawn(driver);

    let reply: String = peer
        .call("echo", &EchoArgs { s: "hi".into() })
        .await
        .expect("call");
    assert_eq!(reply, "hi");

    server.abort();
    let _ = tokio::fs::remove_file(&path).await;
}

/// Two services mounted on distinct sockets: each socket routes to its own
/// peer, so their `echo` handlers stay isolated.
#[tokio::test]
async fn mounted_sockets_route_to_distinct_services() {
    let dir = std::env::temp_dir();
    let pid = std::process::id();
    let path_a = dir.join(format!("peerline-uds-{pid}-a.sock"));
    let path_b = dir.join(format!("peerline-uds-{pid}-b.sock"));

    let mount = |tag: &'static str| -> PeerHandler {
        Arc::new(move |peer: &Peer| {
            peer.on_request("echo", move |a: EchoArgs| async move {
                Ok::<_, RpcError>(format!("{tag}:{}", a.s))
            });
        })
    };
    let (sa, sb) = (path_a.clone(), path_b.clone());
    let server = tokio::spawn(async move {
        serve_mounted(vec![(sa, mount("A")), (sb, mount("B"))]).await.expect("serve_mounted");
    });

    let (peer_a, driver_a) = dial_retry(&path_a).await;
    tokio::spawn(driver_a);
    let (peer_b, driver_b) = dial_retry(&path_b).await;
    tokio::spawn(driver_b);

    let a: String = peer_a.call("echo", &EchoArgs { s: "hi".into() }).await.expect("call a");
    let b: String = peer_b.call("echo", &EchoArgs { s: "hi".into() }).await.expect("call b");
    assert_eq!(a, "A:hi");
    assert_eq!(b, "B:hi");

    server.abort();
    let _ = tokio::fs::remove_file(&path_a).await;
    let _ = tokio::fs::remove_file(&path_b).await;
}
