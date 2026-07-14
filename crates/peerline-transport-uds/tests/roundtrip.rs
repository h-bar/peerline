//! End-to-end check that [`serve`] (accept) and [`connect`] (dial) speak
//! the same wire: an in-process server registers an `echo` handler, a
//! dialed client calls it, and the reply round-trips. Exercises the full
//! `Peer` path over a real Unix domain socket (no network).

use std::time::Duration;

use peerline::wire::RpcError;
use peerline_transport_uds::{connect, serve};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EchoArgs {
    s: String,
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

    // Dial with a bounded retry: `serve` unlinks + binds asynchronously,
    // so the socket may not exist for the first attempt(s).
    let (peer, driver) = {
        let mut attempt = 0u32;
        loop {
            match connect(&path).await {
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
    let _ = tokio::fs::remove_file(&path).await;
}
