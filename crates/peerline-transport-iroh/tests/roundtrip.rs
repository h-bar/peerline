//! Headless transport-level check for the iroh wire contract: two
//! in-process iroh endpoints, dialed by ticket, exchange length-delimited
//! text frames over a bi-stream using the exact [`text_frames`] both ends
//! share. Pins the transport seam (ticket codec, `open_bi` / `accept_bi`
//! ordering, shared framing) without standing up a peerline `Peer`.

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use iroh::endpoint::presets;
use iroh::Endpoint;
use iroh_base::SecretKey;
use peerline_transport_iroh::{decode_ticket, encode_ticket, text_frames};

// Any ALPN works for a transport-level roundtrip; a real service passes
// its own service id here.
const ALPN: &[u8] = b"peerline/iroh/test/1";

#[tokio::test]
async fn ticket_dial_and_frame_roundtrip() {
    // Server endpoint: accept the shared ALPN, echo one framed message.
    let server = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .secret_key(SecretKey::generate())
        .bind()
        .await
        .expect("server bind");
    // Direct (loopback/LAN) addresses are discovered locally and appear
    // within moments of bind — wait for them so the ticket is dialable
    // without a relay (keeps the test offline-safe).
    let addr = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let a = server.addr();
            if !a.addrs.is_empty() {
                break a;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("server acquired direct addresses");
    let ticket = encode_ticket(&addr).expect("encode ticket");

    let server_task = tokio::spawn(async move {
        let incoming = server.accept().await.expect("incoming");
        let conn = incoming.await.expect("accept conn");
        let (send, recv) = conn.accept_bi().await.expect("accept_bi");
        let (mut sink, mut stream) = text_frames(tokio::io::join(recv, send));
        let msg = stream.next().await.expect("server recv").expect("frame");
        sink.send(msg).await.expect("server echo"); // echo verbatim
        // Keep the connection alive until the client has read the echo.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });

    // Client dials purely from the ticket — no out-of-band address.
    let client = Endpoint::builder(presets::N0)
        .bind()
        .await
        .expect("client bind");
    let dialed = decode_ticket(&ticket).expect("decode ticket");
    let conn = client.connect(dialed, ALPN).await.expect("connect");
    let (send, recv) = conn.open_bi().await.expect("open_bi");
    let (mut sink, mut stream) = text_frames(tokio::io::join(recv, send));
    sink.send("{\"jsonrpc\":\"2.0\"}".to_string()).await.expect("send");
    let echoed = stream.next().await.expect("recv").expect("frame");

    assert_eq!(echoed, "{\"jsonrpc\":\"2.0\"}");
    server_task.await.expect("server task");
}
