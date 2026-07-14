//! Iroh P2P (QUIC) transport for peerline.
//!
//! Both ends of the peer link depend on this crate so they cannot drift:
//! an accepting service and the dial side (`tauri-plugin-peerline-iroh`).
//! It owns:
//!
//! 1. [`encode_ticket`] / [`decode_ticket`] — [`TICKET_PREFIX`] + base32
//!    of the postcard-encoded [`EndpointAddr`]. Both ends must pin the
//!    same `iroh`/`iroh-base` major so the encoding stays compatible.
//! 2. [`text_frames`] — length-delimited `String` frames over a joined
//!    QUIC duplex (a raw bi-stream has no message boundaries).
//! 3. [`serve`] — the reusable accept loop: bind an endpoint, hand its
//!    ticket back once, and drive a [`peerline::runtime::Peer`] per
//!    accepted bi-stream, configured by a caller-supplied handler
//!    closure. [`load_or_create_secret_key`] persists a stable identity.
//!
//! The **ALPN is deliberately NOT owned here**: it names a *specific*
//! service, and one host may run several peerline services behind a
//! single endpoint (one ALPN each). So each service passes its own ALPN
//! to [`serve`] (and the dial plugin passes it per `connect`), while the
//! ticket codec and framing stay shared and generic.

#![forbid(unsafe_code)]

use std::io;
use std::path::Path;
use std::pin::Pin;

use bytes::Bytes;
use data_encoding::BASE32_NOPAD;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use iroh::endpoint::{Connection, RecvStream, SendStream, presets};
use iroh::Endpoint;
use peerline::runtime::Peer;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{info, warn};

pub use iroh_base::{EndpointAddr, SecretKey};

/// Human-facing ticket prefix — service-neutral, so every peerline-iroh
/// ticket is recognizable at a glance and cannot be confused with a
/// `host:port` address. Services are distinguished by ALPN, not by this
/// prefix, so one shared value is enough.
pub const TICKET_PREFIX: &str = "peerline1";

/// Encode an [`EndpointAddr`] as a copy-pasteable ticket:
/// `peerline1<base32(postcard(addr))>`.
pub fn encode_ticket(addr: &EndpointAddr) -> Result<String, String> {
    let bytes = postcard::to_stdvec(addr).map_err(|e| format!("iroh ticket encode: {e}"))?;
    Ok(format!("{TICKET_PREFIX}{}", BASE32_NOPAD.encode(&bytes)))
}

/// Decode a ticket produced by [`encode_ticket`]. Rejects a missing or
/// foreign prefix and any base32 / postcard failure.
pub fn decode_ticket(ticket: &str) -> Result<EndpointAddr, String> {
    let body = ticket
        .strip_prefix(TICKET_PREFIX)
        .ok_or_else(|| format!("iroh ticket missing `{TICKET_PREFIX}` prefix"))?;
    let bytes = BASE32_NOPAD
        .decode(body.as_bytes())
        .map_err(|e| format!("iroh ticket base32: {e}"))?;
    postcard::from_bytes(&bytes).map_err(|e| format!("iroh ticket decode: {e}"))
}

/// A length-delimited text-frame sink over the QUIC duplex.
pub type FrameSink = Pin<Box<dyn Sink<String, Error = io::Error> + Send>>;
/// A length-delimited text-frame stream over the QUIC duplex.
pub type FrameStream = Pin<Box<dyn Stream<Item = Result<String, io::Error>> + Send>>;

/// Wrap a joined QUIC duplex (`tokio::io::join(recv, send)`) as
/// length-delimited `String` frames — the shared framing both ends use.
/// The sink accepts one JSON-RPC frame per `send`; the stream yields one
/// per item.
pub fn text_frames<S>(io: S) -> (FrameSink, FrameStream)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    // Pin the frame ceiling to the shared [`peerline::MAX_FRAME_LEN`]
    // rather than the codec's 8 MiB default, so the acceptor and dialer
    // (both routed through this one helper) agree and a large JSON-RPC
    // frame doesn't silently kill the connection.
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(peerline::MAX_FRAME_LEN)
        .new_codec();
    let framed = Framed::new(io, codec);
    let (sink, stream) = framed.split();
    let sink = sink.with(|text: String| async move {
        Ok::<Bytes, io::Error>(Bytes::from(text.into_bytes()))
    });
    // Fail loudly on a non-UTF-8 frame rather than silently substituting
    // replacement characters: JSON-RPC frames are always UTF-8, so a bad
    // frame means a corrupt wire, and surfacing it ends the stream.
    let stream = stream.map(|frame| {
        frame.and_then(|b| {
            String::from_utf8(b.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        })
    });
    (Box::pin(sink), Box::pin(stream))
}

/// Resolve the endpoint identity. `None` ⇒ a fresh ephemeral key (the
/// ticket rotates every boot). `Some(path)` ⇒ read the 32-byte key at
/// `path`, or generate + persist one there on first run so the ticket
/// (and thus the NodeId clients pin) is stable across restarts. The key
/// file is written owner-only (`0600` on unix) — it is long-lived
/// private key material.
pub fn load_or_create_secret_key(path: Option<&Path>) -> io::Result<SecretKey> {
    let Some(path) = path else {
        return Ok(SecretKey::generate());
    };
    if path.exists() {
        let bytes = std::fs::read(path)?;
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "secret key file must be exactly 32 bytes")
        })?;
        Ok(SecretKey::from_bytes(&arr))
    } else {
        let sk = SecretKey::generate();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        write_secret_key(path, &sk.to_bytes())?;
        Ok(sk)
    }
}

/// Persist the 32-byte secret key with owner-only permissions (`0600` on
/// unix) so the endpoint's private identity is never world-readable.
fn write_secret_key(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)?.write_all(bytes)
}

/// Bind an iroh endpoint for `alpn` with the given identity, then run the
/// accept loop forever: for each accepted bi-stream, build a
/// [`peerline::runtime::Peer`] over the shared text framing, hand it to
/// `on_peer` (register your handlers there), and drive it until the
/// stream ends.
///
/// `on_ticket` is called exactly once, after the endpoint binds, with the
/// pasteable ticket — print or log it however the service prefers. The
/// ALPN is the caller's service id; it must match what the dial side
/// passes to `connect`.
///
/// NOTE — the `Peer` is built LAZILY, only after `accept_bi` resolves,
/// which (per QUIC) happens once the client sends its first frame. This
/// relies on the client speaking first — true for peerline's RPC. A
/// server-initiated-first flow would need to open its own bi-stream.
pub async fn serve<T, F>(
    alpn: &[u8],
    secret_key: SecretKey,
    on_ticket: T,
    on_peer: F,
) -> Result<(), String>
where
    T: FnOnce(&str),
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![alpn.to_vec()])
        .secret_key(secret_key)
        .bind()
        .await
        .map_err(|e| format!("iroh bind: {e}"))?;

    let ticket = encode_ticket(&endpoint.addr())?;
    info!(endpoint_id = %endpoint.id(), "peerline-iroh endpoint listening");
    on_ticket(&ticket);

    while let Some(incoming) = endpoint.accept().await {
        let on_peer = on_peer.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => serve_conn(conn, on_peer).await,
                Err(e) => warn!(error = %e, "peerline-iroh: incoming connection failed"),
            }
        });
    }
    Ok(())
}

/// Drive one accepted connection: each bi-stream the client opens becomes
/// its own peer session (looping keeps the connection alive across a
/// reconnect).
async fn serve_conn<F>(conn: Connection, on_peer: F)
where
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    let remote = conn.remote_id();
    info!(%remote, "peerline-iroh connection opened");
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let on_peer = on_peer.clone();
                tokio::spawn(async move { drive_stream(send, recv, on_peer).await });
            }
            Err(e) => {
                info!(%remote, error = %e, "peerline-iroh connection closed");
                break;
            }
        }
    }
}

/// Bridge one bi-stream to a peerline [`Peer`] via the shared text-frame
/// codec, run the handler set (`on_peer`), then drive until it ends.
async fn drive_stream<F>(send: SendStream, recv: RecvStream, on_peer: F)
where
    F: Fn(&Peer),
{
    let (sink, stream) = text_frames(tokio::io::join(recv, send));
    let (peer, driver) = Peer::new(sink, stream);
    on_peer(&peer);
    driver.await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_roundtrips() {
        let addr = EndpointAddr::from(SecretKey::generate().public());
        let ticket = encode_ticket(&addr).expect("encode");
        assert!(ticket.starts_with(TICKET_PREFIX));
        assert_eq!(decode_ticket(&ticket).expect("decode"), addr);
    }

    #[test]
    fn foreign_prefix_is_rejected() {
        assert!(decode_ticket("ws://127.0.0.1:6465").is_err());
        assert!(decode_ticket("").is_err());
        assert!(decode_ticket("peerline1!!!notbase32!!!").is_err());
    }
}
