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
//! 4. [`connect`] — the native-Rust dial side: bind an ephemeral
//!    endpoint, dial a ticket for a given ALPN, and return one
//!    `(Peer, driver)`. (The `tauri-plugin-peerline-iroh` crate is the
//!    JS-bridge dial side for Tauri apps; it shares the same framing.)
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
use std::sync::Arc;

use bytes::Bytes;
use data_encoding::BASE32_NOPAD;
use futures::future::BoxFuture;
use futures::sink::{Sink, SinkExt};
use futures::stream::{Stream, StreamExt};
use iroh::endpoint::{Connection, RecvStream, SendStream, presets};
use iroh::{Endpoint, RelayConfig, RelayMap, RelayMode};
use peerline::runtime::Peer;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{info, warn};

pub use iroh_base::{EndpointAddr, RelayUrl, SecretKey};

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

/// A type-erased per-connection peer initializer — the `on_peer` closure
/// after boxing, so a routing table can hold heterogeneous services.
pub type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;

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

/// iroh **acceptor** settings ([`serve`] / [`serve_mounted`]). Passed in by
/// the caller — never read from the environment — so a consuming app owns how
/// it is sourced (its own config file, etc.). Only the acceptor needs this:
/// it configures the relay it binds to, which is then baked into the ticket.
/// **Dialers ([`connect`], the tauri dial plugin) take no relay config** —
/// they adopt the relay carried by the ticket they dial (see [`connect`]).
#[derive(Clone, Default, Debug)]
pub struct IrohConfig {
    /// Custom relay servers. Empty ⇒ the n0 default relays. A self-hosted
    /// relay is what makes peerline reachable where the n0 public relays are
    /// distant or unreliable and direct connectivity is impossible, e.g.
    /// `http://relay.example:3340`. The acceptor's home relay becomes the
    /// first reachable one, and the ticket advertises it so dialers adopt it.
    pub relays: Vec<RelayUrl>,
}

impl IrohConfig {
    /// Build from relay URL strings (e.g. config-file values), returning an
    /// error that names every entry which isn't a valid relay URL. Empty and
    /// whitespace-only entries are skipped.
    pub fn from_relay_urls<I, S>(urls: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut relays = Vec::new();
        let mut bad = Vec::new();
        for u in urls {
            let s = u.as_ref().trim();
            if s.is_empty() {
                continue;
            }
            match s.parse::<RelayUrl>() {
                Ok(url) => relays.push(url),
                Err(_) => bad.push(s.to_string()),
            }
        }
        if bad.is_empty() {
            Ok(Self { relays })
        } else {
            Err(format!("invalid iroh relay url(s): {}", bad.join(", ")))
        }
    }

    /// The custom [`RelayMode`] for these settings, or `None` to keep the n0
    /// default when no relays are set. Each relay has QUIC address discovery
    /// **disabled** — a plaintext-HTTP relay can't offer it (QAD needs TLS),
    /// and requesting it would only make clients probe a port that isn't
    /// there; relay packet forwarding, the path that matters when peers can't
    /// connect directly, is unaffected.
    ///
    /// Exposed so a caller holding its own [`Endpoint`] builder (e.g. the
    /// tauri dial plugin) applies the same relays as [`serve`]/[`connect`]:
    /// `if let Some(m) = cfg.relay_mode() { builder = builder.relay_mode(m); }`.
    pub fn relay_mode(&self) -> Option<RelayMode> {
        if self.relays.is_empty() {
            return None;
        }
        let map = RelayMap::empty();
        for url in &self.relays {
            map.insert(url.clone(), Arc::new(RelayConfig::new(url.clone(), None)));
        }
        Some(RelayMode::Custom(map))
    }
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
    config: IrohConfig,
    on_ticket: T,
    on_peer: F,
) -> Result<(), String>
where
    T: FnOnce(&str),
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    serve_mounted(secret_key, config, on_ticket, vec![(alpn.to_vec(), Arc::new(on_peer))]).await
}

/// Bind ONE endpoint that accepts several ALPNs and run the accept loop
/// forever, **mounting each service by ALPN**: every `(alpn, handler)`
/// routes connections negotiated on that ALPN onto their own peer. All
/// mounts share the one endpoint identity, so `on_ticket` is called once
/// with the single pasteable ticket — a dialer picks a service by passing
/// its ALPN to [`connect`]. Services never share a peer, so their ops
/// can't collide.
pub async fn serve_mounted<T>(
    secret_key: SecretKey,
    config: IrohConfig,
    on_ticket: T,
    mounts: Vec<(Vec<u8>, PeerHandler)>,
) -> Result<(), String>
where
    T: FnOnce(&str),
{
    let alpns: Vec<Vec<u8>> = mounts.iter().map(|(alpn, _)| alpn.clone()).collect();
    let mut builder = Endpoint::builder(presets::N0).alpns(alpns).secret_key(secret_key);
    if let Some(relay_mode) = config.relay_mode() {
        info!(relays = config.relays.len(), "peerline-iroh: using custom relay(s)");
        builder = builder.relay_mode(relay_mode);
    }
    let endpoint = builder
        .bind()
        .await
        .map_err(|e| format!("iroh bind: {e}"))?;

    // Wait (bounded) for a RELAY address before publishing the ticket. A
    // relay (`TransportAddr::Relay`) is the only path an off-LAN peer can
    // bootstrap, and it appears only once the home-relay handshake
    // completes — hundreds of ms to seconds after bind. `endpoint.addr()`
    // mixes relay and direct (`TransportAddr::Ip`) entries, and the direct
    // LAN/loopback ones are discovered near-instantly, so "addrs non-empty"
    // would publish a silently LAN-only ticket before the relay exists.
    // We also wait for a direct address once the relay is up (a same-LAN
    // peer can then skip the relay), but only the relay is required.
    //
    // If no relay materializes within the cap the endpoint is not remotely
    // reachable, so we FAIL rather than publish a degraded ticket — a
    // peerline-iroh ticket that can't be dialed off-LAN is worse than an
    // explicit error. `online()` resolves once a home relay connects.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), endpoint.online()).await;
    for _ in 0..60 {
        let addr = endpoint.addr();
        let has_relay = addr.addrs.iter().any(|a| a.is_relay());
        let has_direct = addr.addrs.iter().any(|a| a.is_ip());
        if has_relay && has_direct {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let addr = endpoint.addr();
    if !addr.addrs.iter().any(|a| a.is_relay()) {
        return Err("iroh: endpoint acquired no relay address — not remotely reachable".to_string());
    }
    let ticket = encode_ticket(&addr)?;
    info!(endpoint_id = %endpoint.id(), mounts = mounts.len(), "peerline-iroh endpoint listening");
    on_ticket(&ticket);

    let mounts = Arc::new(mounts);
    while let Some(incoming) = endpoint.accept().await {
        let mounts = mounts.clone();
        tokio::spawn(async move {
            let conn = match incoming.await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!(error = %e, "peerline-iroh: incoming connection failed");
                    return;
                }
            };
            // Route to the service whose ALPN the client negotiated.
            let alpn = conn.alpn();
            match mounts.iter().find(|(a, _)| a == alpn) {
                Some((_, handler)) => serve_conn(conn, handler.clone()).await,
                None => warn!(alpn = ?alpn, "peerline-iroh: no mount for negotiated ALPN"),
            }
        });
    }
    Ok(())
}

/// Dial the peerline endpoint named by `ticket` for `alpn` (the dial side
/// of [`serve`]) and return one `(Peer, driver)`. Binds a fresh ephemeral
/// dialing endpoint, connects, opens ONE bi-stream over the shared text
/// framing, and hands back a peer for the caller to configure. Drive
/// `driver` to run the session; it resolves when the stream closes.
///
/// The returned driver owns the endpoint and connection, keeping the QUIC
/// link alive for the peer's lifetime; dropping it closes the link. The
/// `alpn` must match what the accepting [`serve`] was bound with.
///
/// For repeated dials, prefer holding a shared [`Endpoint`] and using it
/// directly — this helper binds a new endpoint per call, which suits a
/// one-shot client.
///
/// **Relay:** the dialer takes no relay config — it adopts the relay(s) the
/// ticket carries. The peer's home relay (baked into the ticket by the
/// acceptor's [`IrohConfig`]) is both where we reach the peer and where the
/// peer reaches us back, so we bind our own endpoint to that same relay. A
/// ticket with no relay ⇒ the n0 defaults.
pub async fn connect(
    ticket: &str,
    alpn: &[u8],
) -> Result<(Peer, BoxFuture<'static, ()>), String> {
    let addr = decode_ticket(ticket)?;
    let relays: Vec<RelayUrl> = addr.relay_urls().cloned().collect();
    let mut builder = Endpoint::builder(presets::N0);
    if let Some(relay_mode) = (IrohConfig { relays }).relay_mode() {
        builder = builder.relay_mode(relay_mode);
    }
    let endpoint = builder
        .bind()
        .await
        .map_err(|e| format!("iroh bind: {e}"))?;
    let conn = endpoint
        .connect(addr, alpn)
        .await
        .map_err(|e| format!("iroh connect: {e}"))?;
    // `accept_bi` on the far side only resolves once we send data;
    // peerline's first client frame does that, so no priming write.
    let (send, recv) = conn.open_bi().await.map_err(|e| format!("iroh open_bi: {e}"))?;
    info!(remote = %conn.remote_id(), "peerline-iroh dialed");

    let (sink, stream) = text_frames(tokio::io::join(recv, send));
    let (peer, driver) = Peer::new(sink, stream);
    let driver = async move {
        // Hold the endpoint + connection for the session's lifetime;
        // dropping either would tear the QUIC link down early.
        let _endpoint = endpoint;
        let _conn = conn;
        driver.await;
    };
    Ok((peer, Box::pin(driver)))
}

/// Drive one accepted connection: each bi-stream the client opens becomes
/// its own peer session (looping keeps the connection alive across a
/// reconnect).
async fn serve_conn(conn: Connection, handler: PeerHandler)
{
    let remote = conn.remote_id();
    info!(%remote, "peerline-iroh connection opened");
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let handler = handler.clone();
                tokio::spawn(async move { drive_stream(send, recv, handler).await });
            }
            Err(e) => {
                info!(%remote, error = %e, "peerline-iroh connection closed");
                break;
            }
        }
    }
}

/// Bridge one bi-stream to a peerline [`Peer`] via the shared text-frame
/// codec, run the handler set, then drive until it ends.
async fn drive_stream(send: SendStream, recv: RecvStream, handler: PeerHandler) {
    let (sink, stream) = text_frames(tokio::io::join(recv, send));
    let (peer, driver) = Peer::new(sink, stream);
    handler(&peer);
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

    // `connect` decodes the ticket before touching the network, so a
    // foreign ticket fails fast without binding an endpoint — the
    // dial-side counterpart to `foreign_prefix_is_rejected`.
    #[tokio::test]
    async fn connect_rejects_foreign_ticket() {
        // `Ok` carries a boxed driver (not `Debug`), so match rather than
        // `unwrap_err`.
        let err = match connect("ws://127.0.0.1:6465", b"peerline/test/1").await {
            Ok(_) => panic!("foreign ticket should not connect"),
            Err(e) => e,
        };
        assert!(err.contains(TICKET_PREFIX), "unexpected error: {err}");
    }
}
