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
    /// UDP port the acceptor binds on all interfaces. `None` (or `Some(0)`) ⇒
    /// an ephemeral port, which the OS re-picks on every restart — so a
    /// ticket's **direct** addresses go stale across restarts (the relay and
    /// endpoint identity stay valid regardless). Set a fixed port to keep the
    /// direct addresses valid across restarts (and to make port-forwarding /
    /// firewall rules stable). Only the acceptor needs this — dialers always
    /// use an ephemeral local port.
    pub bind_port: Option<u16>,
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
            Ok(Self { relays, bind_port: None })
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
    // Bind a fixed UDP port (all interfaces) if configured, so the ticket's
    // direct addresses survive restarts. Replace iroh's default ephemeral
    // v4/v6 sockets rather than adding to them. `0` binds ephemeral, matching
    // the `None` default.
    if let Some(port) = config.bind_port {
        use std::net::{Ipv4Addr, Ipv6Addr};
        builder = builder
            .clear_ip_transports()
            .bind_addr((Ipv4Addr::UNSPECIFIED, port))
            .map_err(|e| format!("iroh bind v4 :{port}: {e}"))?
            .bind_addr((Ipv6Addr::UNSPECIFIED, port))
            .map_err(|e| format!("iroh bind v6 :{port}: {e}"))?;
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

// --- Dial modes -------------------------------------------------------------
//
// The path-pinning semantics live HERE, next to the ticket codec, so the two
// dial sides (this crate's [`connect`] and the tauri plugin) cannot drift:
// what "relay mode" means is defined once.

/// Which path(s) a dial may use — the caller's explicit choice.
///
/// `Auto` dials the ticket's full address (direct preferred, relay as
/// backup). `Relay` pins the dial through the ticket's relay(s) on an
/// endpoint with **no IP transports at all**, so holepunching can never
/// upgrade it off the relay. `Direct` pins the dial to the ticket's direct
/// addresses with the relay disabled.
///
/// The pins exist because auto's mixed dial can anchor the QUIC handshake to
/// a **one-way** direct path (a NAT/VPN asymmetry: the peer hears us, we
/// never hear it) and time out even though the relay is healthy — pinning
/// relay makes the dial deterministic from anywhere the relay is reachable.
///
/// Serde uses the lowercase wire strings (`"auto" | "relay" | "direct"`), so
/// a JS-facing command can take the enum directly and a typo'd mode fails
/// deserialization loudly instead of silently behaving like `Auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DialMode {
    #[default]
    Auto,
    Relay,
    Direct,
}

/// The peer address a dial in `mode` offers iroh: the full ticket address
/// for `Auto`, only the relay(s) for `Relay`, only the direct addresses for
/// `Direct`. Errors when the ticket cannot support the pin — better a loud
/// refusal than a dial that can only time out.
pub fn filtered_addr(addr: &EndpointAddr, mode: DialMode) -> Result<EndpointAddr, String> {
    match mode {
        DialMode::Auto => Ok(addr.clone()),
        DialMode::Relay => {
            let relays: Vec<RelayUrl> = addr.relay_urls().cloned().collect();
            if relays.is_empty() {
                return Err("relay mode: ticket carries no relay".into());
            }
            Ok(relays
                .iter()
                .fold(EndpointAddr::from(addr.id), |a, url| a.with_relay_url(url.clone())))
        }
        DialMode::Direct => {
            let ips: Vec<_> = addr.ip_addrs().copied().collect();
            if ips.is_empty() {
                return Err("direct mode: ticket carries no direct addresses".into());
            }
            Ok(ips.iter().fold(EndpointAddr::from(addr.id), |a, ip| a.with_ip_addr(*ip)))
        }
    }
}

/// Bind the endpoint a dial in `mode` uses. `relays` is the ticket's relay
/// set (ignored for `Direct`).
///
/// - `Auto`: an ephemeral `presets::N0` endpoint bound to the ticket's
///   relays — suits one-shot dialers; an app holding a long-lived shared
///   endpoint uses that for auto instead and never calls this arm.
/// - `Relay`: a discovery-free `presets::Minimal` endpoint with **every IP
///   transport removed** — no UDP sockets, so no path but the relay can
///   ever exist on it.
/// - `Direct`: a discovery-free `presets::Minimal` endpoint with the relay
///   disabled.
pub async fn dial_endpoint(relays: &[RelayUrl], mode: DialMode) -> Result<Endpoint, String> {
    let bound = match mode {
        DialMode::Auto => {
            let mut builder = Endpoint::builder(presets::N0);
            if let Some(relay_mode) = (IrohConfig { relays: relays.to_vec(), ..Default::default() }).relay_mode() {
                builder = builder.relay_mode(relay_mode);
            }
            builder.bind().await
        }
        DialMode::Relay => {
            // Minimal sets no relay transport of its own, so Relay with no
            // relays (plus the cleared IP transports) would bind an endpoint
            // with ZERO transports — "successfully" un-dialable. dial_plan's
            // filtered_addr already refuses this; guard the public fn too.
            if relays.is_empty() {
                return Err("relay mode: no relay to dial through".into());
            }
            let mut builder = Endpoint::builder(presets::Minimal).clear_ip_transports();
            if let Some(relay_mode) = (IrohConfig { relays: relays.to_vec(), ..Default::default() }).relay_mode() {
                builder = builder.relay_mode(relay_mode);
            }
            builder.bind().await
        }
        DialMode::Direct => {
            Endpoint::builder(presets::Minimal)
                .relay_mode(RelayMode::Disabled)
                .bind()
                .await
        }
    };
    bound.map_err(|e| format!("iroh bind ({mode:?}): {e}"))
}

/// One-stop dial preparation: the `(endpoint, filtered peer address)` pair
/// for dialing `addr` in `mode` — [`dial_endpoint`] + [`filtered_addr`],
/// with the relay set taken from the address itself.
pub async fn dial_plan(addr: &EndpointAddr, mode: DialMode) -> Result<(Endpoint, EndpointAddr), String> {
    let peer = filtered_addr(addr, mode)?;
    let relays: Vec<RelayUrl> = addr.relay_urls().cloned().collect();
    let endpoint = dial_endpoint(&relays, mode).await?;
    Ok((endpoint, peer))
}

/// Dial the peerline endpoint named by `ticket` for `alpn` (the dial side
/// of [`serve`]) and return one `(Peer, driver)`. Binds a fresh dialing
/// endpoint shaped by `mode` (see [`DialMode`]), connects over the filtered
/// peer address, opens ONE bi-stream over the shared text
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
/// peer reaches us back, so we bind our own endpoint to that same relay. In
/// `Auto`, a ticket with no relay falls back to the n0 defaults; the pinned
/// modes instead refuse a ticket that can't support the pin.
pub async fn connect(
    ticket: &str,
    alpn: &[u8],
    mode: DialMode,
) -> Result<(Peer, BoxFuture<'static, ()>), String> {
    let addr = decode_ticket(ticket)?;
    let (endpoint, peer_addr) = dial_plan(&addr, mode).await?;
    let conn = endpoint
        .connect(peer_addr, alpn)
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

    #[test]
    fn dial_mode_wire_strings_round_trip_and_typos_fail() {
        for (wire, mode) in [("auto", DialMode::Auto), ("relay", DialMode::Relay), ("direct", DialMode::Direct)] {
            let parsed: DialMode = serde_json::from_str(&format!("\"{wire}\"")).expect(wire);
            assert_eq!(parsed, mode);
            assert_eq!(serde_json::to_string(&mode).expect(wire), format!("\"{wire}\""));
        }
        // A typo'd mode must fail loudly, never silently behave like Auto.
        assert!(serde_json::from_str::<DialMode>("\"relayy\"").is_err());
    }

    #[test]
    fn filtered_addr_pins_and_validates() {
        let id = SecretKey::generate().public();
        let relay: RelayUrl = "http://relay.example:3340".parse().expect("relay url");
        let ip: std::net::SocketAddr = "192.168.1.10:6466".parse().expect("ip");
        let full = EndpointAddr::from(id).with_relay_url(relay.clone()).with_ip_addr(ip);

        // Auto keeps everything.
        let auto = filtered_addr(&full, DialMode::Auto).expect("auto");
        assert_eq!(auto, full);
        // Relay drops the direct addrs; Direct drops the relay.
        let pinned_relay = filtered_addr(&full, DialMode::Relay).expect("relay");
        assert_eq!(pinned_relay.relay_urls().count(), 1);
        assert_eq!(pinned_relay.ip_addrs().count(), 0);
        let pinned_direct = filtered_addr(&full, DialMode::Direct).expect("direct");
        assert_eq!(pinned_direct.relay_urls().count(), 0);
        assert_eq!(pinned_direct.ip_addrs().count(), 1);

        // A ticket that can't support the pin refuses loudly.
        let relay_only = EndpointAddr::from(id).with_relay_url(relay);
        assert!(filtered_addr(&relay_only, DialMode::Direct).is_err());
        let direct_only = EndpointAddr::from(id).with_ip_addr(ip);
        assert!(filtered_addr(&direct_only, DialMode::Relay).is_err());
    }

    // `connect` decodes the ticket before touching the network, so a
    // foreign ticket fails fast without binding an endpoint — the
    // dial-side counterpart to `foreign_prefix_is_rejected`.
    #[tokio::test]
    async fn connect_rejects_foreign_ticket() {
        // `Ok` carries a boxed driver (not `Debug`), so match rather than
        // `unwrap_err`.
        let err = match connect("ws://127.0.0.1:6465", b"peerline/test/1", DialMode::Auto).await {
            Ok(_) => panic!("foreign ticket should not connect"),
            Err(e) => e,
        };
        assert!(err.contains(TICKET_PREFIX), "unexpected error: {err}");
    }
}
