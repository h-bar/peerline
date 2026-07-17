//! Tauri plugin: the **dial side** of a peerline-over-Iroh P2P transport.
//!
//! Pairs with any service's iroh acceptor and shares the wire contract
//! (ticket codec, text framing) via the [`peerline_transport_iroh`]
//! crate, so the two ends cannot drift. Service-neutral: the **ALPN is
//! supplied per
//! `connect`**, so a single app can dial several peerline services on one
//! host (one ALPN each) through this one plugin.
//!
//! It exposes three commands a JS transport wrapper drives:
//!
//! - `connect(ticket, alpn, on_frame)` → dials the peer for `alpn`, opens
//!   ONE QUIC bi-stream, spawns a pump (inbound frames → `on_frame`
//!   [`Channel`], outbound frames ← an mpsc), returns a numeric `id`.
//!   Resolves only after the stream is open, so a dial failure rejects on
//!   the JS side.
//! - `send(id, frame)` → enqueues one text frame.
//! - `close(id)` → tears the connection down (idempotent).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use iroh::endpoint::presets;
use iroh::{Endpoint, RelayMode};
use peerline_transport_iroh::{decode_ticket, dial_plan, text_frames, DialMode, EndpointAddr, IrohConfig, RelayUrl};
use tauri::ipc::Channel;
use tauri::plugin::{Builder, TauriPlugin};
use tauri::{command, AppHandle, Manager, Runtime};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};

/// First ~100 chars of a frame for a log preview (char-safe, no panic on a
/// multi-byte boundary). Frames are JSON-RPC text.
fn preview(s: &str) -> String {
    s.chars().take(100).collect()
}

/// One inbound message delivered to JS over the connection's channel:
/// a decoded text frame, or the single terminal `close` (graceful
/// close, QUIC reset, or silent EOF all collapse to `close`).
#[derive(Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Inbound {
    Frame { frame: String },
    Close,
}

/// Result of `connect` — the handle `send`/`close` address.
#[derive(Clone, serde::Serialize)]
struct ConnectResult {
    id: u64,
}

/// The decoded parts of a peerline ticket, for on-device inspection — e.g.
/// confirming a ticket actually carries the expected relay when there's no
/// console to read.
#[derive(Clone, serde::Serialize)]
struct DecodedTicket {
    /// The endpoint's identity (its public key / `EndpointId`), hex.
    endpoint_id: String,
    /// Relay URL(s) the ticket advertises — the relay(s) a dialer adopts.
    relays: Vec<String>,
    /// Direct IP address hints carried by the ticket (LAN/loopback/public).
    direct_addrs: Vec<String>,
}

/// One QUIC path of a live connection, with its per-path stats.
#[derive(Clone, serde::Serialize)]
struct PathInfo {
    /// `"relay"` (via a relay server) or `"direct"` (a UDP path), or `"other"`.
    kind: String,
    /// The path's remote address — the **relay URL** for a relay path, or the
    /// peer's **ip:port** for a direct path.
    remote_addr: String,
    /// Our local side of the path (best-effort: local IP for a direct path,
    /// the relay URL for a relay path).
    local_addr: String,
    /// True if this path is the one currently carrying application data.
    selected: bool,
    /// Round-trip time estimate for this path, in ms.
    rtt_ms: u64,
    bytes_sent: u64,
    bytes_recv: u64,
    datagrams_sent: u64,
    datagrams_recv: u64,
    /// Congestion window (bytes) and cumulative congestion events.
    cwnd: u64,
    congestion_events: u64,
}

/// A detailed snapshot of a live iroh connection — the "connection status" the
/// UI shows: whether traffic is going over the relay (remote) or a direct path
/// (local), the concrete relay URL / ip:port in use, latency, per-path stats,
/// and connection totals.
#[derive(Clone, serde::Serialize)]
struct ConnectionStatus {
    /// The `connect` handle id this status is for.
    id: u64,
    /// The peer's identity (its public key / endpoint id), hex.
    remote_id: String,
    /// The negotiated ALPN (service id), if any.
    alpn: Option<String>,
    /// `"client"` (we dialed) — always, for this plugin.
    side: String,
    /// Headline: `"local"` if the selected path is direct, `"remote"` if it's
    /// the relay, or `"connecting"` if no path is selected yet.
    connection_type: String,
    /// The selected path's remote address — the relay URL, or the peer ip:port.
    active_remote: Option<String>,
    /// Round-trip time of the selected path, in ms.
    rtt_ms: Option<u64>,
    /// Max QUIC datagram size for this connection, if known.
    max_datagram_size: Option<usize>,
    /// Every open path (relay + any direct), each with its own stats.
    paths: Vec<PathInfo>,
    /// Connection-wide byte / datagram totals and loss counters.
    bytes_sent: u64,
    bytes_recv: u64,
    datagrams_sent: u64,
    datagrams_recv: u64,
    lost_packets: u64,
    lost_bytes: u64,
    /// Set once the connection has closed (the close reason).
    closed: Option<String>,
}

/// One target's reachability result — whether the peer is reachable over that
/// single relay (or single direct address), and how long reaching it took.
#[derive(Clone, serde::Serialize)]
struct PathProbe {
    /// The single relay URL or IP address this result is for.
    target: String,
    /// Did this target reach the peer within the timeout? True on a completed
    /// handshake AND on a peer-side refusal (the probe uses a neutral ALPN the
    /// service rejects — reaching that rejection still proves the path
    /// delivered). False only when it never got through (timeout / no route).
    reachable: bool,
    /// Wall-clock ms to reach the peer — the target's latency signal (relay
    /// hop shows higher; LAN direct shows low). `None` when unreachable.
    latency_ms: Option<u64>,
    /// Human-readable outcome: `connected`, the peer's refusal, or the error.
    detail: String,
}

/// [`inspect_ticket`] plus a live reachability + latency test of the peer over
/// **each relay and each direct address individually**, so the frontend sees
/// exactly which targets work and how fast, from one call.
#[derive(Clone, serde::Serialize)]
struct TicketProbe {
    endpoint_id: String,
    /// One result per relay URL in the ticket (empty if it carries none).
    relays: Vec<PathProbe>,
    /// One result per direct address in the ticket (empty if it carries none).
    direct_addrs: Vec<PathProbe>,
}

/// A live connection's control surface: outbound frames go through
/// `tx`; `abort` tears the pump task (and thus the QUIC connection)
/// down on `close`.
struct Conn {
    tx: mpsc::UnboundedSender<String>,
    abort: tokio::task::AbortHandle,
    /// A clone of the live QUIC connection, so `connection_status` can read
    /// its current paths / RTT / stats. `Connection` is cheap to clone (an
    /// `Arc` inside) and dropping this clone with the entry doesn't close the
    /// link while the pump still holds its own.
    conn: iroh::endpoint::Connection,
    /// For a forced `relay`/`direct` connection: the dedicated endpoint it was
    /// dialed on (kept alive for the connection's lifetime, closed gracefully
    /// when the connection ends). `None` for `auto`, which uses the shared
    /// endpoint held in [`IrohState::endpoint`].
    endpoint: Option<Endpoint>,
}

/// Plugin state: one shared dialing endpoint (built lazily) plus the
/// live connection table. The endpoint is ALPN-agnostic — the ALPN is
/// chosen per `connect`, so one endpoint serves every service.
#[derive(Default)]
struct IrohState {
    /// The one shared dialing endpoint plus the relay set it was bound with.
    /// The dialer's relay is whatever the dialed ticket carries, so a ticket
    /// that introduces a new relay triggers a rebind (with the union) — the
    /// dialer never needs separate relay config.
    endpoint: AsyncMutex<Option<(Endpoint, Vec<RelayUrl>)>>,
    conns: Mutex<HashMap<u64, Conn>>,
    next_id: AtomicU64,
}

impl IrohState {
    /// Lock the connection table, recovering the guard if the mutex was
    /// poisoned. Every holder mutates the map with non-panicking ops, so
    /// a poisoned guard still wraps consistent data — and this keeps the
    /// plugin free of `unwrap()` per the project's no-panic rule.
    fn conns(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Conn>> {
        self.conns.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The one shared client endpoint, bound on first use with the relay(s)
    /// the dialed ticket carries. `Endpoint` is cheap to clone (an `Arc`
    /// inside). No ALPNs are configured on a dialing endpoint — each `connect`
    /// passes its own. If a later ticket carries a relay the endpoint isn't
    /// already bound with, it rebinds with the union so both peers' home
    /// relays remain reachable; existing connections keep their own links.
    async fn endpoint_for(&self, mut want: Vec<RelayUrl>) -> Result<Endpoint, String> {
        let mut guard = self.endpoint.lock().await;
        if let Some((e, bound)) = guard.as_ref() {
            if want.iter().all(|r| bound.contains(r)) {
                debug!(endpoint_id = %e.id(), "peerline-iroh endpoint: reusing bound endpoint");
                return Ok(e.clone());
            }
            // A new relay appeared — rebind with the union of old + new.
            warn!(bound = ?bound.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                  want = ?want.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                  "peerline-iroh endpoint: REBINDING (new relay) — this drops the shared endpoint");
            for r in bound {
                if !want.contains(r) {
                    want.push(r.clone());
                }
            }
        }
        let relay_list: Vec<String> = want.iter().map(|r| r.to_string()).collect();
        let mut builder = Endpoint::builder(presets::N0);
        if let Some(relay_mode) = (IrohConfig { relays: want.clone(), ..Default::default() }).relay_mode() {
            builder = builder.relay_mode(relay_mode);
        }
        let e = builder
            .bind()
            .await
            .map_err(|e| {
                warn!(error = %e, "peerline-iroh endpoint: bind failed");
                format!("iroh bind: {e}")
            })?;
        info!(endpoint_id = %e.id(), relays = ?relay_list, "peerline-iroh endpoint: bound");
        *guard = Some((e.clone(), want));
        Ok(e)
    }
}

#[command]
async fn connect<R: Runtime>(
    app: AppHandle<R>,
    ticket: String,
    alpn: String,
    on_frame: Channel<Inbound>,
    // Path mode — deserialized straight into the shared [`DialMode`]
    // (`"auto" | "relay" | "direct"` on the wire; anything else fails serde,
    // loudly). `auto` (default) uses the shared endpoint and lets iroh pick;
    // the pins dial a dedicated endpoint + filtered address via the shared
    // crate's `dial_plan`, so the semantics can't drift from the native dial
    // side.
    mode: Option<DialMode>,
) -> Result<ConnectResult, String> {
    let state = app.state::<IrohState>();
    let t0 = Instant::now();
    let addr = decode_ticket(&ticket)?;
    // The dialer's relay is the ticket's relay: the peer's home relay is both
    // where we reach it and where it reaches us back, so we bind our endpoint
    // to the same relay(s) — no separate dial-side relay config.
    let relays: Vec<RelayUrl> = addr.relay_urls().cloned().collect();
    let directs: Vec<String> = addr.ip_addrs().map(|a| a.to_string()).collect();
    let mode = mode.unwrap_or_default();
    info!(
        endpoint_id = %addr.id, %alpn, mode = ?mode,
        relays = ?relays.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
        directs = ?directs,
        "peerline-iroh connect: dialing"
    );

    // Pick the endpoint + the peer address to dial per mode. A pinned mode
    // rides the shared crate's `dial_plan` — dedicated no-discovery endpoint
    // + filtered address, so the path can't silently fall back (or get
    // re-widened by discovery). Auto keeps this plugin's long-lived shared
    // endpoint rather than dial_plan's one-shot auto endpoint.
    let (endpoint, peer_addr, dedicated): (Endpoint, EndpointAddr, Option<Endpoint>) = match mode {
        DialMode::Auto => {
            let ep = state.endpoint_for(relays).await?;
            (ep, addr, None)
        }
        DialMode::Relay | DialMode::Direct => {
            let (ep, peer) = dial_plan(&addr, mode).await?;
            info!(endpoint_id = %ep.id(), ?mode, "peerline-iroh: bound dedicated endpoint for pinned mode");
            (ep.clone(), peer, Some(ep))
        }
    };

    let conn = endpoint
        .connect(peer_addr, alpn.as_bytes())
        .await
        .map_err(|e| {
            warn!(error = %e, ?mode, elapsed_ms = t0.elapsed().as_millis() as u64,
                  "peerline-iroh connect: iroh connect FAILED");
            format!("iroh connect: {e}")
        })?;
    let n_relay = conn.paths().iter().filter(|p| p.is_relay()).count();
    let n_direct = conn.paths().iter().filter(|p| p.is_ip()).count();
    let selected = conn
        .paths()
        .iter()
        .find(|p| p.is_selected())
        .map(|p| if p.is_relay() { "relay" } else { "direct" })
        .unwrap_or("none");
    info!(
        remote = %conn.remote_id(), elapsed_ms = t0.elapsed().as_millis() as u64,
        relay_paths = n_relay, direct_paths = n_direct, selected_path = selected,
        "peerline-iroh connect: QUIC connection established"
    );
    // Open the single peer-link bi-stream. The acceptor's `accept_bi`
    // only resolves once we send data — peerline's first client frame
    // does that, so no priming write is needed here.
    let (send, recv) = conn.open_bi().await.map_err(|e| {
        warn!(error = %e, "peerline-iroh connect: open_bi FAILED");
        format!("iroh open_bi: {e}")
    })?;

    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    // The pump owns `conn` so dropping the task (via `abort`) closes the
    // QUIC connection. It also removes its own `conns` entry when the wire
    // dies on its own, so a peer-initiated close doesn't leak the entry.
    //
    // Hold the table lock across spawn + insert: the pump's self-cleanup
    // `remove(id)` also takes this lock, so a wire that dies instantly
    // can't remove the entry before we insert it (which would leak a live
    // handle). No await runs while the lock is held.
    let mut conns = state.conns();
    let conn_for_status = conn.clone();
    let handle = tokio::spawn(pump(app.clone(), id, conn, send, recv, on_frame, rx));
    conns.insert(
        id,
        Conn { tx, abort: handle.abort_handle(), conn: conn_for_status, endpoint: dedicated },
    );
    drop(conns);
    info!(%id, ?mode, elapsed_ms = t0.elapsed().as_millis() as u64,
          "peerline-iroh connect: READY (bi-stream open, pump running) — awaiting first frame from app");
    Ok(ConnectResult { id })
}

#[command]
fn send<R: Runtime>(app: AppHandle<R>, id: u64, frame: String) -> Result<(), String> {
    let state = app.state::<IrohState>();
    let len = frame.len();
    let conns = state.conns();
    let Some(conn) = conns.get(&id) else {
        warn!(%id, "peerline-iroh send: unknown connection (already closed?)");
        return Err("unknown iroh connection".to_string());
    };
    match conn.tx.send(frame) {
        Ok(()) => {
            debug!(%id, len, "peerline-iroh send: frame enqueued (app -> wire)");
            Ok(())
        }
        Err(_) => {
            warn!(%id, len, "peerline-iroh send: connection is closed — frame dropped");
            Err("iroh connection is closed".to_string())
        }
    }
}

#[command]
fn close<R: Runtime>(app: AppHandle<R>, id: u64) {
    match app.state::<IrohState>().conns().remove(&id) {
        Some(conn) => {
            info!(%id, "peerline-iroh close: command received — aborting connection");
            conn.abort.abort();
            // A forced-mode connection owns its endpoint: close it gracefully
            // (relay teardown, pending frames) instead of dropping it cold.
            if let Some(endpoint) = conn.endpoint {
                tauri::async_runtime::spawn(async move { endpoint.close().await });
            }
        }
        None => debug!(%id, "peerline-iroh close: already gone"),
    }
}

/// Detailed live status of connection `id`: whether it's on the relay (remote)
/// or a direct path (local), the concrete relay URL / peer ip:port in use,
/// latency, per-path stats, and connection totals. Read on demand (or polled)
/// by the UI. Errors if `id` isn't a live connection.
#[command]
fn connection_status<R: Runtime>(app: AppHandle<R>, id: u64) -> Result<ConnectionStatus, String> {
    let state = app.state::<IrohState>();
    let conns = state.conns();
    let conn = &conns.get(&id).ok_or("unknown iroh connection")?.conn;

    let mut paths = Vec::new();
    let mut connection_type = "connecting".to_string();
    let mut active_remote = None;
    let mut rtt_ms = None;
    for p in conn.paths().iter() {
        let is_relay = p.is_relay();
        let kind = if is_relay {
            "relay"
        } else if p.is_ip() {
            "direct"
        } else {
            "other"
        };
        // `TransportAddr` Display is `relay:<url>` / `ip:<addr:port>`; strip the
        // kind prefix (only the first colon) to leave the bare URL / ip:port.
        let raw = p.remote_addr().to_string();
        let remote_addr = raw.split_once(':').map(|(_, r)| r.to_string()).unwrap_or(raw);
        let st = p.stats();
        let path_rtt = st.rtt.as_millis() as u64;
        if p.is_selected() {
            connection_type = if is_relay { "remote" } else { "local" }.to_string();
            active_remote = Some(remote_addr.clone());
            rtt_ms = Some(path_rtt);
        }
        paths.push(PathInfo {
            kind: kind.to_string(),
            remote_addr,
            local_addr: format!("{:?}", p.local_addr()),
            selected: p.is_selected(),
            rtt_ms: path_rtt,
            bytes_sent: st.udp_tx.bytes,
            bytes_recv: st.udp_rx.bytes,
            datagrams_sent: st.udp_tx.datagrams,
            datagrams_recv: st.udp_rx.datagrams,
            cwnd: st.cwnd,
            congestion_events: st.congestion_events,
        });
    }

    let s = conn.stats();
    let alpn = match conn.alpn() {
        a if a.is_empty() => None,
        a => Some(String::from_utf8_lossy(a).into_owned()),
    };
    Ok(ConnectionStatus {
        id,
        remote_id: conn.remote_id().to_string(),
        alpn,
        side: if conn.side().is_client() { "client" } else { "server" }.to_string(),
        connection_type,
        active_remote,
        rtt_ms,
        max_datagram_size: conn.max_datagram_size(),
        paths,
        bytes_sent: s.udp_tx.bytes,
        bytes_recv: s.udp_rx.bytes,
        datagrams_sent: s.udp_tx.datagrams,
        datagrams_recv: s.udp_rx.datagrams,
        lost_packets: s.lost_packets,
        lost_bytes: s.lost_bytes,
        closed: conn.close_reason().map(|e| e.to_string()),
    })
}

/// Decode a peerline ticket into its parts WITHOUT dialing — endpoint id,
/// relay URL(s), and direct address hints. Purely local (decodes the ticket
/// bytes); a diagnostic for confirming, on a device, what a ticket actually
/// carries (e.g. whether the custom relay made it in).
#[command]
fn inspect_ticket(ticket: String) -> Result<DecodedTicket, String> {
    let addr = decode_ticket(&ticket)?;
    Ok(DecodedTicket {
        endpoint_id: addr.id.to_string(),
        relays: addr.relay_urls().map(|u| u.to_string()).collect(),
        direct_addrs: addr.ip_addrs().map(|a| a.to_string()).collect(),
    })
}

/// A neutral ALPN used only for reachability probing. A real service won't
/// serve it, so the peer refuses the handshake — but reaching that refusal
/// still proves the path delivered to the peer, which is what we're testing.
const PROBE_ALPN: &[u8] = b"peerline/probe/1";

/// Decode a ticket AND test every relay and every direct address the ticket
/// carries **individually** — each over just that one target — reporting
/// reachability + latency per target. Takes only the ticket (probes at the
/// path level, no ALPN needed). All targets are probed concurrently on
/// throwaway endpoints, independent of the live dialing endpoint, so it never
/// disturbs a real session.
#[command]
async fn probe_ticket(ticket: String) -> Result<TicketProbe, String> {
    let addr = decode_ticket(&ticket)?;
    let id = addr.id;

    // One probe per relay: a ticket-addr with just that relay, relay mode set
    // to just that relay.
    let relay_probes = addr.relay_urls().cloned().map(|url| {
        let single = EndpointAddr::from(id).with_relay_url(url.clone());
        let mode = (IrohConfig { relays: vec![url.clone()], ..Default::default() }).relay_mode();
        probe_path(single, mode, Duration::from_secs(12), url.to_string())
    });

    // One probe per direct address: a ticket-addr with just that IP, relay
    // DISABLED so the only path iroh can use is that direct one.
    let direct_probes = addr.ip_addrs().copied().map(|ip| {
        let single = EndpointAddr::from(id).with_ip_addr(ip);
        probe_path(single, Some(RelayMode::Disabled), Duration::from_secs(8), ip.to_string())
    });

    // Run them all concurrently so timeouts don't stack.
    let (relays, direct_addrs) = futures::future::join(
        futures::future::join_all(relay_probes),
        futures::future::join_all(direct_probes),
    )
    .await;

    Ok(TicketProbe { endpoint_id: id.to_string(), relays, direct_addrs })
}

/// Bind a throwaway endpoint (optionally overriding the relay mode), try to
/// reach `addr` over its single `target` with [`PROBE_ALPN`], measure how long
/// it took, then close. Reaching the peer — even a peer-side ALPN refusal —
/// counts as reachable; only a dead path (timeout / no route) is unreachable.
async fn probe_path(
    addr: EndpointAddr,
    relay_mode: Option<RelayMode>,
    timeout: Duration,
    target: String,
) -> PathProbe {
    let start = Instant::now();
    let mut builder = Endpoint::builder(presets::N0);
    if let Some(mode) = relay_mode {
        builder = builder.relay_mode(mode);
    }
    let endpoint = match builder.bind().await {
        Ok(e) => e,
        Err(e) => return PathProbe { target, reachable: false, latency_ms: None, detail: format!("bind: {e}") },
    };
    let outcome = tokio::time::timeout(timeout, endpoint.connect(addr, PROBE_ALPN)).await;
    endpoint.close().await;
    let ms = start.elapsed().as_millis() as u64;
    match outcome {
        // Unlikely (the peer would have to serve the probe ALPN), but a clean
        // reach nonetheless.
        Ok(Ok(_conn)) => {
            PathProbe { target, reachable: true, latency_ms: Some(ms), detail: "connected".into() }
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            let low = msg.to_lowercase();
            // A dead path never delivers; anything else means the peer itself
            // responded (e.g. refused the probe ALPN) — so the path works.
            let dead = ["timed out", "timeout", "no route", "unreachable", "no known", "no working", "no path", "no addr"]
                .iter()
                .any(|k| low.contains(k));
            if dead {
                PathProbe { target, reachable: false, latency_ms: None, detail: msg }
            } else {
                PathProbe { target, reachable: true, latency_ms: Some(ms), detail: format!("reached peer (probe ALPN refused): {msg}") }
            }
        }
        Err(_) => PathProbe { target, reachable: false, latency_ms: None, detail: "timed out".into() },
    }
}

/// Drive one bi-stream: pump inbound frames → `channel` and outbound
/// frames ← `rx` over the shared text-frame codec, emitting exactly one
/// terminal `Close` when either direction ends. `_conn` is held only to
/// keep the QUIC connection alive for the task's lifetime. On exit it
/// removes its own `conns` entry (idempotent with the `close` command,
/// which may have removed it already), so a peer-initiated close can't
/// leak a dead entry.
async fn pump<R: Runtime>(
    app: AppHandle<R>,
    id: u64,
    _conn: iroh::endpoint::Connection,
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
    channel: Channel<Inbound>,
    mut rx: mpsc::UnboundedReceiver<String>,
) {
    info!(%id, "peerline-iroh pump: started (bi-stream framing live)");
    let (mut sink, mut stream) = text_frames(tokio::io::join(recv, send));

    // inbound: wire (peer) -> JS channel (app)
    let reader = async {
        let mut n = 0u64;
        while let Some(item) = stream.next().await {
            match item {
                Ok(frame) => {
                    n += 1;
                    if n == 1 {
                        info!(%id, len = frame.len(), preview = %preview(&frame),
                              "peerline-iroh pump: FIRST inbound frame (peer -> app)");
                    } else {
                        debug!(%id, seq = n, len = frame.len(), preview = %preview(&frame),
                               "peerline-iroh pump: inbound frame (peer -> app)");
                    }
                    if channel.send(Inbound::Frame { frame }).is_err() {
                        warn!(%id, inbound_total = n,
                              "peerline-iroh pump: reader END — JS Channel dropped (app tore down transport)");
                        return ("reader", "js-channel-dropped", n);
                    }
                }
                Err(e) => {
                    warn!(%id, inbound_total = n, error = %e,
                          "peerline-iroh pump: reader END — wire recv error (connection lost)");
                    return ("reader", "wire-recv-error", n);
                }
            }
        }
        info!(%id, inbound_total = n,
              "peerline-iroh pump: reader END — stream closed (peer closed the bi-stream / connection)");
        ("reader", "stream-ended", n)
    };

    // outbound: JS `send` (app) -> wire (peer)
    let writer = async {
        let mut n = 0u64;
        while let Some(frame) = rx.recv().await {
            n += 1;
            if n == 1 {
                info!(%id, len = frame.len(), preview = %preview(&frame),
                      "peerline-iroh pump: FIRST outbound frame (app -> peer)");
            } else {
                debug!(%id, seq = n, len = frame.len(), preview = %preview(&frame),
                       "peerline-iroh pump: outbound frame (app -> peer)");
            }
            if let Err(e) = sink.send(frame).await {
                warn!(%id, outbound_total = n, error = %e,
                      "peerline-iroh pump: writer END — wire send error (connection lost)");
                return ("writer", "wire-send-error", n);
            }
        }
        info!(%id, outbound_total = n,
              "peerline-iroh pump: writer END — outbound channel closed (all `send` handles dropped)");
        ("writer", "outbound-closed", n)
    };

    // Wire health every 5s while frames flow, so a slow transfer explains
    // itself in the log: per-path RTT / congestion window / loss counters
    // distinguish "path is lossy → cwnd collapsed" from "clean but thin pipe".
    // Only emits while bytes moved since the last tick — an idle wire is
    // silent.
    let stats = async {
        // Baseline at ticker start: the handshake already moved bytes, and a
        // wire idle ever since should stay silent.
        let mut last_bytes = {
            let s = _conn.stats();
            s.udp_tx.bytes + s.udp_rx.bytes
        };
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let s = _conn.stats();
            let total = s.udp_tx.bytes + s.udp_rx.bytes;
            if total == last_bytes {
                continue;
            }
            last_bytes = total;
            for p in _conn.paths().iter().filter(|p| p.is_selected()) {
                let ps = p.stats();
                info!(%id, path = %p.remote_addr(), rtt_ms = ps.rtt.as_millis() as u64,
                      cwnd = ps.cwnd, congestion_events = ps.congestion_events,
                      tx_bytes = s.udp_tx.bytes, rx_bytes = s.udp_rx.bytes,
                      lost_packets = s.lost_packets, lost_bytes = s.lost_bytes,
                      "peerline-iroh pump: wire stats");
            }
        }
    };

    let (side, reason, count) = tokio::select! {
        r = reader => r,
        w = writer => w,
        _ = stats => unreachable!("stats ticker never returns"),
    };
    warn!(%id, ended_side = side, reason, frame_count = count,
          "peerline-iroh pump: ENDED — closing connection, signalling app");
    // Best-effort terminal signal; JS collapses it to a single onClose.
    let _ = channel.send(Inbound::Close);
    // Reclaim the table entry (no-op if `close` already removed it). If this
    // task was aborted by `close`, `close` did the removal instead. A
    // forced-mode connection's dedicated endpoint gets a graceful close.
    if let Some(conn) = app.state::<IrohState>().conns().remove(&id) {
        if let Some(endpoint) = conn.endpoint {
            tauri::async_runtime::spawn(async move { endpoint.close().await });
        }
    }
}

/// Initialize the plugin. Register with the Tauri builder via
/// `.plugin(tauri_plugin_peerline_iroh::init())`. No relay configuration is
/// needed — the dialing endpoint adopts the relay carried by each ticket it
/// dials (the acceptor bakes its relay into the ticket).
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("peerline-iroh")
        .invoke_handler(tauri::generate_handler![
            connect,
            send,
            close,
            connection_status,
            inspect_ticket,
            probe_ticket
        ])
        .setup(|app, _api| {
            app.manage(IrohState::default());
            Ok(())
        })
        .build()
}
