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

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use iroh::endpoint::presets;
use iroh::Endpoint;
use peerline_transport_iroh::{decode_ticket, text_frames, IrohConfig, RelayUrl};
use tauri::ipc::Channel;
use tauri::plugin::{Builder, TauriPlugin};
use tauri::{command, AppHandle, Manager, Runtime};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

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

/// A live connection's control surface: outbound frames go through
/// `tx`; `abort` tears the pump task (and thus the QUIC connection)
/// down on `close`.
struct Conn {
    tx: mpsc::UnboundedSender<String>,
    abort: tokio::task::AbortHandle,
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
                return Ok(e.clone());
            }
            // A new relay appeared — rebind with the union of old + new.
            for r in bound {
                if !want.contains(r) {
                    want.push(r.clone());
                }
            }
        }
        let mut builder = Endpoint::builder(presets::N0);
        if let Some(relay_mode) = (IrohConfig { relays: want.clone() }).relay_mode() {
            builder = builder.relay_mode(relay_mode);
        }
        let e = builder
            .bind()
            .await
            .map_err(|e| format!("iroh bind: {e}"))?;
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
) -> Result<ConnectResult, String> {
    let state = app.state::<IrohState>();
    let addr = decode_ticket(&ticket)?;
    // The dialer's relay is the ticket's relay: the peer's home relay is both
    // where we reach it and where it reaches us back, so we bind our endpoint
    // to the same relay(s) — no separate dial-side relay config.
    let relays: Vec<RelayUrl> = addr.relay_urls().cloned().collect();
    let endpoint = state.endpoint_for(relays).await?;
    let conn = endpoint
        .connect(addr, alpn.as_bytes())
        .await
        .map_err(|e| format!("iroh connect: {e}"))?;
    // Open the single peer-link bi-stream. The acceptor's `accept_bi`
    // only resolves once we send data — peerline's first client frame
    // does that, so no priming write is needed here.
    let (send, recv) = conn.open_bi().await.map_err(|e| format!("iroh open_bi: {e}"))?;

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
    let handle = tokio::spawn(pump(app.clone(), id, conn, send, recv, on_frame, rx));
    conns.insert(id, Conn { tx, abort: handle.abort_handle() });
    drop(conns);
    Ok(ConnectResult { id })
}

#[command]
fn send<R: Runtime>(app: AppHandle<R>, id: u64, frame: String) -> Result<(), String> {
    let state = app.state::<IrohState>();
    let conns = state.conns();
    let conn = conns.get(&id).ok_or("unknown iroh connection")?;
    conn.tx
        .send(frame)
        .map_err(|_| "iroh connection is closed".to_string())
}

#[command]
fn close<R: Runtime>(app: AppHandle<R>, id: u64) {
    if let Some(conn) = app.state::<IrohState>().conns().remove(&id) {
        conn.abort.abort();
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
    let (mut sink, mut stream) = text_frames(tokio::io::join(recv, send));

    let reader = async {
        while let Some(item) = stream.next().await {
            match item {
                Ok(frame) => {
                    if channel.send(Inbound::Frame { frame }).is_err() {
                        break; // JS side dropped the channel
                    }
                }
                Err(_) => break, // stream error ⇒ wire gone
            }
        }
    };
    let writer = async {
        while let Some(frame) = rx.recv().await {
            if sink.send(frame).await.is_err() {
                break; // write error ⇒ wire gone
            }
        }
    };

    tokio::select! {
        _ = reader => {}
        _ = writer => {}
    }
    // Best-effort terminal signal; JS collapses it to a single onClose.
    let _ = channel.send(Inbound::Close);
    // Reclaim the table entry (no-op if `close` already removed it). If this
    // task was aborted by `close`, `close` did the removal instead.
    app.state::<IrohState>().conns().remove(&id);
}

/// Initialize the plugin. Register with the Tauri builder via
/// `.plugin(tauri_plugin_peerline_iroh::init())`. No relay configuration is
/// needed — the dialing endpoint adopts the relay carried by each ticket it
/// dials (the acceptor bakes its relay into the ticket).
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("peerline-iroh")
        .invoke_handler(tauri::generate_handler![connect, send, close])
        .setup(|app, _api| {
            app.manage(IrohState::default());
            Ok(())
        })
        .build()
}
