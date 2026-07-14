//! WebSocket transport for peerline. Symmetric — the same text-frame wire
//! on both ends:
//!
//! - [`serve`] (accept side, axum) binds an address, applies permissive
//!   CORS, upgrades each connection, and drives one
//!   [`peerline::runtime::Peer`] per connection via a handler closure.
//!   axum is fully encapsulated: the caller passes an address + closure
//!   and no axum types cross the boundary.
//! - [`connect`] (dial side, tokio-tungstenite) dials a `ws://` / `wss://`
//!   URL and returns one `(Peer, driver)` for the caller to configure.
//!
//! peerline is peer-symmetric, so binding vs dialing only decides which
//! end is addressable — once connected, either peer may issue any frame.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use futures::future::BoxFuture;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use peerline::runtime::Peer;
use tower_http::cors::CorsLayer;
use tracing::info;

/// A type-erased per-connection peer initializer — the `on_peer` closure
/// after boxing, so a routing table can hold heterogeneous services.
pub type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;

/// Bind to `addr` and serve peerline over WebSocket forever, driving one
/// [`peerline::runtime::Peer`] per connection — configured by `on_peer`
/// (register your handlers there). The service is mounted at `/`.
///
/// Permissive CORS is applied so browser clients on any origin can
/// connect; front a scoped origin allowlist in production. A single
/// JSON-RPC frame is capped at [`peerline::MAX_FRAME_LEN`].
pub async fn serve<F>(addr: SocketAddr, on_peer: F) -> Result<(), String>
where
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    serve_mounted(addr, vec![("/".to_string(), Arc::new(on_peer))]).await
}

/// Bind to `addr` and serve several peerline services on one port,
/// **mounted by path**: each `(path, handler)` routes connections to that
/// path onto their own peer. A client dials `ws://host:port<path>` to
/// reach one service; the services never share a peer, so their ops can't
/// collide. Same CORS + frame ceiling as [`serve`].
pub async fn serve_mounted(
    addr: SocketAddr,
    mounts: Vec<(String, PeerHandler)>,
) -> Result<(), String> {
    let mut app = Router::new();
    for (path, handler) in mounts {
        info!(addr = %addr, path, "peerline-ws mount");
        app = app.route(&path, get(move |ws: WebSocketUpgrade| ws_upgrade(ws, handler.clone())));
    }
    let app = app.layer(CorsLayer::permissive());
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("ws bind: {e}"))?;
    axum::serve(listener, app).await.map_err(|e| format!("ws serve: {e}"))
}

async fn ws_upgrade(ws: WebSocketUpgrade, handler: PeerHandler) -> impl IntoResponse {
    // Pin the frame ceiling to the shared [`peerline::MAX_FRAME_LEN`]
    // rather than tungstenite's default. `max_message_size` bounds the
    // reassembled message; `max_frame_size` bounds one WebSocket frame.
    ws.max_message_size(peerline::MAX_FRAME_LEN)
        .max_frame_size(peerline::MAX_FRAME_LEN)
        .on_upgrade(move |socket| serve_conn(socket, handler))
}

async fn serve_conn(socket: WebSocket, handler: PeerHandler) {
    info!("peerline-ws connection opened");
    let (ws_sink, ws_stream) = socket.split();
    let sink = Box::pin(
        ws_sink
            .with(|text: String| async move { Ok::<_, axum::Error>(Message::Text(text.into())) }),
    );
    let stream = Box::pin(ws_stream.filter_map(|frame| async move {
        match frame {
            Ok(Message::Text(t)) => Some(Ok::<_, axum::Error>(t.to_string())),
            Ok(Message::Close(_)) => None,
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }
    }));
    let (peer, driver) = Peer::new(sink, stream);
    handler(&peer);
    driver.await;
    info!("peerline-ws connection closed");
}

/// Dial a peerline endpoint at `url` (`ws://host:port` or `wss://…`, the
/// dial side of [`serve`]) and return one `(Peer, driver)`. Register
/// handlers on the peer, then drive `driver` to run the session; it
/// resolves when the socket closes.
///
/// A single frame is capped at [`peerline::MAX_FRAME_LEN`], matching the
/// accept side. Uses `tokio-tungstenite`, so no axum types are involved
/// on the dial path.
pub async fn connect(url: &str) -> Result<(Peer, BoxFuture<'static, ()>), String> {
    use tokio_tungstenite::tungstenite::Message as TMessage;
    use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

    // Pin the ceiling to the shared [`peerline::MAX_FRAME_LEN`] rather than
    // tungstenite's defaults, so the dialer agrees with every acceptor.
    let config = WebSocketConfig::default()
        .max_message_size(Some(peerline::MAX_FRAME_LEN))
        .max_frame_size(Some(peerline::MAX_FRAME_LEN));
    let (ws, _resp) = tokio_tungstenite::connect_async_with_config(url, Some(config), false)
        .await
        .map_err(|e| format!("ws connect {url}: {e}"))?;
    info!(url, "peerline-ws dialed");

    let (ws_sink, ws_stream) = ws.split();
    let sink = Box::pin(ws_sink.with(|text: String| async move {
        Ok::<_, tokio_tungstenite::tungstenite::Error>(TMessage::Text(text.into()))
    }));
    let stream = Box::pin(ws_stream.filter_map(|frame| async move {
        match frame {
            Ok(TMessage::Text(t)) => Some(Ok(t.to_string())),
            // Tungstenite answers ping/pong at the protocol layer; binary
            // frames aren't part of the JSON-RPC wire. Drop both rather
            // than tearing the connection down.
            Ok(TMessage::Close(_)) => None,
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }
    }));
    let (peer, driver) = Peer::new(sink, stream);
    Ok((peer, Box::pin(driver)))
}
