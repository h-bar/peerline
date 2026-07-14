//! WebSocket transport for peerline (axum), for browser clients.
//!
//! [`serve`] binds an address, applies permissive CORS, upgrades each
//! connection to a WebSocket, and drives one [`peerline::runtime::Peer`]
//! per connection over its text frames — configured by a caller-supplied
//! handler closure. axum is fully encapsulated here: the caller passes an
//! address + closure and no axum types cross the boundary.

#![forbid(unsafe_code)]

use std::net::SocketAddr;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use peerline::runtime::Peer;
use tower_http::cors::CorsLayer;
use tracing::info;

/// Bind to `addr` and serve peerline over WebSocket forever, driving one
/// [`peerline::runtime::Peer`] per connection — configured by `on_peer`
/// (register your handlers there).
///
/// Permissive CORS is applied so browser clients on any origin can
/// connect; front a scoped origin allowlist in production. A single
/// JSON-RPC frame is capped at [`peerline::MAX_FRAME_LEN`].
pub async fn serve<F>(addr: SocketAddr, on_peer: F) -> Result<(), String>
where
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    info!(addr = %addr, "peerline-ws listening");
    let app = Router::new()
        .route("/", get(ws_upgrade::<F>))
        .layer(CorsLayer::permissive())
        .with_state(on_peer);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("ws bind: {e}"))?;
    axum::serve(listener, app).await.map_err(|e| format!("ws serve: {e}"))
}

async fn ws_upgrade<F>(ws: WebSocketUpgrade, State(on_peer): State<F>) -> impl IntoResponse
where
    F: Fn(&Peer) + Clone + Send + Sync + 'static,
{
    // Pin the frame ceiling to the shared [`peerline::MAX_FRAME_LEN`]
    // rather than tungstenite's default. `max_message_size` bounds the
    // reassembled message; `max_frame_size` bounds one WebSocket frame.
    ws.max_message_size(peerline::MAX_FRAME_LEN)
        .max_frame_size(peerline::MAX_FRAME_LEN)
        .on_upgrade(move |socket| serve_conn(socket, on_peer))
}

async fn serve_conn<F>(socket: WebSocket, on_peer: F)
where
    F: Fn(&Peer),
{
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
    on_peer(&peer);
    driver.await;
    info!("peerline-ws connection closed");
}
