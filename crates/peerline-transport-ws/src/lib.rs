//! WebSocket transport for peerline. Symmetric — the same text-frame wire
//! on both ends:
//!
//! - [`serve`] (accept side, axum) binds an address, screens each incoming
//!   handshake, and drives one [`peerline::runtime::Peer`] per admitted
//!   connection. axum is fully encapsulated: the caller passes an address
//!   plus an [`Acceptor`], and no axum types cross the boundary.
//! - [`connect`] (dial side, tokio-tungstenite) dials a `ws://` / `wss://`
//!   URL and returns one `(Peer, driver)` for the caller to configure.
//!
//! peerline is peer-symmetric, so binding vs dialing only decides which
//! end is addressable — once connected, either peer may issue any frame.
//!
//! ### Admission
//!
//! The accept side decides **before upgrading** whether to serve a
//! connection. The [`Acceptor`] closure receives a [`WsAccept`] — the
//! handshake's facts — and either returns the peer initializer to run, or
//! refuses, which answers the handshake with `403 Forbidden` instead of
//! the `101` upgrade.
//!
//! Read [`WsAccept::origin`] before exposing a service to browsers. This
//! crate installs no CORS layer: CORS does not govern WebSocket
//! handshakes, so one would restrict nothing while implying otherwise.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::ConnectInfo;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::future::BoxFuture;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use peerline::runtime::Peer;
use tracing::{info, warn};

/// A type-erased per-connection peer initializer — the closure that
/// registers a service's handlers, after boxing.
pub type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;

// ---------------------------------------------------------------------------
// WsAccept — the handshake's facts
// ---------------------------------------------------------------------------

/// What is known about an incoming WebSocket handshake, before it is
/// upgraded. Handed to an [`Acceptor`] to screen the connection.
///
/// Accessors rather than public fields, so no `http` / `axum` type is part
/// of this crate's surface — the same encapsulation [`serve`] keeps for
/// the rest of axum.
#[derive(Clone)]
pub struct WsAccept {
    path: String,
    query: Option<String>,
    headers: HeaderMap,
    remote_addr: SocketAddr,
}

impl WsAccept {
    /// The mount path this handshake arrived on.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The request's query string, if any.
    #[must_use]
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// The client's socket address. Useful mainly to tell loopback from
    /// everything else.
    #[must_use]
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// The `Origin` header, verbatim.
    ///
    /// **This is the only cross-site control available**, and it is worth
    /// knowing exactly what it is. A browser sets `Origin` itself, outside
    /// the page's control, and always sends it on a WebSocket handshake —
    /// so comparing it against an allowlist is what stops a page the user
    /// happens to visit from opening a socket to a `localhost` service and
    /// invoking every registered op. CORS does not do this: browsers
    /// exempt WebSocket handshakes from it entirely.
    ///
    /// A non-browser client sends whatever it likes, or nothing. `Origin`
    /// therefore bounds what a *browser* can be made to do; it is worth
    /// nothing against an attacker holding a socket. Where the caller's
    /// identity is what matters, use a transport that carries one —
    /// `peerline-transport-uds` has kernel-attested credentials,
    /// `peerline-transport-iroh` a handshake-proved endpoint id.
    ///
    /// `None` means the header was absent **or** not parseable as text.
    #[must_use]
    pub fn origin(&self) -> Option<&str> {
        self.header("origin")
    }

    /// The `Host` header, verbatim.
    #[must_use]
    pub fn host(&self) -> Option<&str> {
        self.header("host")
    }

    /// Any request header, by lowercase name. `None` if absent or not
    /// parseable as text.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

/// Hand-written rather than derived: the handshake's headers include
/// `Cookie` and `Authorization`, and `HeaderValue`'s `Debug` prints
/// whatever it holds unless the value was explicitly marked sensitive. A
/// derive would hand every embedder that logs `{accept:?}` a credential
/// leak. Only the fields a refusal log wants are printed; reach the rest
/// through [`WsAccept::header`] deliberately.
impl std::fmt::Debug for WsAccept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsAccept")
            .field("path", &self.path)
            .field("origin", &self.origin())
            .field("remote_addr", &self.remote_addr)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// WsAdmit — what an admitted handshake gets
// ---------------------------------------------------------------------------

/// The outcome of admitting a handshake: the peer initializer to run, plus
/// any WebSocket subprotocol to select.
pub struct WsAdmit {
    init: PeerHandler,
    subprotocol: Option<String>,
}

impl WsAdmit {
    /// Admit the connection, registering handlers with `init`.
    pub fn new<F>(init: F) -> Self
    where
        F: Fn(&Peer) + Send + Sync + 'static,
    {
        Self {
            init: Arc::new(init),
            subprotocol: None,
        }
    }

    /// Select a WebSocket subprotocol in the handshake response.
    ///
    /// Browser JavaScript cannot set request headers, so
    /// `Sec-WebSocket-Protocol` is the only channel a browser client has
    /// for something like a token — and a browser **fails the connection**
    /// unless the server selects one of the protocols it offered. A
    /// token-in-subprotocol scheme therefore needs this echo; without it
    /// the browser rejects the handshake the acceptor just admitted.
    #[must_use]
    pub fn with_subprotocol(mut self, protocol: impl Into<String>) -> Self {
        self.subprotocol = Some(protocol.into());
        self
    }
}

impl From<PeerHandler> for WsAdmit {
    fn from(init: PeerHandler) -> Self {
        Self {
            init,
            subprotocol: None,
        }
    }
}

/// Screens one handshake: given its [`WsAccept`] facts, either admit it —
/// returning the initializer to run on the resulting peer — or refuse with
/// a reason.
///
/// A refusal answers the handshake with `403 Forbidden` and logs `reason`;
/// the reason itself never reaches the client. Because this runs before
/// the upgrade, a refused connection costs no `Peer`, no split socket and
/// not one dispatched frame — and the client sees a clean handshake
/// failure rather than a socket that opens and immediately closes.
///
/// The closure is synchronous on purpose: everything it judges is already
/// in memory. Anything needing I/O to verify is application-level
/// authentication, which belongs in an op on the admitted peer.
pub type Acceptor = Arc<dyn Fn(&WsAccept) -> Result<WsAdmit, String> + Send + Sync + 'static>;

// ---------------------------------------------------------------------------
// WsPolicy — the common decisions, as a value
// ---------------------------------------------------------------------------

type CustomCheck = Arc<dyn Fn(&WsAccept) -> Result<(), String> + Send + Sync + 'static>;

/// A reusable admission policy — the common cases spelled out, so callers
/// need not hand-roll a closure for each. A value rather than a closure,
/// so a mount table can carry it.
#[derive(Clone, Default)]
pub struct WsPolicy {
    origins: Option<Arc<[String]>>,
    loopback_only: bool,
    /// Every check must pass. A list, not one slot: a policy that silently
    /// dropped an earlier `and` would be weaker than it reads, which is
    /// the failure this whole type exists to prevent.
    custom: Vec<CustomCheck>,
}

impl WsPolicy {
    /// Admit every handshake — what this crate did before admission
    /// existed, now stated at the call site instead of assumed.
    #[must_use]
    pub fn allow_any() -> Self {
        Self::default()
    }

    /// Admit a handshake whose `Origin` matches one of `origins` exactly
    /// (compare serialized origins: `https://app.example`,
    /// `http://127.0.0.1:6467`, `tauri://localhost`).
    ///
    /// A handshake carrying **no** `Origin` is admitted: native clients
    /// send none, and refusing them would break every non-browser dialer
    /// while stopping no attacker — anyone able to open a socket can also
    /// omit the header. See [`WsAccept::origin`]; pair this with
    /// [`Self::loopback_only`], or use a transport that authenticates the
    /// caller, when that matters.
    ///
    /// A sandboxed iframe or a `file://` page sends the literal string
    /// `"null"`, which is a value like any other here — refused unless
    /// listed.
    #[must_use]
    pub fn origins<I, S>(origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            origins: Some(origins.into_iter().map(Into::into).collect()),
            ..Self::default()
        }
    }

    /// Additionally require the client's address to be loopback.
    #[must_use]
    pub fn loopback_only(mut self) -> Self {
        self.loopback_only = true;
        self
    }

    /// Additionally require `f` to accept. Composes: every `and` is kept
    /// and all must pass.
    #[must_use]
    pub fn and<F>(mut self, f: F) -> Self
    where
        F: Fn(&WsAccept) -> Result<(), String> + Send + Sync + 'static,
    {
        self.custom.push(Arc::new(f));
        self
    }

    /// A policy that is only `f`.
    #[must_use]
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(&WsAccept) -> Result<(), String> + Send + Sync + 'static,
    {
        Self::default().and(f)
    }

    /// Apply the policy. `Err(reason)` refuses the handshake.
    pub fn check(&self, accept: &WsAccept) -> Result<(), String> {
        if let Some(allowed) = &self.origins
            && let Some(origin) = accept.origin()
            && !allowed.iter().any(|a| a == origin)
        {
            return Err(format!("origin not permitted: {origin}"));
        }
        if self.loopback_only && !accept.remote_addr().ip().is_loopback() {
            return Err(format!("not loopback: {}", accept.remote_addr()));
        }
        for custom in &self.custom {
            custom(accept)?;
        }
        Ok(())
    }

    /// Pair this policy with a peer initializer, giving the closure
    /// [`serve`] wants when the policy is all the screening needed.
    /// Wrap in [`Arc`] for a [`serve_mounted`] table.
    pub fn acceptor<F>(
        self,
        on_peer: F,
    ) -> impl Fn(&WsAccept) -> Result<WsAdmit, String> + Send + Sync + 'static
    where
        F: Fn(&Peer) + Send + Sync + 'static,
    {
        let on_peer: PeerHandler = Arc::new(on_peer);
        move |accept: &WsAccept| {
            self.check(accept)?;
            Ok(WsAdmit::from(on_peer.clone()))
        }
    }
}

// ---------------------------------------------------------------------------
// Accept side
// ---------------------------------------------------------------------------

/// Bind to `addr` and serve peerline over WebSocket forever, mounted at
/// `/`, screening each handshake with `acceptor`.
///
/// A single JSON-RPC frame is capped at [`peerline::MAX_FRAME_LEN`].
///
/// ```ignore
/// serve(addr, WsPolicy::origins(["https://app.example"])
///           .loopback_only()
///           .acceptor(|peer| {
///               peer.on_request("ping", |_: ()| async { Ok::<_, RpcError>("pong") });
///           }))
/// .await
/// ```
pub async fn serve<F>(addr: SocketAddr, acceptor: F) -> Result<(), String>
where
    F: Fn(&WsAccept) -> Result<WsAdmit, String> + Send + Sync + 'static,
{
    serve_mounted(addr, vec![("/".to_string(), Arc::new(acceptor))]).await
}

/// Bind to `addr` and serve several peerline services on one port,
/// **mounted by path**: each `(path, acceptor)` screens and serves
/// connections to that path. A client dials `ws://host:port<path>` to
/// reach one service; the services never share a peer, so their ops can't
/// collide. Same frame ceiling as [`serve`].
pub async fn serve_mounted(
    addr: SocketAddr,
    mounts: Vec<(String, Acceptor)>,
) -> Result<(), String> {
    let mut app = Router::new();
    for (path, acceptor) in mounts {
        info!(addr = %addr, path, "peerline-ws mount");
        let mount = path.clone();
        app = app.route(
            &path,
            get(
                move |headers: HeaderMap,
                      ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
                      uri: Uri,
                      ws: WebSocketUpgrade| {
                    let accept = WsAccept {
                        path: mount.clone(),
                        query: uri.query().map(str::to_owned),
                        headers,
                        remote_addr,
                    };
                    let acceptor = acceptor.clone();
                    async move { screen(ws, accept, &acceptor) }
                },
            ),
        );
    }
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("ws bind: {e}"))?;
    // `into_make_service_with_connect_info` is what makes the client's
    // address reachable as `ConnectInfo` above.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|e| format!("ws serve: {e}"))
}

/// Run the acceptor, then either upgrade or answer `403`.
fn screen(ws: WebSocketUpgrade, accept: WsAccept, acceptor: &Acceptor) -> Response {
    let admit = match acceptor(&accept) {
        Ok(admit) => admit,
        Err(reason) => {
            warn!(
                path = %accept.path(),
                origin = ?accept.origin(),
                remote = %accept.remote_addr(),
                %reason,
                "peerline-ws handshake refused"
            );
            return StatusCode::FORBIDDEN.into_response();
        }
    };
    // Pin the frame ceiling to the shared [`peerline::MAX_FRAME_LEN`]
    // rather than tungstenite's default. `max_message_size` bounds the
    // reassembled message; `max_frame_size` bounds one WebSocket frame.
    let mut ws = ws
        .max_message_size(peerline::MAX_FRAME_LEN)
        .max_frame_size(peerline::MAX_FRAME_LEN);
    if let Some(protocol) = admit.subprotocol {
        ws = ws.protocols([protocol]);
    }
    let init = admit.init;
    ws.on_upgrade(move |socket| serve_conn(socket, init))
        .into_response()
}

async fn serve_conn(socket: WebSocket, init: PeerHandler) {
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
    init(&peer);
    driver.await;
    info!("peerline-ws connection closed");
}

// ---------------------------------------------------------------------------
// Dial side — unchanged
// ---------------------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn accept_with(origin: Option<&str>, remote: &str) -> WsAccept {
        let mut headers = HeaderMap::new();
        if let Some(o) = origin {
            headers.insert("origin", o.parse().expect("header"));
        }
        WsAccept {
            path: "/".into(),
            query: None,
            headers,
            remote_addr: remote.parse().expect("addr"),
        }
    }

    /// The allowlist rejects a mismatched browser origin and passes a
    /// matching one. Note the third case: **no** `Origin` is admitted, by
    /// design — see `WsPolicy::origins`.
    #[test]
    fn origin_policy_matches_exactly_and_admits_absent() {
        let p = WsPolicy::origins(["https://app.example"]);
        assert!(
            p.check(&accept_with(Some("https://app.example"), "127.0.0.1:1"))
                .is_ok()
        );
        assert!(
            p.check(&accept_with(Some("https://evil.example"), "127.0.0.1:1"))
                .is_err()
        );
        assert!(
            p.check(&accept_with(
                Some("https://app.example.evil"),
                "127.0.0.1:1"
            ))
            .is_err()
        );
        assert!(p.check(&accept_with(None, "127.0.0.1:1")).is_ok());
    }

    /// Every `and` is kept. A policy that dropped an earlier check would
    /// be silently weaker than it reads — the exact failure this type is
    /// meant to prevent.
    #[test]
    fn and_composes_rather_than_replacing() {
        let p = WsPolicy::allow_any()
            .and(|a: &WsAccept| {
                if a.path() == "/" {
                    Ok(())
                } else {
                    Err("bad path".into())
                }
            })
            .and(|a: &WsAccept| {
                if a.header("x-token").is_some() {
                    Ok(())
                } else {
                    Err("no token".into())
                }
            });
        // The second check is not enough on its own: the first still runs.
        let mut ok = accept_with(None, "127.0.0.1:1");
        ok.headers.insert("x-token", "t".parse().expect("header"));
        assert!(p.check(&ok).is_ok());
        assert_eq!(
            p.check(&accept_with(None, "127.0.0.1:1")).unwrap_err(),
            "no token"
        );
        let mut wrong_path = ok.clone();
        wrong_path.path = "/other".into();
        assert_eq!(p.check(&wrong_path).unwrap_err(), "bad path");
    }

    /// `Debug` must not print headers: they carry `Cookie` and
    /// `Authorization`, and an embedder logging the accept struct should
    /// not thereby log credentials.
    #[test]
    fn debug_does_not_leak_headers() {
        let mut a = accept_with(Some("https://app.example"), "127.0.0.1:1");
        a.headers
            .insert("cookie", "session=hunter2".parse().expect("header"));
        a.headers
            .insert("authorization", "Bearer sekrit".parse().expect("header"));
        let rendered = format!("{a:?}");
        assert!(!rendered.contains("hunter2"), "leaked a cookie: {rendered}");
        assert!(!rendered.contains("sekrit"), "leaked a token: {rendered}");
        assert!(
            rendered.contains("https://app.example"),
            "should still show origin"
        );
    }

    #[test]
    fn loopback_only_composes_with_origins() {
        let p = WsPolicy::origins(["https://app.example"]).loopback_only();
        assert!(
            p.check(&accept_with(Some("https://app.example"), "127.0.0.1:1"))
                .is_ok()
        );
        let err = p
            .check(&accept_with(Some("https://app.example"), "192.168.1.9:1"))
            .expect_err("non-loopback must be refused");
        assert!(err.contains("loopback"), "unexpected: {err}");
    }

    /// End to end: a refused handshake gets **403 and no upgrade**, so the
    /// client never reaches an op. The status is the point — a post-upgrade
    /// refusal would look to the client like a crash.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refused_handshake_answers_403_without_upgrading() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        use tokio_tungstenite::tungstenite::error::Error as WsError;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);
        let server = tokio::spawn(async move {
            serve(
                addr,
                WsPolicy::origins(["https://app.example"]).acceptor(|peer: &Peer| {
                    peer.on_request("ping", |_: serde_json::Value| async {
                        Ok::<_, peerline::wire::RpcError>("pong")
                    });
                }),
            )
            .await
        });

        let dial = |origin: &'static str| async move {
            let url = format!("ws://{addr}");
            for _ in 0..200 {
                let mut req = url.as_str().into_client_request().expect("request");
                req.headers_mut()
                    .insert("origin", origin.parse().expect("header"));
                match tokio_tungstenite::connect_async(req).await {
                    Ok(ok) => return Ok(ok),
                    // Refusal is an HTTP response, not a connect failure —
                    // retry only while nothing is listening yet.
                    Err(WsError::Http(r)) => return Err(r.status()),
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
            panic!("server never came up");
        };

        assert_eq!(
            dial("https://evil.example").await.err(),
            Some(axum::http::StatusCode::FORBIDDEN),
            "a disallowed origin must be refused with 403, not upgraded"
        );
        assert!(
            dial("https://app.example").await.is_ok(),
            "allowed origin must be served"
        );

        server.abort();
    }
}
