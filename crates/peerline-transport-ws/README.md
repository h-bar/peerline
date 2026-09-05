# peerline-transport-ws

WebSocket transport for [peerline](https://crates.io/crates/peerline) —
an axum-powered accept side that screens each handshake with a
[`WsPolicy`] acceptor (origin allowlist, loopback-only, custom checks)
and drives one peerline `Peer` per admitted connection, plus a
tokio-tungstenite dial side whose `connect` reports policy refusals
structurally (`ConnectError::Refused`).

```rust,ignore
use peerline_transport_ws::{serve, WsPolicy};

serve(
    "127.0.0.1:6467".parse()?,
    WsPolicy::origins(["https://app.example"])
        .loopback_only()
        .acceptor(|peer| {
            peer.on_request("ping", |_: serde_json::Value| async {
                Ok::<_, peerline::wire::RpcError>("pong")
            });
        }),
)
.await?;
```

See the [peerline repository](https://github.com/h-bar/peerline) for the
wire format, the runtime, and the sibling transports.

License: MIT.
