# peerline-transport-uds

Unix-domain-socket transport for [peerline](https://crates.io/crates/peerline),
for local tooling on one host. The socket is published owner-only
(`0600`) via an atomic staging rename, and each connection is screened
by a [`UdsPolicy`] acceptor over the kernel-reported peer credentials —
the one peerline transport that authenticates a *user*.

```rust,ignore
use peerline_transport_uds::{serve, UdsPolicy};

serve(
    "/run/app.sock",
    UdsPolicy::same_user().acceptor(|peer| {
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
