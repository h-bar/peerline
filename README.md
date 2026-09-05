# peerline

Peer-symmetric bidirectional RPC: requests, notifications, correlated streams
and a pubsub layer over a small versioned JSON wire format.

**Peer-symmetric** means there are no client and server roles at the protocol
level. Either endpoint may send any frame at any time, and both use the same
API to call, notify, reply and open streams. A "server" that needs to ask its
"client" a question just calls it.

This is the Rust implementation. A TypeScript one, [`@h-bar/peerline`], lives
in [`peerline-ts`], and the two are pinned to each other by a shared
conformance suite rather than by good intentions — see
[Conformance](#conformance).

```jsonc
// request                                    // notification (no id)
{"ver":"1","kind":"req","id":7,"op":"add","args":{"a":1,"b":2}}
{"ver":"1","kind":"notif","op":"event","args":{}}
// success response                           // error response
{"ver":"1","kind":"resp","id":7,"data":3}
{"ver":"1","kind":"resp","id":7,"err":{"code":-32603,"msg":"bad"}}
// stream item, then terminal (seq -1)
{"ver":"1","kind":"stream","id":7,"seq":0,"data":"a"}
{"ver":"1","kind":"stream","id":7,"seq":-1}
```

Every frame is one of four envelopes unified under a two-level tag: the outer
`ver` selects the wire version, the inner `kind` selects the envelope. Adding
a `ver: "2"` is purely additive — v1 code is untouched. Field names are kept
to four characters or fewer.

Streaming is first-class and flat: there is no `Open` / `Close` / `Cancel`
frame. The lifecycle lives in `seq` — `>= 0` is an item, `-1` is the terminal,
which may carry a final payload and/or an error. Cancellation is a reserved
notification, not a frame kind.

## Quick start

```rust
use peerline::runtime::Peer;

let sum: i64 = peer.call("add", &Args { a: 1, b: 2 }).await?;
peer.notify("log", &Line { line: "started" })?;

peer.on_request("ping", |_: ()| async { Ok::<_, RpcError>("pong") });

let mut items = peer.call_stream::<_, String>("tail", &N { n: 5 })?;
while let Some(item) = items.next().await { /* … */ }
```

## Layout

```
crates/peerline/                the protocol: wire, peer, pubsub, runtime
crates/peerline-transport-ws/   WebSocket (axum accept, tungstenite dial)
crates/peerline-transport-uds/  Unix domain sockets, newline-delimited JSON
crates/peerline-transport-iroh/ iroh P2P over QUIC, ticket codec + ALPN
```

The core crate is transport-agnostic and runtime-agnostic. `peerline` with
default features is wire types only; `runtime` adds the stateful `Peer` on
`futures::channel`, which works on tokio, async-std, smol and wasm. Byte-level
framing is a transport concern, but the frame-size ceiling
(`peerline::MAX_FRAME_LEN`) is shared policy so no peer can make any decoder
buffer more than that.

The service layer built on top of this — a composable `Service` trait, a
multi-transport `Host`, and a live service registry — lives in the sibling
[`peerline-host`](https://github.com/h-bar/peerline-host) repository. The
dependency arrow only ever points here.

## Conformance

Two implementations of a protocol drift unless something stops them. The suite
that stops them lives in the sibling
[`peerline-conformance`](https://github.com/h-bar/peerline-conformance)
repository, which depends on this one — nothing here depends on it.

It holds the contract and both languages' enforcement of it: 108 golden frame
vectors read by a Rust and a TypeScript parser check, and a live battery with
one server and one client per language, so a cross-language run is one
language's server paired with the other's client. It also owns the
cross-transport matrix — which is why the transport crates here carry no
`tests/` of their own — and the drift check on the generated TypeScript wire
types, since regenerating them is the one job that needs to see both
languages at once.

```
libs/
├── peerline/              this repo — the Rust implementation
├── peerline-ts/           the TypeScript implementation
├── peerline-conformance/  the suite, which depends on both
└── peerline-host/         the service layer, which depends on this repo
```

```sh
cd ../peerline-conformance && just
```

## Development

```sh
just test              # cargo test --workspace
just clippy            # cargo clippy --workspace --all-targets
just fmt
```

Requires a Rust toolchain and nothing else — this repository is Rust only.
The toolchain is pinned to stable in `rust-toolchain.toml`.

## License

MIT.

[`@h-bar/peerline`]: https://www.npmjs.com/package/@h-bar/peerline
[`peerline-ts`]: https://github.com/h-bar/peerline-ts

