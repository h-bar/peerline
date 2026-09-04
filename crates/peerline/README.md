# peerline

Peer-symmetric bidirectional RPC toolkit — typed request / response /
notification / stream envelopes with a pubsub layer, a version-tagged JSON
wire format, and an optional stateful `Peer` runtime.

Peer-symmetric means there are no client and server roles at the protocol
level. Either endpoint may send any frame at any time, and both use the same
API to call, notify, reply and open streams.

```toml
[dependencies]
peerline = { version = "0.0.6", features = ["runtime"] }
```

## Wire format

```jsonc
{"ver":"1","kind":"req",   "id":7, "op":"add", "args":{"a":1,"b":2}}
{"ver":"1","kind":"resp",  "id":7, "data":3}
{"ver":"1","kind":"resp",  "id":7, "err":{"code":-32603,"msg":"bad"}}
{"ver":"1","kind":"notif",         "op":"event", "args":{}}
{"ver":"1","kind":"stream","id":7, "seq":0, "data":"a"}
```

Four envelopes under a two-level tag: the outer `ver` selects the wire
version, the inner `kind` selects the envelope shape. Responses are a
`Result`-shaped enum, so success/error mutual exclusion is enforced by the
type system rather than by runtime checks. Adding a `ver: "2"` is purely
additive.

Streaming is flat — no `Open` / `Close` / `Cancel` frame. `seq >= 0` is an
item, `seq == -1` is the terminal and may carry a final payload and/or an
error. All frames correlate to the originating request by `id`.

## Modules

- **`wire`** — frame envelopes, version-tag dispatch, standard error codes.
  Single-frame only; packing several frames into one message is a transport
  concern.
- **`peer`** — parsing and dispatch primitives. `parse_frame` turns text into
  a `Frame`; `classify` sorts it into a pending response, an incoming
  request, a notification or a stream frame. Outgoing builders and
  `RequestIdGen` cover the send side. No state, no I/O.
- **`pubsub`** — subscription layer on top of the core. Subscribe RPCs return
  a `SubscriptionAck`; the pushing peer sends `event` / `end` notifications;
  the receiver cancels with an `unsubscribe` request.
- **`runtime`** *(feature `runtime`)* — the stateful `Peer`: pending-request
  map, handler registry, stream registry, outbound channel. Built on
  `futures::channel`, so it is runtime-agnostic — tokio, async-std, smol and
  wasm all work.

## Features

| Feature | Adds |
| --- | --- |
| *(default)* | wire types and the pure `peer` / `pubsub` helpers only |
| `runtime` | the stateful `Peer` — `call` / `notify` / `call_stream` / handlers |
| `ts-export` | build-time only: ts-rs bindings generation |

## Transports

Transport crates in the same repository carry these frames over the wire:
[`peerline-transport-ws`], [`peerline-transport-uds`] and
[`peerline-transport-iroh`]. Each is a self-contained accept loop that drives
one `Peer` per connection, so no transport types cross into your code.

Byte-level framing is a transport concern, but the frame-size ceiling
(`peerline::MAX_FRAME_LEN`) is shared policy: every transport pins its codec
to that one value, so no peer can make any decoder buffer more than that
before a frame is rejected.

## Wire compatibility

A TypeScript implementation, [`@yanz/peerline`], lives in the same repository
and is pinned to this crate by golden frame vectors both parsers read and by a
live interop battery run over a real WebSocket in both directions. That suite
is a separate repository, [`peerline-conformance`], which depends on this
crate — nothing here depends on it, and no test scaffolding reaches this
crate's API or dependency set.

## License

MIT.

[`peerline-transport-ws`]: https://crates.io/crates/peerline-transport-ws
[`peerline-transport-uds`]: https://crates.io/crates/peerline-transport-uds
[`peerline-transport-iroh`]: https://crates.io/crates/peerline-transport-iroh
[`@yanz/peerline`]: https://www.npmjs.com/package/@yanz/peerline
[`peerline-conformance`]: https://github.com/h-bar/peerline-conformance
