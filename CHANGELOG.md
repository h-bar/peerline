# Changelog

All crates in this workspace share one version and one changelog. The
TypeScript package (`peerline-ts`) versions itself but tracks the same
wire contract; wire-affecting entries below call that out.

## 0.0.7 — 2026-09-05

First published release. Everything below landed since the repository
split at 0.0.6 (which was never published to crates.io).

### Breaking

- **Admission control** (`feat(transports)!`): `serve` / `serve_mounted`
  on every transport now take an *acceptor* — a closure screening each
  connection's facts (`UdsAccept` / `WsAccept` / `IrohAccept`) before a
  `Peer` exists — instead of a bare per-peer handler. Build one from a
  policy: `UdsPolicy::same_user().acceptor(on_peer)`. The policy
  machinery is shared: `peerline::runtime::Policy` is a conjunction of
  checks with uniform `and` / `custom` / `check` / `acceptor` semantics
  on all transports, and `PeerHandler` lives in `peerline::runtime`
  (re-exported by each transport crate).
- **Typed connect refusals**: `peerline-transport-ws::connect` and
  `peerline-transport-iroh::connect` now return their crate's
  `ConnectError` instead of `String`, with a `Refused` variant a client
  can match to distinguish "the server's policy refused me" from "the
  network broke" (`From<ConnectError> for String` keeps `?`-into-String
  callers compiling). UDS refusal remains a silent close — the dialer
  cannot observe it, as its `connect` doc now states.
- **Pubsub ops are namespaced**: the pubsub wire ops are now
  `$peerline/pubsub.event`, `$peerline/pubsub.end` and
  `$peerline/pubsub.unsubscribe` (previously the bare names `event` /
  `end` / `unsubscribe`, which occupied the application op namespace —
  an app with its own `event` op collided with pubsub pushes). Old and
  new peers do not interop on pubsub across this change; update both
  ends together. Wire-contract change — mirrored in `peerline-ts` and
  the conformance vectors.
- **Kind-mismatched replies fail fast**: a `resp` frame answering a
  `call_stream`, or `stream` frames answering a `call`, now fail the
  caller promptly (forwarding the remote's own error when it sent one)
  instead of hanging until disconnect. The unary-got-stream case also
  cancels the remote producer via `$peerline/stream.cancel`.
  Wire-behavior change — mirrored in `peerline-ts`.
- **CORS removed from the WS transport**: the permissive `CorsLayer`
  restricted nothing while implying otherwise; CORS does not govern
  WebSocket handshakes. Non-WebSocket HTTP probes of a peerline mount
  no longer receive `Access-Control-Allow-Origin` headers.

### Changed

- **Stream `seq` past `i64::MAX` is rejected** instead of wrapping
  silently — a peer-visible tightening on the wire: frames a buggy
  producer previously slipped through as wrapped sequence numbers now
  fail parsing. The remaining wire envelopes also derive `PartialEq`
  (purely additive).
- **UDS sockets publish owner-only, atomically**: the socket is bound
  and `chmod`ed under a staging name and renamed into place, so the
  advertised path never exists in a permissive state. **Deployment
  note**: the staging name appends `.<pid>.<seq>.tmp`, so a socket path
  within ~14 bytes of the platform `sun_path` limit (104 bytes on
  macOS, 108 on Linux) that previously bound now fails at startup with
  an error naming both paths — move such sockets to a shorter path.
  Staging leftovers from crashed runs are swept on the next bind.
- `WsPolicy::loopback_only` admits IPv4-mapped IPv6 loopback
  (`::ffff:127.0.0.1`), as seen on dual-stack binds.
- The UDS accept loop backs off briefly on accept errors instead of
  spinning hot under fd exhaustion.
- `StreamReceiver` is fused: at most one final `Err`, even if a stray
  frame was queued behind the terminal.
- `RpcError::internal` / `RpcError::invalid_params` constructors.
- The protocol-error hook path costs nothing when no hook is
  registered; `pubsub::classify` no longer clones the full args map
  per event.

## 0.0.6

Extracted into standalone repositories (`peerline`, `peerline-ts`,
`peerline-host`, `peerline-conformance`). History before this point
lives in the source monorepo.
