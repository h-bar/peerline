# peerline-transport-iroh

[Iroh](https://www.iroh.computer) P2P (QUIC) transport for
[peerline](https://crates.io/crates/peerline): ticket codec,
length-delimited text framing, and a reusable accept loop that screens
each connection with an [`IrohPolicy`] acceptor (endpoint-id allowlist
or custom checks over the handshake-proved identity) and drives one
peerline `Peer` per bi-stream. The dial side's `connect` reports policy
refusals structurally (`ConnectError::Refused`, from the
`CLOSE_REFUSED` application close).

The ALPN is caller-supplied, so one host can run several peerline
services behind a single endpoint — one ALPN each.

See the [peerline repository](https://github.com/h-bar/peerline) for the
wire format, the runtime, and the sibling transports.

License: MIT.
