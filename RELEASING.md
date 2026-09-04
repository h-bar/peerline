# Releasing

Every crate and the npm package share one version, bumped together.

## Bump

`version` is declared once, in the root `Cargo.toml` under
`[workspace.package]`, and inherited by every member. It also appears in
`[workspace.dependencies]`, where each internal crate is declared with both a
path and a version — the path is what a workspace build uses, the version is
what `cargo package` writes into the published manifest.

So a bump touches two places:

1. `[workspace.package] version` in `Cargo.toml`
2. each `version = "…"` in `[workspace.dependencies]` in the same file

The TypeScript package versions itself in the `peerline-ts` repository. The
two are kept in lockstep by convention, not by tooling.

Then `cargo check --workspace` to refresh `Cargo.lock`.

## Publish order

The crates form a DAG, and `cargo package` resolves internal version
requirements against the crates.io index — so a dependent cannot even be
packaged until its dependency is live. Publish in this order, waiting for the
index to update between steps:

```sh
cargo publish -p peerline
cargo publish -p peerline-transport-ws
cargo publish -p peerline-transport-uds
cargo publish -p peerline-transport-iroh
```

The conformance suite is a separate repository and publishes nothing.

This is also why CI only runs `cargo package -p peerline`: that crate has no
internal dependencies, so it is the only one verifiable before a release.

## npm

`@yanz/peerline` releases from the `peerline-ts` repository; see its README.

## Before any release

```sh
just test
cargo package -p peerline

# and, from the sibling suite — vectors, bindings drift, both interop
# directions, against this checkout and peerline-ts:
cd ../peerline-conformance && just
```

## The sibling repository

`peerline-host` — the `Service` trait, the multi-transport `Host` and the
`peerline-manager` registry — releases separately and depends on this one.
Publish here first; that repo's crates reference these by version.
