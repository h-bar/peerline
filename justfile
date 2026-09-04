# peerline workspace tasks. List with `just`.

default:
    @just --list

# --- workspace ---

test:
    cargo test --workspace

clippy:
    cargo clippy --workspace --all-targets

fmt:
    cargo fmt --all

# --- cross-language ---
# This repository is the Rust implementation alone. The TypeScript one lives
# in `peerline-ts`, and everything that checks the two against each other —
# golden vectors, the shared battery, the interop harness and the generated
# wire-type drift check — lives in `peerline-conformance`, which depends on
# both. Run those from there:
#
#     cd ../peerline-conformance && just

clean:
    cargo clean
