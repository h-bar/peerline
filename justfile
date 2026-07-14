# peerline workspace + peerline-manager service tasks. List with `just`.

default:
    @just --list

# --- workspace ---

build:
    cargo build --release -p peerline-manager

test:
    cargo test --workspace

clippy:
    cargo clippy --workspace --all-targets

fmt:
    cargo fmt --all

# --- peerline-manager service (the daemon + the peerline-mon CLI) ---
# The daemon is env-driven with canonical defaults (uds socket, WS RPC on
# 127.0.0.1:6466, web dashboard on http://127.0.0.1:6467/), so no config
# file is installed — set PEERLINE_MANAGER_* in the unit/plist to override.

install: build
    #!/usr/bin/env bash
    set -euo pipefail
    BIN_DIR="$HOME/.local/bin"
    mkdir -p "$BIN_DIR"
    cp "target/release/peerline-manager" "$BIN_DIR/peerline-manager"
    cp "target/release/peerline-mon" "$BIN_DIR/peerline-mon"
    echo "installed binaries: $BIN_DIR/peerline-manager, $BIN_DIR/peerline-mon"
    case "$(uname -s)" in
        Darwin)
            PLIST_DST="$HOME/Library/LaunchAgents/com.peerline-manager.plist"
            mkdir -p "$(dirname "$PLIST_DST")" "$HOME/Library/Logs"
            sed "s|REPLACE_ME|$USER|g" dist/macos/com.peerline-manager.plist > "$PLIST_DST"
            echo "installed plist: $PLIST_DST"
            launchctl bootout "gui/$UID/com.peerline-manager" 2>/dev/null || true
            launchctl bootstrap "gui/$UID" "$PLIST_DST"
            echo "loaded launchd agent com.peerline-manager"
            ;;
        Linux)
            UNIT_DST="$HOME/.config/systemd/user/peerline-manager.service"
            mkdir -p "$(dirname "$UNIT_DST")"
            cp dist/linux/peerline-manager.service "$UNIT_DST"
            echo "installed unit: $UNIT_DST"
            systemctl --user daemon-reload
            systemctl --user enable --now peerline-manager.service
            echo "loaded systemd user unit peerline-manager.service"
            ;;
        *)
            echo "install: unsupported platform $(uname -s)" >&2
            exit 1
            ;;
    esac
    just status

uninstall:
    #!/usr/bin/env bash
    set -euo pipefail
    case "$(uname -s)" in
        Darwin)
            launchctl bootout "gui/$UID/com.peerline-manager" 2>/dev/null || true
            rm -f "$HOME/Library/LaunchAgents/com.peerline-manager.plist"
            echo "removed launchd agent + plist"
            ;;
        Linux)
            systemctl --user disable --now peerline-manager.service 2>/dev/null || true
            rm -f "$HOME/.config/systemd/user/peerline-manager.service"
            systemctl --user daemon-reload
            echo "removed systemd user unit"
            ;;
        *)
            echo "uninstall: unsupported platform $(uname -s)" >&2
            exit 1
            ;;
    esac
    rm -f "$HOME/.local/bin/peerline-manager" "$HOME/.local/bin/peerline-mon"
    echo "removed binaries"

status:
    #!/usr/bin/env bash
    set -uo pipefail
    echo "Supervisor:"
    case "$(uname -s)" in
        Darwin)
            line=$(launchctl list | awk '$3 == "com.peerline-manager" { print }')
            if [ -z "$line" ]; then
                echo "  com.peerline-manager: not loaded"
            else
                pid=$(echo "$line" | awk '{print $1}')
                stat=$(echo "$line" | awk '{print $2}')
                if [ "$pid" = "-" ]; then
                    echo "  com.peerline-manager: loaded, not running (last exit $stat)"
                else
                    echo "  com.peerline-manager: running (pid $pid)"
                fi
            fi
            LOGS="$HOME/Library/Logs/peerline-manager.log"
            ;;
        Linux)
            if systemctl --user is-enabled peerline-manager.service >/dev/null 2>&1; then
                state=$(systemctl --user is-active peerline-manager.service 2>/dev/null || true)
                echo "  peerline-manager.service: enabled, $state"
            else
                echo "  peerline-manager.service: not installed"
            fi
            LOGS="journalctl --user -u peerline-manager"
            ;;
        *)
            echo "  unsupported platform $(uname -s)"
            LOGS=""
            ;;
    esac
    echo
    echo "Endpoints (defaults):"
    echo "  uds:       /tmp/peerline-manager.sock"
    echo "  ws rpc:    ws://127.0.0.1:6466/"
    echo "  dashboard: http://127.0.0.1:6467/"
    echo
    echo "Paths:"
    echo "  binaries:  $HOME/.local/bin/peerline-manager, $HOME/.local/bin/peerline-mon"
    echo "  logs:      $LOGS"

# Tail the manager logs (macOS log file / Linux journal).
logs:
    #!/usr/bin/env bash
    case "$(uname -s)" in
        Darwin) exec tail -f "$HOME/Library/Logs/peerline-manager.log" ;;
        Linux)  exec journalctl --user -u peerline-manager -f ;;
        *)      echo "logs: unsupported platform $(uname -s)" >&2; exit 1 ;;
    esac

clean:
    cargo clean
