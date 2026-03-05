#!/bin/bash
# Start Codex with SGP (Agentex) as the model provider.
# Usage: ./start-codex-sgp.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROXY_BIN="$SCRIPT_DIR/codex-rs/target/debug/codex-sgp-proxy"
CODEX_BIN="$SCRIPT_DIR/codex-rs/target/debug/codex"
PROXY_PORT=8090

AGENTEX_URL="https://agentex.dev-sgp.scale.com"
AGENT_ID="langgraph"
ACCOUNT_ID="${SGP_ACCOUNT_ID:-68754be7ac3f41b875f912a1}"
API_KEY="${SGP_API_KEY:-faae4d17791afc04a6628386f4c7aa9f}"

# Kill any existing proxy on the port.
if lsof -ti :"$PROXY_PORT" >/dev/null 2>&1; then
    echo "Stopping existing proxy on port $PROXY_PORT..."
    kill $(lsof -ti :"$PROXY_PORT") 2>/dev/null || true
    sleep 1
fi

# Start the proxy in the background.
echo "$API_KEY" | "$PROXY_BIN" \
    --agentex-url "$AGENTEX_URL" \
    --agent-id "$AGENT_ID" \
    --account-id "$ACCOUNT_ID" \
    --port "$PROXY_PORT" \
    --task-lifecycle per-session &
PROXY_PID=$!

# Wait for the proxy to be ready.
for i in $(seq 1 10); do
    if curl -s -o /dev/null "http://127.0.0.1:$PROXY_PORT/shutdown" 2>/dev/null; then
        break
    fi
    sleep 0.3
done

echo "Proxy running (PID $PROXY_PID) on port $PROXY_PORT"

# Clean up proxy when codex exits.
trap "kill $PROXY_PID 2>/dev/null; echo 'Proxy stopped.'" EXIT

# Start codex.
SGP_DUMMY_KEY=dummy exec "$CODEX_BIN" "$@"
