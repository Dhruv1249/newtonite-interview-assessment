#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PORT="${PORT:-8088}"
TARGET_URL="http://127.0.0.1:${PORT}"

echo ">>> Building FlashDrop release binary & 10k customer example..."
cd "${ROOT_DIR}"
cargo build --release --bin flashdrop --example simulate_10k_customers

echo ">>> Launching FlashDrop server on port ${PORT}..."
PORT="${PORT}" "${ROOT_DIR}/target/release/flashdrop" &
SERVER_PID=$!

cleanup() {
    echo ""
    echo ">>> Stopping FlashDrop server (PID: ${SERVER_PID})..."
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
    echo ">>> Done."
}
trap cleanup EXIT

echo ">>> Awaiting server health check at ${TARGET_URL}/health..."
for i in {1..30}; do
    if curl -s "${TARGET_URL}/health" >/dev/null 2>&1; then
        echo ">>> Server is UP and healthy!"
        break
    fi
    sleep 0.1
done

echo ">>> Starting 10,000 customer concurrent load simulation..."
TARGET_URL="${TARGET_URL}" "${ROOT_DIR}/target/release/examples/simulate_10k_customers"

echo ">>> 10,000 customer simulation completed successfully!"
