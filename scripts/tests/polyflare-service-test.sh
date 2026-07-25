#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_SCRIPT="$(cd -- "$SCRIPT_DIR/.." && pwd)/polyflare-service"
TEST_DIR="$(mktemp -d)"
FAKE_BUILD_BINARY="$TEST_DIR/build/polyflare"
INSTALLED_BINARY="$TEST_DIR/bin/polyflare"
STATE_DIR="$TEST_DIR/state"

cleanup() {
  POLYFLARE_SERVICE_BUILD_BINARY="$FAKE_BUILD_BINARY" \
    POLYFLARE_SERVICE_BINARY="$INSTALLED_BINARY" \
    POLYFLARE_SERVICE_STATE_DIR="$STATE_DIR" \
    POLYFLARE_SERVICE_PORT="$PORT" \
    POLYFLARE_SERVICE_URL="http://127.0.0.1:$PORT" \
    "$SERVICE_SCRIPT" stop >/dev/null 2>&1 || true
  rm -rf "$TEST_DIR"
}

PORT="$(
  python3 - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
)"
trap cleanup EXIT

mkdir -p "$(dirname "$FAKE_BUILD_BINARY")"
cat >"$FAKE_BUILD_BINARY" <<'PY'
#!/usr/bin/env python3
import http.server
import os
import signal
import sys

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

port = int(os.environ["POLYFLARE_SERVICE_PORT"])
server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(0))
server.serve_forever()
PY
chmod +x "$FAKE_BUILD_BINARY"

export POLYFLARE_SERVICE_BUILD_BINARY="$FAKE_BUILD_BINARY"
export POLYFLARE_SERVICE_BINARY="$INSTALLED_BINARY"
export POLYFLARE_SERVICE_STATE_DIR="$STATE_DIR"
export POLYFLARE_SERVICE_PORT="$PORT"
export POLYFLARE_SERVICE_URL="http://127.0.0.1:$PORT"
export POLYFLARE_SERVICE_SKIP_BUILD=1
export POLYFLARE_SERVICE_TEST_ALLOW_ANY_PROCESS=1
export POLYFLARE_SERVICE_START_TIMEOUT_SECS=5
export POLYFLARE_SERVICE_STOP_TIMEOUT_SECS=5
export POLYFLARE_SERVICE_USE_LAUNCHD=0

"$SERVICE_SCRIPT" start >/dev/null
[[ -x "$INSTALLED_BINARY" ]]
cmp -s "$FAKE_BUILD_BINARY" "$INSTALLED_BINARY"
"$SERVICE_SCRIPT" status >/dev/null
first_pid="$(tr -d '[:space:]' <"$STATE_DIR/polyflare.pid")"
kill -0 "$first_pid"

"$SERVICE_SCRIPT" restart >/dev/null
second_pid="$(tr -d '[:space:]' <"$STATE_DIR/polyflare.pid")"
kill -0 "$second_pid"
[[ "$first_pid" != "$second_pid" ]]
curl --fail --silent --max-time 1 "http://127.0.0.1:$PORT/dashboard" >/dev/null

"$SERVICE_SCRIPT" stop >/dev/null
[[ ! -e "$STATE_DIR/polyflare.pid" ]]
if kill -0 "$second_pid" 2>/dev/null; then
  printf 'polyflare-service-test: process %s survived stop\n' "$second_pid" >&2
  exit 1
fi
if POLYFLARE_SERVICE_PORT=invalid "$SERVICE_SCRIPT" status >/dev/null 2>&1; then
  printf 'polyflare-service-test: invalid service port was accepted\n' >&2
  exit 1
fi

printf 'polyflare-service lifecycle test passed\n'
