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

# --- Migration guard: pending migrations are named, and the DB is copied before they apply. -------
MIG_DATA_DIR="$TEST_DIR/migration-guard"
mkdir -p "$MIG_DATA_DIR"
if command -v sqlite3 >/dev/null 2>&1; then
  # A store that has applied 0001 only, while the repo carries far more.
  sqlite3 "$MIG_DATA_DIR/store.db" \
    'CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY); INSERT INTO _sqlx_migrations VALUES (1);'

  guard_output="$(POLYFLARE_DATA_DIR="$MIG_DATA_DIR" "$SERVICE_SCRIPT" start 2>&1)"
  POLYFLARE_DATA_DIR="$MIG_DATA_DIR" "$SERVICE_SCRIPT" stop >/dev/null 2>&1 || true

  # It must NAME what is about to change the database, not just that something will.
  grep -q "pending migration" <<<"$guard_output" ||
    { printf 'expected a pending-migration report, got:\n%s\n' "$guard_output" >&2; exit 1; }
  grep -q "0025_provider_upstream_identity.sql" <<<"$guard_output" ||
    { printf 'pending list must name each file, got:\n%s\n' "$guard_output" >&2; exit 1; }
  # An already-applied migration is not pending.
  if grep -q "0001_accounts_and_usage.sql" <<<"$guard_output"; then
    printf 'applied migrations must not be listed as pending:\n%s\n' "$guard_output" >&2
    exit 1
  fi

  # And a recoverable copy exists before anything runs.
  backup_count="$(find "$MIG_DATA_DIR/backups" -name 'store-pre-migration-*.db' 2>/dev/null | wc -l)"
  (( backup_count == 1 )) ||
    { printf 'expected exactly one pre-migration backup, found %s\n' "$backup_count" >&2; exit 1; }

  # A database already at the newest version needs neither report nor backup.
  UPTODATE_DIR="$TEST_DIR/migration-uptodate"
  mkdir -p "$UPTODATE_DIR"
  versions="$(
    for f in "$(cd -- "$SCRIPT_DIR/../.." && pwd)"/crates/polyflare-store/migrations/*.sql; do
      base="$(basename -- "$f")"
      printf '(%s),' "$((10#${base%%_*}))"
    done | sed 's/,$//'
  )"
  sqlite3 "$UPTODATE_DIR/store.db" \
    "CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY); INSERT INTO _sqlx_migrations VALUES $versions;"
  uptodate_output="$(POLYFLARE_DATA_DIR="$UPTODATE_DIR" "$SERVICE_SCRIPT" start 2>&1)"
  POLYFLARE_DATA_DIR="$UPTODATE_DIR" "$SERVICE_SCRIPT" stop >/dev/null 2>&1 || true
  if grep -q "pending migration" <<<"$uptodate_output"; then
    printf 'an up-to-date DB must report nothing pending:\n%s\n' "$uptodate_output" >&2
    exit 1
  fi
  if [[ -d "$UPTODATE_DIR/backups" ]]; then
    printf 'an up-to-date DB must not be backed up\n' >&2
    exit 1
  fi
fi

# --- A linked worktree must never default to the shared live store. -------------------------------
# `status` exits non-zero when nothing is running; only the announced target matters here.
worktree_target="$({ "$SERVICE_SCRIPT" status 2>&1 || true; } | sed -n '1p')"
if git -C "$(cd -- "$SCRIPT_DIR/../.." && pwd)" rev-parse --absolute-git-dir >/dev/null 2>&1; then
  repo_root="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
  git_dir="$(git -C "$repo_root" rev-parse --absolute-git-dir)"
  common_dir="$(cd "$repo_root" && cd "$(git rev-parse --git-common-dir)" && pwd)"
  if [[ "$git_dir" != "$common_dir" ]]; then
    grep -q "polyflare-worktree" <<<"$worktree_target" ||
      { printf 'a linked worktree must use an isolated data dir, got: %s\n' "$worktree_target" >&2; exit 1; }
    if grep -q "$HOME/.polyflare " <<<"$worktree_target"; then
      printf 'a linked worktree must NOT target the live store: %s\n' "$worktree_target" >&2
      exit 1
    fi
  fi
fi

printf 'polyflare-service migration/worktree guards OK\n'
