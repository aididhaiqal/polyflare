# PolyFlare Loopback Companion

This small proxy is **only for remotely hosted PolyFlare**. If PolyFlare already runs on the same
computer as Codex, do not run the companion: use PolyFlare's own loopback listener directly.

Codex permits remote-control enrollment through `chatgpt.com`, `chatgpt-staging.com`, or localhost.
The companion gives a remote PolyFlare deployment a local, allowlisted address without changing the
actual destination:

```text
Codex -> http://localhost:8000/backend-api
      -> polyflare-loopback
      -> https/wss://remote-polyflare-host/backend-api
```

## Run manually

Build and validate the remote origin first:

```sh
cargo build --release -p polyflare-loopback
target/release/polyflare-loopback \
  --upstream-origin https://ultraflux.example.ts.net \
  --check-config
```

Then run it:

```sh
target/release/polyflare-loopback \
  --upstream-origin https://ultraflux.example.ts.net
```

The default and recommended listener is `127.0.0.1:8000`. The binary rejects non-loopback bind
addresses, non-HTTPS upstreams, localhost upstreams, redirects to another origin, URL credentials,
and upstream URLs containing a path/query/fragment. `--check-config` also performs an HTTPS
readiness request to the configured origin. The health endpoint checks both the local service and
that remote HTTPS leg:

```text
GET http://127.0.0.1:8000/_polyflare-loopback/health
```

It returns `200` with `status: "ok"` when the remote origin responds (any non-redirect HTTP status
is sufficient), or `503` with `status: "degraded"` when it cannot be reached.

### Docker Desktop on Windows

When Windows Smart App Control blocks Cargo-generated build programs, keep it enabled and build the
Linux companion inside Docker Desktop instead:

```powershell
$branch = git branch --show-current
git fetch origin +refs/heads/main:refs/remotes/origin/main
$revision = git rev-parse HEAD
$remoteRevision = git rev-parse origin/main
if ($branch -ne "main") { throw "Build the Hyperflux deployment from main" }
if ($revision -ne $remoteRevision) { throw "HEAD must match the pushed origin/main revision" }
if (git status --porcelain) { throw "Build only from a clean checkout" }
docker build --file crates/polyflare-loopback/Dockerfile `
  --build-arg SOURCE_REVISION=$revision `
  --tag "polyflare-loopback:$revision" .
docker run --detach --name polyflare-loopback --restart unless-stopped `
  --publish 127.0.0.1:8000:8000 `
  --env POLYFLARE_LOOPBACK_UPSTREAM_ORIGIN=https://ultraflux.tail6de914.ts.net `
  "polyflare-loopback:$revision"
docker image inspect "polyflare-loopback:$revision" `
  --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}'
```

The host publication must remain exactly `127.0.0.1:8000:8000`; do not publish the companion on
every host interface. The container runs without root privileges. Inside it, the Rust companion
still binds only to loopback, while a container-local TCP relay makes the port publishable through
Docker. Docker Desktop must start at login for `--restart unless-stopped` to take effect after a
Windows reboot.

Back up the Windows Codex configuration before setting its top-level ChatGPT backend key:

```powershell
$configPath = Join-Path $env:USERPROFILE ".codex\config.toml"
if (Test-Path $configPath) {
  $backupPath = "$configPath.loopback-backup-$(Get-Date -Format yyyyMMddHHmmss)"
  Copy-Item $configPath $backupPath
}
```

Set this top-level entry in `config.toml`, preserving all unrelated settings:

```toml
chatgpt_base_url = "http://localhost:8000/backend-api"
```

Then set and read back the renderer's persistent per-user environment value:

```powershell
$previousApiBaseUrl = [Environment]::GetEnvironmentVariable("CODEX_API_BASE_URL", "User")
Write-Output "Previous CODEX_API_BASE_URL: $previousApiBaseUrl"
[Environment]::SetEnvironmentVariable(
  "CODEX_API_BASE_URL",
  "http://localhost:8000/backend-api",
  "User"
)
[Environment]::GetEnvironmentVariable("CODEX_API_BASE_URL", "User")
Select-String -Path $configPath -Pattern '^chatgpt_base_url\s*='
```

Fully exit and reopen Codex Desktop after changing the configuration or environment. The
`launchctl` commands below apply only to macOS.

For Codex, change only the ChatGPT backend setting:

```toml
chatgpt_base_url = "http://localhost:8000/backend-api"
```

The desktop renderer uses a separate base for Usage and other ChatGPT backend reads. Set it in the
per-user launchd environment before starting Codex, then fully quit and reopen the app:

```sh
launchctl setenv CODEX_API_BASE_URL http://localhost:8000/backend-api
launchctl getenv CODEX_API_BASE_URL
```

Codex Desktop only permits attaching the signed-in ChatGPT authorization to `localhost` without a
port or exactly `localhost:8000`. Use the literal `localhost:8000` here: `127.0.0.1` or another port
is rejected before the request reaches the companion. The companion still binds only to loopback.
`launchctl setenv` applies to subsequently launched processes in the current login session; arrange
to run it at login if the setting must survive logout or reboot.

Provider `base_url` entries can continue pointing directly to the remote PolyFlare host. Model
`/responses` traffic therefore goes directly to remote PolyFlare, while the backend HTTP and
remote-control WebSocket paths traverse the companion and then PolyFlare. The companion never
bypasses PolyFlare.

## Behavior and safety

- HTTP request and response bodies are streamed; SSE is not buffered.
- WebSocket upstream handshakes complete before the local upgrade is accepted.
- HTTP connections are pooled and recreated normally after network loss. Interrupted WebSocket
  sessions are not replayed or silently reconnected because replaying frames can corrupt protocol
  state; Codex reconnects at its own session boundary.
- Upstream failures return `502`; there is no OpenAI or alternate-provider fallback.
- Logs contain only lifecycle/status fields. Headers, cookies, credentials, bodies, query strings,
  and WebSocket frames are never logged.

## Optional persistent startup

Inert packaging templates and installers are under `packaging/`. Nothing is installed by a build.

- macOS: `packaging/macos/install.sh <binary> <https-origin>` installs and loads a per-user
  LaunchAgent. `uninstall.sh` unloads and removes only that companion.
- Windows (run PowerShell as Administrator): `packaging/windows/install.ps1 -BinaryPath <exe>
  -UpstreamOrigin <https-origin>` installs and starts an auto-start Windows service.

Stop any local service already using port 8000 before installing the companion. A bind conflict
fails closed; the companion never takes over or kills the existing process.
