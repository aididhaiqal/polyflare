# PolyFlare Loopback Companion

This small proxy is **only for remotely hosted PolyFlare**. If PolyFlare already runs on the same
computer as Codex, do not run the companion: use PolyFlare's own loopback listener directly.

Codex permits remote-control enrollment through `chatgpt.com`, `chatgpt-staging.com`, or localhost.
The companion gives a remote PolyFlare deployment a local, allowlisted address without changing the
actual destination:

```text
Codex -> http://127.0.0.1:8080/backend-api
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

The default and recommended listener is `127.0.0.1:8080`. The binary rejects non-loopback bind
addresses, non-HTTPS upstreams, localhost upstreams, redirects to another origin, URL credentials,
and upstream URLs containing a path/query/fragment. Its local health endpoint is:

```text
GET http://127.0.0.1:8080/_polyflare-loopback/health
```

For Codex, change only the ChatGPT backend setting:

```toml
chatgpt_base_url = "http://127.0.0.1:8080/backend-api"
```

Provider `base_url` entries can continue pointing directly to the remote PolyFlare host. Do not set
a global `CODEX_API_BASE_URL` for this: that changes renderer/API origin behavior and can break the
origin-bound remote-control enrollment challenge.

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

Stop any local PolyFlare already using port 8080 before installing the companion. A bind conflict
fails closed; the companion never takes over or kills the existing process.
