# Hyperflux loopback companion deployment

**Goal:** Deploy the native Windows PolyFlare loopback companion on Hyperflux and configure Codex
Desktop to use it for authenticated backend Usage and remote control.

**Why planning is required:** This changes an auto-start Windows service and per-user Codex routing
on a remote machine. A bad target or configuration can make Codex backend and remote-control
traffic unavailable.

**Acceptance:** The exact pushed `main` revision builds natively on Hyperflux, the
`PolyFlareLoopback` service runs automatically on `127.0.0.1:8000`, its health reports `ok` against
Ultraflux, Hyperflux Codex configuration uses `http://localhost:8000/backend-api`, and prior local
configuration is backed up before mutation. Do not restart the Ultraflux PolyFlare service.

### Outcome 1: Deliver and build the committed companion

- Work: Push the clean local `main`, clone or fast-forward a native Windows checkout at
  `D:\src\polyflare`, verify the checkout revision, and build `polyflare-loopback.exe` in release
  mode.
- Risks/open questions: Stop if the remote checkout has unrelated changes, the resolved revision
  differs from the pushed revision, or the native Windows build fails. Do not substitute a WSL
  binary.
- Verify: `cargo test -p polyflare-loopback` and
  `cargo build --release -p polyflare-loopback` on Hyperflux.

### Outcome 2: Install the Windows service with rollback

- Work: Confirm port 8000 and service ownership, preserve any existing installed binary or service
  state, then run the repository Windows installer against
  `https://ultraflux.tail6de914.ts.net`. Roll back with the supplied uninstaller if the new service
  cannot become healthy.
- Risks/open questions: Stop rather than replace an unexpected listener or unrelated service.
  Never log or print request headers, credentials, bodies, cookies, or WebSocket frames.
- Verify: `Get-Service PolyFlareLoopback` reports `Running` and `Automatic`, and
  `Invoke-RestMethod http://127.0.0.1:8000/_polyflare-loopback/health` reports `status=ok` and
  `mode=remote-polyflare-loopback`.

### Outcome 3: Configure the Hyperflux Codex user

- Work: Back up `%USERPROFILE%\.codex\config.toml` when present; set top-level
  `chatgpt_base_url = "http://localhost:8000/backend-api"` without disturbing unrelated settings;
  set persistent user `CODEX_API_BASE_URL` to the same value. Preserve the prior environment value
  in deployment evidence for rollback without exposing unrelated environment variables.
- Risks/open questions: Do not launch or terminate Codex remotely if an active session exists;
  configuration applies after the user fully exits and reopens Codex.
- Verify: Read back only the configured TOML key and named user environment variable, then perform
  an unauthenticated local `/backend-api/wham/usage` probe expecting PolyFlare's structured `401`
  response as evidence that the full local-to-remote HTTP path is live.
