# Hyperflux loopback companion deployment

**Goal:** Deploy the PolyFlare loopback companion on Hyperflux and configure Codex Desktop to use
it for authenticated backend Usage and remote control.

**Why planning is required:** This changes an auto-start Docker-managed container and per-user
Codex routing on a remote machine. A bad target or configuration can make Codex backend and
remote-control traffic unavailable.

**Acceptance:** A container image built from the exact pushed `main` revision runs automatically
through Hyperflux Docker Desktop, publishes only `127.0.0.1:8000`, reports healthy against
Ultraflux, and survives a container restart. Hyperflux Codex configuration uses
`http://localhost:8000/backend-api`, and prior local configuration is backed up before mutation.
Do not restart the Ultraflux PolyFlare service or weaken Smart App Control.

**Approved disposition (2026-08-04):** Native Windows compilation remains blocked by enforced
Smart App Control even after Developer Mode was enabled (`os error 4551` on Cargo-generated build
scripts). The user approved replacing the native Windows-service outcome with an existing Docker
Desktop deployment. The failed native build artifacts are not deployed.

### Outcome 1: Deliver and build the committed container

- Work: Add a reproducible, non-root companion image and build it from the clean Hyperflux checkout
  at `D:\src\polyflare`. Keep the companion itself loopback-bound inside the container and use a
  container-local TCP relay solely to publish the container port. Publish the host port only on
  `127.0.0.1:8000`.
- Risks/open questions: Stop if the remote checkout has unrelated changes, the resolved revision
  differs from the pushed revision, Docker Desktop is unavailable, or port 8000 has an unexpected
  listener. Do not expose the host port on every interface.
- Verify: `cargo test -p polyflare-loopback`, build the image from the committed revision, inspect
  its non-root user and published-port binding, and run its image health check.

### Outcome 2: Install the restart-managed container with rollback

- Work: Confirm port 8000 and container ownership, preserve any existing companion container state,
  then install the committed image against `https://ultraflux.tail6de914.ts.net` with Docker's
  `unless-stopped` restart policy. Roll back by restoring the prior container, or remove the new
  container when none existed, if it cannot become healthy.
- Risks/open questions: Stop rather than replace an unexpected listener or unrelated service.
  Never log or print request headers, credentials, bodies, cookies, or WebSocket frames.
- Verify: Docker reports the container running with `unless-stopped`, its only host publication is
  `127.0.0.1:8000`, and `Invoke-RestMethod
  http://127.0.0.1:8000/_polyflare-loopback/health` reports `status=ok` and
  `mode=remote-polyflare-loopback`. Restart the container once and repeat the health probe.

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
