# Running PolyFlare non-locally (client API keys)

PolyFlare's proxy surface (`POST /responses`, `/v1/messages`, and their `/{pool}/…` variants) can
be authenticated with client API keys (D18). This note is the short operator path: create a key,
present it, and what changes when you bind off loopback. It does not cover the `/api/*` dashboard
API — that has always been its own `POLYFLARE_ADMIN_TOKEN` gate (`require_admin`), unrelated to
these client keys.

## The default posture

At startup, `polyflare serve` resolves ONE of three postures for the proxy surface, from whatever
keys already exist plus `POLYFLARE_BIND`:

1. **Any client key exists** ⇒ enforced, on ANY bind (including `127.0.0.1`). Once you've created a
   key, every proxy request needs one.
2. **No keys + bound to loopback** (`127.0.0.1`, `::1`, the default `POLYFLARE_BIND`) ⇒ open,
   exactly like every PolyFlare deployment before D18 — nothing changes for local dev.
3. **No keys + bound off loopback** (e.g. `POLYFLARE_BIND=0.0.0.0:8080`, a LAN/public IP) ⇒ the
   process **refuses to start**, unless you explicitly set
   `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1`.

## ⚠ BREAKING CHANGE for existing non-local deployments

If you already run PolyFlare bound to `0.0.0.0` or a LAN/public IP **with no client keys
configured**, upgrading to this version means `polyflare serve` will **refuse to start** the next
time you launch it — that combination used to run open (anonymous quota drain to anyone who could
reach the port), and it no longer boots silently into that state.

To keep running, do ONE of:

- **(recommended) Create a key** — see below — then restart. The proxy surface becomes
  key-enforced; nothing else about your deployment changes.
- **Explicitly opt back into the old open behavior**: set
  `POLYFLARE_ALLOW_UNAUTHENTICATED_REMOTE=1` and restart. PolyFlare will start, but it emits a
  loud startup warning (`tracing::warn!`) every time, and the proxy remains genuinely
  unauthenticated on a non-loopback bind — **anyone who can reach that address/port can spend your
  account quota**. Only use this as a stopgap while you set up keys, or if you already have your
  own auth layer (e.g. a fronting reverse proxy — see the caveat below) in front of PolyFlare.

## (a) Create a key

```
polyflare keys create --label my-caller
```

This prints the **raw key exactly once**, to stdout — e.g. `sk-pf-<43 base64url chars>`. Save it
now; PolyFlare never stores the plaintext (only its sha256 hash) and cannot show it to you again.
Losing it means creating a new key and revoking the old one.

Other key management:

```
polyflare keys list             # id / prefix / label / enabled / created / last_used — never a raw key
polyflare keys revoke --id <id> # disable a key (kept for audit history, not deleted)
```

## (b) Present the key

Every proxied request needs the key as a Bearer token:

```
curl -X POST http://your-host:8080/responses \
  -H "Authorization: Bearer sk-pf-<your-key>" \
  -H "Content-Type: application/json" \
  -d '{"model": "gpt-5.6-sol", "input": "hi"}'
```

A missing, malformed, unknown, or revoked key gets `401`. This does not apply to the GET-426
WS-fallback handshake shim (a WS-capable client's keyless GET probe still degrades to `426`, never
`401`), `/dashboard` static assets, or `/api/*` (its own, separate `require_admin` gate) — none of
those are covered by the client-key layer.

## (c) Bind non-locally

```
POLYFLARE_BIND=0.0.0.0:8080 polyflare serve
```

Create at least one key first (step (a)) so this doesn't hit the refuse-to-start guard above.

## ⚠ The posture is resolved at STARTUP, not live

`enforce_client_keys` is decided ONCE, when `polyflare serve` boots, from whatever keys exist and
the bind address at that moment — it is **never re-evaluated per request**. Concretely:

- If you run `polyflare keys create` while a loopback (open-posture) server is **already
  running**, that server keeps serving the proxy surface **unauthenticated** — the new key exists
  in the store, but the running process doesn't know it changed anything.
- **You must restart PolyFlare (`polyflare serve`) for the new posture (enforcement) to take
  effect.**

This is deliberate — resolving posture per-request would mean a key created mid-request-storm
could flip enforcement mid-flight in a way that's hard to reason about; a startup-time property is
easier to audit ("what does this running process require right now") than a live one.

## Fronting-proxy caveat

If PolyFlare sits behind a reverse proxy (nginx, an ALB, etc.), the socket peer PolyFlare sees is
the proxy's address — typically `127.0.0.1` — even though the real callers are remote. The
bind-address posture has no way to see through that: it will conclude "loopback bind, no keys,
stay open" even though the effective deployment is public. **If you front PolyFlare with a proxy,
opt into key enforcement explicitly** (`polyflare keys create`) rather than relying on the
bind-address default to catch this case for you.
