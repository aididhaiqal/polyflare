# Agent instructions — PolyFlare

PolyFlare is a **live production service**, not just a repo. A launchd agent
(`~/Library/LaunchAgents/com.polyflare.server.plist`) runs `~/.local/bin/polyflare serve` on
127.0.0.1:8080, and the user's Codex CLI, ChatGPT app, and Claude Code all route real work through
it against the live SQLite store at `~/.polyflare/store.db`. Every restart severs in-flight
conversation turns.

## Deploying (read before touching the running service)

**Never** `cp` a binary into `~/.local/bin/polyflare`, and never `launchctl kickstart` the service,
outside the procedure below. Ad-hoc deploys on 2026-07-25 destroyed the user's conversation
history (see "Why" below) and twice came one build away from a boot crash-loop.

The procedure, in order — no step is optional:

1. **Commit first.** The deployed binary must be reproducible from git. Never build a service
   binary from an uncommitted tree.
2. **Full suite green from a clean tree:** `cargo test --workspace`. If the working tree has
   unrelated WIP that breaks the build, deploy from a worktree at the commit instead:
   `git worktree add <tmpdir> <commit>` with a shared `CARGO_TARGET_DIR`.
   Note `scripts/polyflare-service` refuses a launchd restart from a linked worktree, because
   launchd re-execs from its plist and would bring the service back on the live store while the
   worktree carries unmerged code. When a worktree deploy is genuinely what you want, say so:
   `POLYFLARE_ALLOW_WORKTREE_LIVE_DEPLOY=1`. Outside launchd, a worktree gets its own scratch
   store and never touches `~/.polyflare`.
3. **Keep a rollback binary:** `cp ~/.local/bin/polyflare ~/.local/bin/polyflare.bak-<short-sha>`.
4. **Restart only when idle.** Wait for zero in-flight work before kickstarting — poll
   `curl -s localhost:8080/metrics | grep polyflare_lease_inflight` for `0` **and** no
   `/responses` row in `request_log` for ~25s. Restarting mid-turn loses the user's messages.
5. **Verify after boot:** process is up, `curl localhost:8080/` answers, and
   `SELECT MAX(version) FROM _sqlx_migrations` is what you expect.

## Migrations — the unforgiving part

The live DB records a **sha384 checksum** of every applied migration file. If the file later
changes by even one byte, the next boot dies with `Migrate(VersionMismatch(N))` and the service
crash-loops.

- **Never let an uncommitted-tree binary apply a migration.** Commit the migration file *before*
  the binary that applies it ever runs. If you polish the SQL afterwards, you have corrupted the
  live DB's reproducibility and someone has to recover the applied bytes by hand.
- **Never reuse a migration number** that another in-flight branch or an untracked draft already
  claims — duplicate versions fail the build and mismatch the ledger. Use
  `scripts/new-migration <name>`, which stamps a UTC timestamp version instead of the next integer,
  so two branches cannot collide. See `crates/polyflare-store/migrations/README.md`.
- **Nothing unfinished goes in `migrations/`.** `sqlx::migrate!` embeds every `.sql` there and
  applies it on the next boot; there is no staging area. `scripts/polyflare-service` lists what is
  pending and snapshots the DB before starting, and refuses to start if that snapshot fails.
- Migrations must be **additive** (`ADD COLUMN` with defaults, new tables, rename-to-legacy).
  A migration that renames or drops a column used by committed code breaks the service the moment
  it ships without its matching code.

## Why these rules exist (2026-07-25)

Nine unpipelined self-deploys in ninety minutes each severed every live conversation turn. The
Codex client only persists a user's message to its transcript when a turn *completes*, so severed
turns silently discarded the user's typed messages — seven real threads lost messages
permanently, unrecoverable. Separately, two migrations were applied from uncommitted trees; one
source file then vanished entirely and had to be recovered by brute-forcing SQL bytes out of the
deployed binary until the checksum matched.

## Also worth knowing

- **Content-free is inviolable.** Conversation text, prompts, reasoning summaries, and error
  `message` fields must never be logged, persisted, or copied anywhere. Only structural facts
  (status, model, ids, counts, hashed keys) may be recorded. New telemetry = counters, not text.
- The relay's pump (`ws_relay/pump.rs`) is wedge-sacred: its recovery paths were root-caused from
  live incidents. Add behavior behind an explicit gate; do not restructure the loop casually.
- Runtime knobs live in the launchd plist's `EnvironmentVariables` (e.g.
  `POLYFLARE_ADMISSION_WAIT_TIMEOUT_MS=90000`, raised deliberately — a shorter window rejected
  legitimate queued turns). A `kickstart` preserves them; `bootout` + `bootstrap` is needed only
  when the plist itself changes, and `bootout` is asynchronous, so retry `bootstrap` if it races.
