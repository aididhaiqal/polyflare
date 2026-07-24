# Reimport latest codex-lb credentials

**Goal:** Refresh PolyFlare account credentials from the latest local codex-lb store without exposing secrets or changing account routing configuration.
**Why planning is required:** This is a security-sensitive credential migration that mutates the live PolyFlare data store.
**Acceptance:** Use `/Users/wmaididhaiqal/.codex-lb/store.db` and its matching Fernet key only after confirming codex-lb is not holding the database open; target `/Users/wmaididhaiqal/.polyflare/store.db`; create a timestamped recoverable backup of the target database and key before mutation; require a successful dry run with five source accounts and no decryption/schema errors; run the refresh-existing import only if those checks pass; preserve aliases, pools, and routing; verify redacted account counts and token-health metadata afterward; never print token values. Abort on source/target identity drift, dry-run failure, unexpected account count, or backup failure.

### Outcome 1: Proven source, target, and recovery point
- Work: Confirm source and target paths, file ownership/permissions, database activity, redacted row counts, and make timestamped copies of the PolyFlare store and encryption key.
- Risks/open questions: A running PolyFlare process may retain cached tokens until restarted; do not stop a user-owned process implicitly.
- Verify: `stat`, `lsof`, and redacted SQLite count checks

### Outcome 2: Refresh existing credentials safely
- Work: Run `polyflare accounts import` first with `--dry-run --refresh-existing`, then repeat without `--dry-run` only when the preview succeeds and reports the expected five-account scope.
- Verify: `target/debug/polyflare accounts import --from /Users/wmaididhaiqal/.codex-lb/store.db --fernet-key /Users/wmaididhaiqal/.codex-lb/encryption.key --refresh-existing`

### Outcome 3: Post-import integrity verification
- Work: Recheck account and usage counts plus redacted dashboard token-health/status metadata; report that the running service needs a restart before serving with refreshed cached credentials.
- Verify: read-only SQLite counts and `GET /api/accounts`
