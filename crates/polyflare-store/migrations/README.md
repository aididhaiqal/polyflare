# Store migrations

Create one with `scripts/new-migration <snake_case_name>` rather than by hand.

## Versions are UTC timestamps, not the next integer

`20260725143012_reset_credit_expiry.sql`, not `0029_reset_credit_expiry.sql`.

Sequential numbering is a single global namespace, and parallel worktrees cannot allocate from it
safely: two agents each ask "what is the next number?" and both get the same answer. On 2026-07-25
two unrelated migrations both claimed `0025`, which is simultaneously a duplicate-version build
error and a `VersionMismatch` crash-loop on the next deploy. A timestamp encodes *when* a migration
was authored rather than *how many* exist, so two branches cannot collide.

`sqlx` parses the version as the integer before the first underscore, so timestamps sort after the
legacy `0001`–`0028` files. Nothing already applied needs renumbering, and the two schemes coexist.

## Everything in this directory is live

`sqlx::migrate!` embeds every `.sql` here at compile time and applies whatever the database is
missing as a side effect of the process starting. There is no staging area and no dry run: a file
here is a file that will run.

So a work-in-progress migration does not belong in this directory. Keep it outside until it is
ready, or give it an extension `sqlx` ignores. An unfinished `0026_reset_credits.sql` sat here
untracked for several hours on 2026-07-25; any deploy from that checkout would have applied it to
the live database irreversibly.

## Migrations are forward-only and must preserve every existing row

There is no `down`. A migration runs once against real operator data that cannot be regenerated.

- Back-fill new columns from existing ones; never rewrite or drop what is already there.
- Where the source data is ambiguous, leave the new value unset for an operator to resolve. Do not
  guess, and do not merge or delete rows to make a constraint fit — `0025` leaves duplicate Codex
  identities unset for exactly this reason, so a duplicate cannot wedge startup.
- Adding a constraint? Check first whether live data already violates it.

## Deploying

`scripts/polyflare-service` lists pending migrations and snapshots the database with `sqlite3
.backup` before starting, and refuses to start if that backup fails. A linked worktree gets its own
scratch database, so only the primary checkout migrates the live store.
