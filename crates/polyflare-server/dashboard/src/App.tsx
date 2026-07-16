import { useCallback, useEffect, useState } from "react";
import { api, Account, Pool, RequestRow, Window } from "./api";

const REFRESH_MS = 15_000;

/** Format an absolute unix-epoch (seconds) reset time as a live countdown relative to `nowMs`. */
function countdown(resetAt: number | null, nowMs: number): string {
  if (resetAt == null) return "—";
  const secs = Math.round(resetAt - nowMs / 1000);
  if (secs <= 0) return "due";
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

/** Absolute local time for a unix-epoch (seconds), for the countdown's tooltip. */
function absTime(resetAt: number | null): string {
  return resetAt == null ? "not reported" : new Date(resetAt * 1000).toLocaleString();
}

function poolLabel(pool: string | null): string {
  return pool ?? "unpooled";
}

function StatusBadge({ status }: { status: string }) {
  return <span className={`badge badge-${status}`}>{status.replace(/_/g, " ")}</span>;
}

function UsageBar({ pct, stale }: { pct: number; stale: boolean }) {
  const clamped = Math.max(0, Math.min(100, pct));
  const tone = clamped >= 100 ? "full" : clamped >= 80 ? "high" : "ok";
  return (
    <div className={`usage${stale ? " usage-stale" : ""}`} title={stale ? `${pct.toFixed(1)}% (stale)` : `${pct.toFixed(1)}%`}>
      <div className={`usage-fill usage-${tone}`} style={{ width: `${clamped}%` }} />
      <span className="usage-label">{pct.toFixed(0)}%</span>
    </div>
  );
}

/** A window's reset time as a live countdown pill; dimmed + tagged when the window's data is stale
 *  (upstream stopped refreshing it), and showing `absent` when the window isn't reported at all. */
function WindowReset({ w, now, absent }: { w: Window | null; now: number; absent: string }) {
  if (!w || w.reset_at == null) return <span className="reset reset-none">{absent}</span>;
  const label = countdown(w.reset_at, now);
  const cls = ["reset", label === "due" ? "reset-due" : "", w.stale ? "reset-stale" : ""]
    .filter(Boolean)
    .join(" ");
  const title = w.stale ? `stale — last refreshed data; resets ${absTime(w.reset_at)}` : absTime(w.reset_at);
  return (
    <span className={cls} title={title}>
      {label}
      {w.stale && <span className="stale-tag">stale</span>}
    </span>
  );
}

function PoolCards({ pools }: { pools: Pool[] }) {
  if (pools.length === 0) return null;
  return (
    <div className="pool-cards">
      {pools.map((p) => (
        <div className="pool-card" key={poolLabel(p.pool)}>
          <div className="pool-name">{poolLabel(p.pool)}</div>
          <div className="pool-stat">
            <strong>{p.active}</strong>
            <span>/ {p.accounts} active</span>
          </div>
          {p.pool && <code className="pool-slug">/{p.pool}/responses</code>}
        </div>
      ))}
    </div>
  );
}

/** Inline pool editor: shows the current pool (click to edit); type an existing or NEW name to
 *  assign/create, or clear it to unpool. Submits a PATCH and refreshes on success. */
function PoolEditor({
  account,
  onSaved,
}: {
  account: Account;
  onSaved: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(account.pool ?? "");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(false);

  async function save(pool: string | null) {
    setBusy(true);
    setErr(false);
    try {
      await api.patchAccount(account.id, { pool });
      setEditing(false);
      onSaved();
    } catch {
      setErr(true);
    } finally {
      setBusy(false);
    }
  }

  if (!editing) {
    return (
      <button
        className="cell-edit"
        title="Edit pool"
        onClick={() => {
          setValue(account.pool ?? "");
          setEditing(true);
        }}
      >
        {account.pool ? <span className="pill">{account.pool}</span> : <span className="muted">— set pool</span>}
      </button>
    );
  }
  return (
    <span className={`pool-edit${err ? " has-err" : ""}`}>
      <input
        list="pf-pool-names"
        className="pool-input"
        value={value}
        autoFocus
        disabled={busy}
        placeholder="pool name"
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") save(value.trim() || null);
          if (e.key === "Escape") setEditing(false);
        }}
      />
      <button className="mini" disabled={busy} title="Save" onClick={() => save(value.trim() || null)}>
        ✓
      </button>
      <button className="mini" disabled={busy} title="Cancel" onClick={() => setEditing(false)}>
        ✕
      </button>
    </span>
  );
}

/** Routing-policy dropdown; PATCHes on change. */
function RoutingPolicySelect({ account, onSaved }: { account: Account; onSaved: () => void }) {
  const [busy, setBusy] = useState(false);
  async function change(rp: string) {
    setBusy(true);
    try {
      await api.patchAccount(account.id, { routing_policy: rp });
      onSaved();
    } finally {
      setBusy(false);
    }
  }
  return (
    <select
      className="policy-select"
      value={account.routing_policy}
      disabled={busy}
      onChange={(e) => change(e.target.value)}
      title="Routing policy"
    >
      <option value="normal">normal</option>
      <option value="burn_first">burn first</option>
      <option value="preserve">preserve</option>
    </select>
  );
}

/** Status badge + a pause/resume toggle (only for accounts in a togglable active/paused state — the
 *  rate-limit / reauth statuses are owned by the server and shown read-only). */
function StatusCell({ account, onSaved }: { account: Account; onSaved: () => void }) {
  const [busy, setBusy] = useState(false);
  const paused = account.status === "paused";
  const togglable = account.status === "active" || account.status === "paused";
  async function toggle() {
    setBusy(true);
    try {
      await api.patchAccount(account.id, { status: paused ? "active" : "paused" });
      onSaved();
    } finally {
      setBusy(false);
    }
  }
  return (
    <span className="status-cell">
      <StatusBadge status={account.status} />
      {togglable && (
        <button
          className="mini"
          disabled={busy}
          onClick={toggle}
          title={paused ? "Resume (make eligible)" : "Pause (hold out of rotation)"}
        >
          {paused ? "▶" : "⏸"}
        </button>
      )}
    </span>
  );
}

function AccountsTable({
  accounts,
  now,
  onMutate,
}: {
  accounts: Account[];
  now: number;
  onMutate: () => void;
}) {
  // Existing pool names power the input's datalist (pick an existing pool or type a new one).
  const poolNames = Array.from(
    new Set(accounts.map((a) => a.pool).filter((p): p is string => !!p)),
  ).sort();
  return (
    <table className="grid">
      <datalist id="pf-pool-names">
        {poolNames.map((p) => (
          <option key={p} value={p} />
        ))}
      </datalist>
      <thead>
        <tr>
          <th>Account</th>
          <th>Pool</th>
          <th>Status</th>
          <th>Policy</th>
          <th>Plan</th>
          <th>Weekly</th>
          <th>Weekly reset</th>
          <th>5h reset</th>
        </tr>
      </thead>
      <tbody>
        {accounts.map((a) => (
          <tr key={a.id}>
            <td>
              <div className="acct-email">{a.email || a.id}</div>
              <div className="acct-id">{a.id}</div>
            </td>
            <td>
              <PoolEditor account={a} onSaved={onMutate} />
            </td>
            <td>
              <StatusCell account={a} onSaved={onMutate} />
            </td>
            <td>
              <RoutingPolicySelect account={a} onSaved={onMutate} />
            </td>
            <td className="muted">{a.plan_type}</td>
            <td>
              {a.weekly ? (
                <UsageBar pct={a.weekly.used_percent} stale={a.weekly.stale} />
              ) : (
                <span className="muted">—</span>
              )}
            </td>
            <td>
              <WindowReset w={a.weekly} now={now} absent="—" />
            </td>
            <td>
              <WindowReset w={a.five_hour} now={now} absent="not reported" />
            </td>
          </tr>
        ))}
        {accounts.length === 0 && (
          <tr>
            <td colSpan={8} className="empty">
              No accounts yet. Add one with <code>polyflare accounts login</code>.
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}

function RequestsTable({ rows, now }: { rows: RequestRow[]; now: number }) {
  return (
    <table className="grid">
      <thead>
        <tr>
          <th>When</th>
          <th>Method</th>
          <th>Path</th>
          <th>Provider</th>
          <th>Status</th>
          <th>Latency</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => {
          const ago = Math.max(0, Math.round(now / 1000 - r.requested_at));
          const statusTone = r.status >= 500 ? "err" : r.status >= 400 ? "warn" : "ok";
          return (
            <tr key={r.id}>
              <td className="muted" title={new Date(r.requested_at * 1000).toLocaleString()}>
                {ago < 60 ? `${ago}s ago` : `${Math.floor(ago / 60)}m ago`}
              </td>
              <td>{r.method}</td>
              <td>
                <code>{r.path}</code>
                {r.aliased && <span className="tag">aliased</span>}
              </td>
              <td className="muted">{r.provider}</td>
              <td>
                <span className={`code code-${statusTone}`}>{r.status}</span>
              </td>
              <td className="muted">{r.duration_ms} ms</td>
            </tr>
          );
        })}
        {rows.length === 0 && (
          <tr>
            <td colSpan={6} className="empty">
              No requests recorded yet.
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}

export function App() {
  const [pools, setPools] = useState<Pool[]>([]);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [requests, setRequests] = useState<RequestRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  // A 1s-ticking clock so the reset countdowns advance live without refetching.
  const [now, setNow] = useState<number>(() => Date.now());
  // Theme: initialized from the attribute the pre-paint script in index.html already set.
  const [theme, setTheme] = useState<string>(
    () => document.documentElement.getAttribute("data-theme") || "dark",
  );

  function toggleTheme() {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    document.documentElement.setAttribute("data-theme", next);
    try {
      localStorage.setItem("pf-theme", next);
    } catch {
      /* private mode / storage disabled — the in-memory toggle still works for this session */
    }
  }

  // Lifted out of the effect so the account-settings controls can trigger an immediate refresh
  // after a mutation (in addition to the 15s poll).
  const load = useCallback(async () => {
    try {
      const [p, a, r] = await Promise.all([api.pools(), api.accounts(), api.requests(50)]);
      setPools(p);
      setAccounts(a);
      setRequests(r.rows);
      setUpdatedAt(Date.now());
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    load();
    const dataTimer = setInterval(load, REFRESH_MS);
    const clockTimer = setInterval(() => setNow(Date.now()), 1000);
    return () => {
      clearInterval(dataTimer);
      clearInterval(clockTimer);
    };
  }, [load]);

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="flare" />
          <h1>PolyFlare</h1>
        </div>
        <div className="meta">
          {error ? (
            <span className="err-text">{error}</span>
          ) : (
            <span className="muted">
              {updatedAt ? `updated ${new Date(updatedAt).toLocaleTimeString()}` : "loading…"}
            </span>
          )}
          <button
            className="theme-toggle"
            onClick={toggleTheme}
            title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
            aria-label="Toggle color theme"
          >
            {theme === "dark" ? "☀" : "☾"}
          </button>
        </div>
      </header>

      <main>
        <section>
          <h2>Pools</h2>
          <PoolCards pools={pools} />
        </section>

        <section>
          <h2>Accounts</h2>
          <AccountsTable accounts={accounts} now={now} onMutate={load} />
        </section>

        <section>
          <h2>Recent requests</h2>
          <RequestsTable rows={requests} now={now} />
        </section>
      </main>
    </div>
  );
}
