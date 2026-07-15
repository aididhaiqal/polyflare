import { useEffect, useState } from "react";
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

function AccountsTable({ accounts, now }: { accounts: Account[]; now: number }) {
  return (
    <table className="grid">
      <thead>
        <tr>
          <th>Account</th>
          <th>Pool</th>
          <th>Status</th>
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
              {a.pool ? <span className="pill">{a.pool}</span> : <span className="muted">—</span>}
            </td>
            <td>
              <StatusBadge status={a.status} />
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
            <td colSpan={7} className="empty">
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

  useEffect(() => {
    let alive = true;
    async function load() {
      try {
        const [p, a, r] = await Promise.all([api.pools(), api.accounts(), api.requests(50)]);
        if (!alive) return;
        setPools(p);
        setAccounts(a);
        setRequests(r.rows);
        setUpdatedAt(Date.now());
        setError(null);
      } catch (e) {
        if (alive) setError(String(e));
      }
    }
    load();
    const dataTimer = setInterval(load, REFRESH_MS);
    const clockTimer = setInterval(() => alive && setNow(Date.now()), 1000);
    return () => {
      alive = false;
      clearInterval(dataTimer);
      clearInterval(clockTimer);
    };
  }, []);

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
        </div>
      </header>

      <main>
        <section>
          <h2>Pools</h2>
          <PoolCards pools={pools} />
        </section>

        <section>
          <h2>Accounts</h2>
          <AccountsTable accounts={accounts} now={now} />
        </section>

        <section>
          <h2>Recent requests</h2>
          <RequestsTable rows={requests} now={now} />
        </section>
      </main>
    </div>
  );
}
