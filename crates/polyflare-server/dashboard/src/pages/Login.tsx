// The only unauthenticated route. A single centered ccflare-styled card: no signup, no other
// credential fields — the dashboard has exactly one shared operator token (POLYFLARE_ADMIN_TOKEN).
import { useState, type FormEvent } from "react";
import { Navigate, useNavigate } from "react-router-dom";

import { AccessCheck, useAuth } from "../auth/AuthProvider";
import { ApiError, fetchJson, type WhoamiView } from "../lib/api";
import { BrandMark } from "../shell/Sidebar";

export function Login() {
  const { token, localAccess, checkingAccess, signIn, signOut } = useAuth();
  const navigate = useNavigate();
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [verifying, setVerifying] = useState(false);

  if (checkingAccess) return <AccessCheck />;
  if (token || localAccess) return <Navigate to="/" replace />;

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!value || verifying) return;

    setError(null);
    setVerifying(true);
    // Stored optimistically so the /api/whoami probe below carries it as the Authorization
    // header (fetchJson reads the token from localStorage on every call).
    signIn(value);
    try {
      await fetchJson<WhoamiView>("/api/whoami");
      navigate("/", { replace: true });
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        signOut();
        setError("Invalid token");
      } else if (err instanceof ApiError && err.status === 503) {
        setError("Dashboard is disabled (POLYFLARE_ADMIN_TOKEN is not set on the server)");
      } else {
        signOut();
        setError("Could not reach the server. Try again.");
      }
    } finally {
      setVerifying(false);
    }
  }

  return (
    <div className="relative flex min-h-screen items-center justify-center overflow-hidden px-5 py-10">
      <div className="absolute left-[10%] top-[18%] h-52 w-52 rounded-full bg-signal/[0.06] blur-3xl" />
      <div className="absolute bottom-[12%] right-[12%] h-60 w-60 rounded-full bg-accent/[0.08] blur-3xl" />
      <div className="relative grid w-full max-w-4xl overflow-hidden rounded-2xl border border-border/80 bg-card/90 shadow-[0_28px_90px_hsl(var(--surface-shadow)/0.42)] backdrop-blur-xl md:grid-cols-[1.1fr_0.9fr]">
        <section className="hidden min-h-[480px] flex-col justify-between border-r border-border/70 bg-muted/20 p-10 md:flex">
          <BrandMark />
          <div>
            <p className="text-[9px] font-bold uppercase tracking-[0.24em] text-signal opacity-75">
              Operator workspace
            </p>
            <h1 className="mt-4 max-w-sm text-4xl font-semibold leading-[1.04] tracking-[-0.05em] text-fg">
              Every route.<br />One clear signal.
            </h1>
            <p className="mt-5 max-w-sm text-sm leading-6 text-fg opacity-55">
              Watch account health, capacity, continuity, and request outcomes from one content-safe control plane.
            </p>
          </div>
          <p className="font-mono text-[9px] uppercase tracking-[0.14em] text-fg opacity-30">
            Local operator access · encrypted at rest
          </p>
        </section>

        <section className="flex min-h-[480px] flex-col justify-center p-7 sm:p-10">
          <div className="mb-10 md:hidden">
            <BrandMark />
          </div>
          <p className="text-[9px] font-bold uppercase tracking-[0.2em] text-signal opacity-70">
            Secure access
          </p>
          <h2 className="mt-2 text-2xl font-semibold tracking-[-0.035em] text-fg">Open dashboard</h2>
          <p className="mb-7 mt-2 text-[12px] leading-5 text-fg opacity-55">
            Use the admin token configured on this PolyFlare server.
          </p>
          <form onSubmit={handleSubmit} className="flex flex-col gap-3">
          <label htmlFor="admin-token" className="text-[9px] font-bold uppercase tracking-[0.14em] text-fg opacity-45">
            Admin token
          </label>
          <input
            id="admin-token"
            type="password"
            autoFocus
            autoComplete="off"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            placeholder="Paste token"
            className="rounded-lg border border-border bg-bg/70 px-3.5 py-2.5 font-mono text-[12px] text-fg placeholder:opacity-30 focus:border-signal focus:outline-none"
          />
          {error && <p className="text-sm text-error">{error}</p>}
          <button
            type="submit"
            disabled={verifying || !value}
            className="mt-2 rounded-lg bg-accent px-3 py-2.5 text-[12px] font-bold text-bg shadow-[0_8px_24px_hsl(var(--accent)/0.2)] transition-transform hover:-translate-y-px disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:translate-y-0"
          >
            {verifying ? "Connecting…" : "Connect"}
          </button>
        </form>
        </section>
      </div>
    </div>
  );
}
