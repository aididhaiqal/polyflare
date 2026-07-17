// The only unauthenticated route. A single centered ccflare-styled card: no signup, no other
// credential fields — the dashboard has exactly one shared operator token (POLYFLARE_ADMIN_TOKEN).
import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import { useAuth } from "../auth/AuthProvider";
import { ApiError, fetchJson, type WhoamiView } from "../lib/api";

export function Login() {
  const { signIn, signOut } = useAuth();
  const navigate = useNavigate();
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [verifying, setVerifying] = useState(false);

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
    <div className="min-h-screen flex items-center justify-center p-6">
      <div className="w-full max-w-sm bg-card border border-border rounded p-6">
        <h1 className="text-2xl font-semibold text-fg mb-1">
          Poly<span className="text-accent">Flare</span>
        </h1>
        <p className="text-sm text-fg opacity-70 mb-6">
          Enter the <code className="font-mono">POLYFLARE_ADMIN_TOKEN</code> configured on the
          server to open the dashboard.
        </p>
        <form onSubmit={handleSubmit} className="flex flex-col gap-3">
          <input
            type="password"
            autoFocus
            autoComplete="off"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            placeholder="Admin token"
            className="bg-bg border border-border rounded px-3 py-2 text-fg placeholder:opacity-40 focus:outline-none focus:border-accent"
          />
          {error && <p className="text-sm text-error">{error}</p>}
          <button
            type="submit"
            disabled={verifying || !value}
            className="bg-accent text-bg font-medium rounded px-3 py-2 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {verifying ? "Connecting…" : "Connect"}
          </button>
        </form>
      </div>
    </div>
  );
}
