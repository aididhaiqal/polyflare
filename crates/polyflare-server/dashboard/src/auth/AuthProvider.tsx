// Access context: holds an admin bearer token (when configured) or the result of the startup
// tokenless `/api/whoami` probe for a loopback-open server. Keeping both in React state is what
// makes a later 401 re-render <RequireAuth> and redirect without imperative router access.
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { Navigate } from "react-router-dom";

import { clearToken, getToken, setToken, setUnauthorizedHandler } from "../lib/api";

interface AuthContextValue {
  /** The current admin bearer token, or null when signed out. */
  token: string | null;
  /** True when an unauthenticated `/api/whoami` probe proves this is a tokenless local server. */
  localAccess: boolean;
  /** Initial local-access probe is still in flight. */
  checkingAccess: boolean;
  /** Persists `token` (localStorage) and updates context state. */
  signIn: (token: string) => void;
  /** Clears the persisted token and context state. */
  signOut: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setTokenState] = useState<string | null>(() => getToken());
  const [localAccess, setLocalAccess] = useState(false);
  const [checkingAccess, setCheckingAccess] = useState(token === null);

  useEffect(() => {
    // Fires once per 401 from any fetchJson call (or notifyUnauthorized() for the raw-fetch SSE
    // path in useLogStream.ts). Clearing state here — not calling navigate() — is deliberate: this
    // callback lives outside the component tree, so re-rendering <RequireAuth> is the only clean
    // way to force it back to /login.
    setUnauthorizedHandler(() => {
      clearToken();
      setTokenState(null);
      setLocalAccess(false);
    });
  }, []);

  useEffect(() => {
    if (token) {
      setLocalAccess(false);
      setCheckingAccess(false);
      return;
    }

    let cancelled = false;
    setCheckingAccess(true);
    fetch("/api/whoami", { headers: { Accept: "application/json" } })
      .then((response) => {
        if (!cancelled) setLocalAccess(response.ok);
      })
      .catch(() => {
        if (!cancelled) setLocalAccess(false);
      })
      .finally(() => {
        if (!cancelled) setCheckingAccess(false);
      });
    return () => {
      cancelled = true;
    };
  }, [token]);

  const value = useMemo<AuthContextValue>(
    () => ({
      token,
      localAccess,
      checkingAccess,
      signIn: (next: string) => {
        setToken(next);
        setTokenState(next);
        setLocalAccess(false);
        setCheckingAccess(false);
      },
      signOut: () => {
        clearToken();
        setTokenState(null);
        setLocalAccess(false);
        setCheckingAccess(true);
      },
    }),
    [checkingAccess, localAccess, token],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an <AuthProvider>");
  return ctx;
}

/** Renders children after either token auth or the tokenless loopback probe succeeds. */
export function RequireAuth({ children }: { children: ReactNode }) {
  const { token, localAccess, checkingAccess } = useAuth();
  if (checkingAccess) return <AccessCheck />;
  if (!token && !localAccess) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

export function AccessCheck() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-bg px-5 text-fg">
      <div className="flex items-center gap-3 text-[11px] font-semibold uppercase tracking-[0.14em] opacity-55">
        <span className="h-2 w-2 animate-pulse rounded-full bg-signal" />
        Checking local access
      </div>
    </div>
  );
}
