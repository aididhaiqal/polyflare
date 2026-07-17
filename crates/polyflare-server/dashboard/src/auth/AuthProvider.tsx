// Auth context: holds the admin bearer token in React state (seeded from localStorage) and keeps
// it in sync with api.ts's unauthorized handler. Keeping the token in state — not just in
// localStorage — is what makes a 401 anywhere in the app actually navigate: clearing state
// re-renders <RequireAuth>, which redirects to /login. A callback outside the component tree has
// no navigate() to call, so this is the cleaner wiring than reaching for the router imperatively.
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
  /** Persists `token` (localStorage) and updates context state. */
  signIn: (token: string) => void;
  /** Clears the persisted token and context state. */
  signOut: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setTokenState] = useState<string | null>(() => getToken());

  useEffect(() => {
    // Fires once per 401 from any fetchJson call (or notifyUnauthorized() for the raw-fetch SSE
    // path in useLogStream.ts). Clearing state here — not calling navigate() — is deliberate: this
    // callback lives outside the component tree, so re-rendering <RequireAuth> is the only clean
    // way to force it back to /login.
    setUnauthorizedHandler(() => {
      clearToken();
      setTokenState(null);
    });
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      token,
      signIn: (next: string) => {
        setToken(next);
        setTokenState(next);
      },
      signOut: () => {
        clearToken();
        setTokenState(null);
      },
    }),
    [token],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an <AuthProvider>");
  return ctx;
}

/** Renders `children` only when a token is present; otherwise bounces to /login. */
export function RequireAuth({ children }: { children: ReactNode }) {
  const { token } = useAuth();
  if (!token) return <Navigate to="/login" replace />;
  return <>{children}</>;
}
