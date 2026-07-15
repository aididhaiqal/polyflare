// Typed client for the polyflare read API (see crates/polyflare-server/src/read_api.rs). All
// endpoints are same-origin GETs returning JSON; the dashboard is served from the same binary.

export interface Window {
  used_percent: number;
  reset_at: number | null;
}

export interface Account {
  id: string;
  email: string;
  pool: string | null;
  provider: string;
  status: string;
  plan_type: string;
  reset_at: number | null;
  primary: Window | null;
  secondary: Window | null;
}

export interface Pool {
  pool: string | null;
  accounts: number;
  active: number;
}

export interface RequestRow {
  id: number;
  requested_at: number;
  provider: string;
  method: string;
  path: string;
  aliased: boolean;
  status: number;
  duration_ms: number;
}

export interface RequestsPage {
  total: number;
  rows: RequestRow[];
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(path, { headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return (await res.json()) as T;
}

export const api = {
  accounts: () => getJson<Account[]>("/api/accounts"),
  pools: () => getJson<Pool[]>("/api/pools"),
  requests: (limit = 100) => getJson<RequestsPage>(`/api/requests?limit=${limit}`),
};
