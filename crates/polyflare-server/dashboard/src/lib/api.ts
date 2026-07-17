// Typed client for the polyflare dashboard read API. All response interfaces below mirror the
// EXACT serde field names/casing emitted by the backend — see:
//   crates/polyflare-server/src/read_api.rs  (OverviewView, AccountView, AccountDetailView,
//     TrendsView, PoolView, RequestRowView/RequestsView, RequestsQuery)
//   crates/polyflare-server/src/auth.rs       (whoami_handler, capabilities_handler)
//   crates/polyflare-server/src/log_bus.rs    (LogEvent, LogLevel)
//
// IMPORTANT: `/api/*` paths are absolute-from-origin (e.g. `/api/overview`), NOT prefixed with the
// `/dashboard/` SPA base that `vite.config.ts`'s `base` applies to built assets. Every call site in
// this file (and in queries.ts / useLogStream.ts) passes an absolute `/api/...` path for this
// reason — do not route these through the Vite `base`.

/** localStorage key holding the admin bearer token (see crates/polyflare-server/src/auth.rs). */
export const TOKEN_STORAGE_KEY = "polyflare_admin_token";

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_STORAGE_KEY);
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_STORAGE_KEY, token);
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_STORAGE_KEY);
}

/** Thrown by `fetchJson` for any non-2xx response, including 401 (after `onUnauthorized` fires). */
export class ApiError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(status: number, body: unknown) {
    super(`request failed with status ${status}`);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

type UnauthorizedHandler = () => void;
let unauthorizedHandler: UnauthorizedHandler | null = null;

/** Registers a callback invoked once per 401 response, before `fetchJson` throws. Typically wired
 * by the auth/shell layer to clear the stored token and redirect to a login screen. */
export function setUnauthorizedHandler(fn: UnauthorizedHandler): void {
  unauthorizedHandler = fn;
}

/** Invokes the registered unauthorized handler, if any, without throwing. For callers that hit
 * `/api/*` via a raw `fetch` instead of `fetchJson` (e.g. `useLogStream.ts`'s manual SSE reader,
 * which can't use `fetchJson` because it needs the raw `Response` body stream) so a 401 there
 * still clears the token / redirects to login the same way a `fetchJson` 401 would. */
export function notifyUnauthorized(): void {
  unauthorizedHandler?.();
}

async function readBody(res: Response): Promise<unknown> {
  const text = await res.text().catch(() => "");
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

/** Fetches `path`, attaching the stored admin bearer token. Throws `ApiError` on any non-2xx
 * response (calling the registered `onUnauthorized` handler first on 401); otherwise resolves with
 * the parsed JSON body. */
export async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getToken();
  const headers = new Headers(init?.headers);
  headers.set("Accept", "application/json");
  if (token) headers.set("Authorization", `Bearer ${token}`);

  const res = await fetch(path, { ...init, headers });

  if (res.status === 401) {
    unauthorizedHandler?.();
    throw new ApiError(res.status, await readBody(res));
  }
  if (!res.ok) {
    throw new ApiError(res.status, await readBody(res));
  }
  return (await res.json()) as T;
}

// ---------------------------------------------------------------------------------------------
// Response shapes — mirror read_api.rs's `#[derive(Serialize)]` structs field-for-field.
// ---------------------------------------------------------------------------------------------

/** `read_api.rs::WindowView` — one rate-limit window as `/api/accounts` consumes it. */
export interface WindowView {
  used_percent: number;
  reset_at: number | null;
  stale: boolean;
}

/** `read_api.rs::UsageWindowView` — one entry of `AccountView.usage` / `AccountDetailView.
 * quota_windows`. `window` is `"five_hour" | "weekly"` in practice but left as `string` since the
 * backend types it `&'static str`, not a closed enum. */
export interface UsageWindowView {
  window: string;
  used_percent: number;
  reset_at: number | null;
}

/** `read_api.rs::TokenHealthView` — derived JWT-`exp` state only; NEVER a token. */
export interface TokenHealthView {
  access_state: "missing" | "expired" | "valid";
  access_expires_at: number | null;
}

/** `read_api.rs::AccountView` — one row of `GET /api/accounts`. */
export interface AccountView {
  id: string;
  email: string;
  pool: string | null;
  provider: string;
  status: string;
  plan_type: string;
  routing_policy: string;
  reset_at: number | null;
  five_hour: WindowView | null;
  weekly: WindowView | null;
  usage: UsageWindowView[];
  token_health: TokenHealthView;
  request_count_24h: number;
}

/** `read_api.rs::AccountIdentityView` — `AccountDetailView.identity`. */
export interface AccountIdentityView {
  id: string;
  email: string;
  alias: string | null;
  workspace_id: string | null;
  workspace_label: string | null;
  seat_type: string | null;
  plan_type: string;
  provider: string;
  pool: string | null;
}

/** `read_api.rs::RequestTotalsView` — `AccountDetailView.request_totals`. */
export interface RequestTotalsView {
  request_count: number;
  total_tokens: number;
}

/** `read_api.rs::AccountDetailView` — `GET /api/accounts/{id}` response. */
export interface AccountDetailView {
  identity: AccountIdentityView;
  status: string;
  quota_windows: UsageWindowView[];
  token_status: TokenHealthView;
  routing_policy: string;
  security_work_authorized: boolean;
  request_totals: RequestTotalsView;
}

/** `read_api.rs::Point` — one `{t, v}` sample of a `TrendsView` series. */
export interface Point {
  t: number;
  v: number;
}

/** `read_api.rs::TrendsView` — `GET /api/accounts/{id}/trends` response. */
export interface TrendsView {
  account_id: string;
  primary: Point[];
  secondary: Point[];
}

/** `read_api.rs::PoolView` — one row of `GET /api/pools`. */
export interface PoolView {
  pool: string | null;
  accounts: number;
  active: number;
  available: number;
  usage_percent: number;
  strategy: string;
}

/** `read_api.rs::RequestRowView` — one row of `GET /api/requests`. */
export interface RequestRowView {
  id: number;
  requested_at: number;
  provider: string;
  method: string;
  path: string;
  aliased: boolean;
  status: number;
  duration_ms: number;
  account_id: string | null;
  model: string | null;
  reasoning_effort: string | null;
  service_tier: string | null;
  transport: string | null;
  ttft_ms: number | null;
  total_tokens: number | null;
  cached_tokens: number | null;
  tps: number | null;
}

/** `read_api.rs::RequestsView` — `GET /api/requests` response envelope. */
export interface RequestsView {
  total: number;
  rows: RequestRowView[];
}

/** `read_api.rs::RequestsQuery` — filter/pagination params for `GET /api/requests`. All optional;
 * `useRequests` (queries.ts) serializes only the defined ones into the query string. */
export interface RequestsQueryParams {
  limit?: number;
  offset?: number;
  account?: string;
  provider?: string;
  status_class?: string;
  model?: string;
  transport?: string;
  since_ts?: number;
}

/** `read_api.rs::KpisView` — `OverviewView.kpis`. */
export interface KpisView {
  requests: number;
  success: number;
  errors: number;
  success_rate: number;
  avg_latency_ms: number;
  total_tokens: number;
}

/** `read_api.rs::ProviderQuotaView` — one entry of `OverviewView.quota`. */
export interface ProviderQuotaView {
  provider: string;
  five_hour: number;
  weekly: number;
}

/** `read_api.rs::PoolOverviewView` — one entry of `OverviewView.pools`. */
export interface PoolOverviewView {
  pool: string | null;
  accounts: number;
  available: number;
}

/** `read_api.rs::RecentErrorView` — one entry of `OverviewView.recent_errors`. */
export interface RecentErrorView {
  status: number;
  account_id: string | null;
  error_code: string | null;
  requested_at: number;
}

/** `read_api.rs::OverviewView` — `GET /api/overview` response. */
export interface OverviewView {
  kpis: KpisView;
  quota: ProviderQuotaView[];
  pools: PoolOverviewView[];
  accounts_available: number;
  recent_errors: RecentErrorView[];
}

/** `auth.rs::whoami_handler` — `GET /api/whoami` response. No identity beyond `ok` today (a single
 * shared operator token has no per-user identity to report). */
export interface WhoamiView {
  ok: boolean;
}

/** `auth.rs::capabilities_handler` — `GET /api/capabilities` response. Grows as later tasks add
 * capability flags; `live_logs` is the only one today. */
export interface CapabilitiesView {
  live_logs: boolean;
}

/** `log_bus.rs::LogLevel` — `#[serde(rename_all = "lowercase")]`. */
export type LogLevel = "info" | "warn" | "error" | "debug";

/** `log_bus.rs::LogEvent` — one content-free line from `GET /api/logs/stream` (one JSON object per
 * SSE `data:` line — see crates/polyflare-server/src/sse.rs). Optional fields are
 * `#[serde(skip_serializing_if = "Option::is_none")]` on the Rust side, so they may be entirely
 * absent from the wire payload rather than present-as-null. */
export interface LogEvent {
  ts_ms: number;
  level: LogLevel;
  provider?: string;
  account?: string;
  model?: string;
  status?: number;
  latency_ms?: number;
  kind: string;
  message: string;
}

// ---------------------------------------------------------------------------------------------
// Thin per-endpoint helpers (queries.ts wraps these in useQuery).
// ---------------------------------------------------------------------------------------------

export const api = {
  overview: () => fetchJson<OverviewView>("/api/overview"),
  accounts: () => fetchJson<AccountView[]>("/api/accounts"),
  account: (id: string) => fetchJson<AccountDetailView>(`/api/accounts/${encodeURIComponent(id)}`),
  accountTrends: (id: string) =>
    fetchJson<TrendsView>(`/api/accounts/${encodeURIComponent(id)}/trends`),
  pools: () => fetchJson<PoolView[]>("/api/pools"),
  requests: (qs: string) => fetchJson<RequestsView>(`/api/requests${qs}`),
  capabilities: () => fetchJson<CapabilitiesView>("/api/capabilities"),
  whoami: () => fetchJson<WhoamiView>("/api/whoami"),
};
