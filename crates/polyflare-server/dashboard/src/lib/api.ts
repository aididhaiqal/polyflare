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
 * backend types it `&'static str`, not a closed enum. Stale observations remain available as
 * historical evidence but are not current limits. */
export interface UsageWindowView {
  window: string;
  used_percent: number;
  reset_at: number | null;
  stale: boolean;
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
  alias: string | null;
  pool: string | null;
  pools: string[];
  provider: string;
  status: string;
  plan_type: string;
  routing_policy: string;
  security_work_authorized: boolean;
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
  pools: string[];
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

/** `polyflare_core::depletion::RiskLevel` — `#[serde(rename_all = "lowercase")]`. Plain `>=`
 * thresholds (0.60/0.80/0.95 of the depletion-risk fraction), no hysteresis. */
export type RiskLevel = "safe" | "warning" | "danger" | "critical";

/** `polyflare_core::depletion::DepletionForecast` — the per-account (secondary/weekly-window)
 * EWMA depletion forecast. Content-free: numeric fields + a `RiskLevel` enum only. `rate_per_second`
 * is smoothed d(used%)/dt; `burn_rate` is dimensionless (current/sustainable, >1 = burning faster
 * than budget); `seconds_until_exhaustion`/`projected_exhaustion_at` are `null` when the projected
 * exhaustion would land after the window's own reset (i.e. it resets before it would run out). */
export interface DepletionForecast {
  risk: number;
  risk_level: RiskLevel;
  rate_per_second: number;
  burn_rate: number;
  used_percent: number;
  safe_usage_percent: number;
  seconds_until_reset: number;
  seconds_until_exhaustion: number | null;
  projected_exhaustion_at: number | null;
}

/** `read_api.rs::TrendsView` — `GET /api/accounts/{id}/trends` response. `forecast` (D16 T5) is
 * `null` when there are fewer than 2 secondary-window samples, the EWMA rate never establishes, or
 * the window has already reset. */
export interface TrendsView {
  account_id: string;
  primary: Point[];
  secondary: Point[];
  forecast: DepletionForecast | null;
}

/** `polyflare_core::weekly_pace::PaceStatus` — `#[serde(rename_all = "snake_case")]`. */
export type PaceStatus = "on_track" | "ahead" | "behind" | "danger";

/** `polyflare_core::weekly_pace::Confidence` — `#[serde(rename_all = "lowercase")]`. How many of
 * the pool's paced accounts have an established forecast burn rate, and whether any are stale. */
export type PaceConfidence = "high" | "medium" | "low";

/** `polyflare_core::weekly_pace::WeeklyCreditPaceReport` — the pool-wide weekly credit pace: actual
 * vs. scheduled (linear-budget) usage, a discrete-event pool-drain simulation (soonest-reset-first,
 * refilling at each account's own reset boundary) answering "does the pool run dry before enough
 * resets refill it?", and the resulting recommendation fields. All fields content-free (credits/
 * percentages/hours/counts + status/confidence enums only) — see `read_api.rs::pace_handler`. */
export interface WeeklyCreditPaceReport {
  total_full_credits: number;
  total_actual_remaining_credits: number;
  total_expected_remaining_credits: number;
  actual_used_percent: number;
  scheduled_used_percent: number;
  delta_percent: number;
  schedule_gap_credits: number;
  smoothed_delta_percent: number;
  smoothed_schedule_gap_credits: number;
  projected_shortfall_credits: number;
  pause_for_break_even_hours: number | null;
  pace_multiplier: number | null;
  throttle_to_percent: number | null;
  reduce_by_percent: number | null;
  pro_account_equivalent_to_cover_over_plan: number | null;
  pro_accounts_to_cover_over_plan: number | null;
  projected_depletion_hours: number | null;
  projected_minimum_remaining_credits: number;
  forecast_burn_rate_credits_per_hour: number | null;
  scheduled_burn_rate_credits_per_hour: number;
  status: PaceStatus;
  account_count: number;
  stale_account_count: number;
  inactive_account_count: number;
  confidence: PaceConfidence;
}

/** `read_api.rs::PaceView` — `GET /api/pace` response. `pace` is `null` when there is no eligible,
 * fresh, positive-capacity account to project a pace for. */
export interface PaceResponse {
  pace: WeeklyCreditPaceReport | null;
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
  /** PolyFlare-generated correlation id shared with structured and live logs. */
  request_id: string | null;
  /** One-way SHA-256 continuity key shared with the Sessions view. */
  session_key: string | null;
  requested_at: number;
  provider: string;
  method: string;
  path: string;
  aliased: boolean;
  status: number;
  duration_ms: number;
  account_id: string | null;
  target_kind: "account" | "credential" | null;
  provider_credential_id: string | null;
  model: string | null;
  upstream_model: string | null;
  upstream_transport: string | null;
  reasoning_effort: string | null;
  service_tier: string | null;
  transport: string | null;
  ttft_ms: number | null;
  /** API total: upstream-reported total when present, otherwise a compatibility fallback. */
  total_tokens: number | null;
  cached_tokens: number | null;
  input_tokens: number | null;
  cached_input_tokens: number | null;
  cache_write_input_tokens: number | null;
  uncached_input_tokens: number | null;
  output_tokens: number | null;
  reasoning_output_tokens: number | null;
  visible_output_tokens: number | null;
  reported_total_tokens: number | null;
  /** Codex blended/effective usage: uncached input + all output. */
  effective_tokens: number | null;
  usage_schema: "openai_responses_v1" | "legacy_unknown" | null;
  usage_source: "upstream_response" | "codex_lb_import" | "polyflare_legacy" | null;
  usage_status: "final" | "legacy" | null;
  orchestration_input_tokens: number | null;
  orchestration_output_tokens: number | null;
  orchestration_cached_input_tokens: number | null;
  tps: number | null;
  /** Imported codex-lb rows use status=0 as "no HTTP status" and carry this bounded outcome. */
  outcome: "success" | "error" | null;
  /** Native Codex stream terminal result; authoritative over its initial HTTP status. */
  protocol_outcome:
    | "completed"
    | "failed"
    | "incomplete"
    | "cancelled"
    | "transport_lost"
    | null;
  error_code: string | null;
  /** The codex sub-agent role label from `x-openai-subagent` (`"review"` / `"compact"` /
   * `"memory_consolidation"` / `"collab_spawn"`), or `null` for the main agent. A bounded role
   * slug — content-free, same content-safety class as `model`. */
  subagent: string | null;
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
  request_id?: string;
  session_key?: string;
  account?: string;
  provider?: string;
  status_class?: string;
  model?: string;
  transport?: string;
  since_ts?: number;
}

/** `read_api.rs::SessionRowView` — one row of `GET /api/sessions`. Content-free: `session_key` is a
 * sha256 hash (one-way, never raw header/content — see read_api.rs module doc), and no field here
 * carries a token/body/prompt. `owning_account_id`/`owner_email` are null for a session that never
 * completed a turn or whose account was deleted (LEFT JOIN — those rows survive). */
export interface SessionRowView {
  session_key: string;
  key_strength: string;
  owning_account_id: string | null;
  owner_email: string | null;
  provider: string;
  target_kind: "account" | "credential";
  target_id: string | null;
  target_label: string | null;
  model: string | null;
  state: string;
  required_capabilities: string | null;
  created_at: number;
  updated_at: number;
  last_activity_at: number;
  request_count: number;
}

/** `read_api.rs::SessionsView` — `GET /api/sessions` response envelope. */
export interface SessionsView {
  total: number;
  rows: SessionRowView[];
}

/** `read_api.rs::SessionsQuery` — pagination params for `GET /api/sessions`. Both optional;
 * `useSessions` (queries.ts) serializes only the defined ones into the query string. */
export interface SessionsQueryParams {
  limit?: number;
  offset?: number;
  session_key?: string;
}

/** `read_api.rs::ReportBucketView`/`ReportBreakdownView`/`ReportTotalsView` share this same flat
 * set of `polyflare_store::ReportMetrics` fields — never nested under a `metrics` key, same
 * flat-field convention as `SeriesBucketView`. Not itself a wire type (the backend doesn't emit a
 * `ReportMetricsView` struct), just the shared TS shape the three view interfaces below extend. */
export interface ReportMetricsView {
  requests: number;
  errors: number;
  cost_usd: number;
  tokens: number;
  input_tokens: number;
  cached_tokens: number;
  cache_write_tokens: number;
  reasoning_tokens: number;
  effective_tokens: number;
  orchestration_tokens: number;
  orchestration_cached_tokens: number;
  avg_duration_ms: number;
  avg_ttft_ms: number;
  ttft_sample_count: number;
}

/** `read_api.rs::ReportBucketView` — one entry of `ReportsView.time_series`. `ts` is the bucket
 * start (unix-epoch seconds); zero-filled across the aligned `[since_ts, now]` grid, same
 * zero-fill contract as `SeriesBucketView`. */
export interface ReportBucketView extends ReportMetricsView {
  ts: number;
}

/** `read_api.rs::ReportBreakdownView` — one row of `ReportsView.breakdown`: metrics scoped to one
 * value of the requested `dimension` (`account`/`model`/`provider`). */
export interface ReportBreakdownView extends ReportMetricsView {
  key: string;
}

/** `read_api.rs::ReportTotalsView` — `ReportsView.totals`: the same flat metrics fields plus two
 * derived ratios (`error_rate = errors/requests`, `cache_hit_rate = cached_tokens/input_tokens`, both
 * `0.0` on a 0/0 divide — the same guard `KpisView.success_rate` uses). */
export interface ReportTotalsView extends ReportMetricsView {
  error_rate: number;
  cache_hit_rate: number;
}

/** `read_api.rs::ReportsView` — `GET /api/reports` response: a zero-filled time series, a
 * per-dimension breakdown, and top-line totals, all sourced from the same `(since_ts, provider)`
 * window. */
export interface ReportsView {
  time_series: ReportBucketView[];
  breakdown: ReportBreakdownView[];
  totals: ReportTotalsView;
}

/** `read_api.rs::KpisView` — `OverviewView.kpis`. */
export interface KpisView {
  requests: number;
  success: number;
  errors: number;
  success_rate: number;
  avg_latency_ms: number;
  total_tokens: number;
  effective_tokens: number;
  cache_write_input_tokens: number;
  orchestration_tokens: number;
  orchestration_cached_tokens: number;
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
  provider: string;
  account_id: string | null;
  target_kind: "account" | "credential" | null;
  provider_credential_id: string | null;
  error_code: string | null;
  requested_at: number;
}

/** Process-local, content-free admission pressure aggregated across request/socket and
 * new/owner lanes. */
export interface AdmissionOverviewView {
  waiters: number;
  waits_total: number;
  acquired_after_wait_total: number;
  timeouts_total: number;
  ineligible_total: number;
  cancelled_total: number;
  owner_recovery_total: number;
  avg_wait_ms: number;
  in_flight_pressure: number;
  calibration_ratio: number;
  calibration_samples: number;
}

/** `read_api.rs::OverviewView` — `GET /api/overview` response. */
export interface OverviewView {
  kpis: KpisView;
  quota: ProviderQuotaView[];
  pools: PoolOverviewView[];
  accounts_available: number;
  admission: AdmissionOverviewView;
  recent_errors: RecentErrorView[];
}

/** `read_api.rs::SeriesBucketView` — one entry of `OverviewSeriesView.buckets`. `ts` is the bucket
 * start (unix-epoch seconds); every bucket in `[since_ts, now]` is present, zero-filled where the
 * backend had no rows for that hour — never a gap in the array. */
export interface SeriesBucketView {
  ts: number;
  requests: number;
  errors: number;
  avg_latency_ms: number;
  total_tokens: number;
  effective_tokens: number;
  cache_write_input_tokens: number;
  orchestration_tokens: number;
  orchestration_cached_tokens: number;
}

/** `read_api.rs::OverviewSeriesView` — `GET /api/overview/series` response: the rolling-24h
 * request-volume chart, bucketed hourly (`bucket_secs` is fixed today, not client-configurable). */
export interface OverviewSeriesView {
  bucket_secs: number;
  buckets: SeriesBucketView[];
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
  /** Matches `RequestRowView.request_id` for request-completion events. */
  request_id?: string;
  /** One-way SHA-256 continuity key shared with request history. */
  session_key?: string;
  provider?: string;
  account?: string;
  target_kind?: "account" | "credential";
  target_id?: string;
  model?: string;
  status?: number;
  latency_ms?: number;
  /** The codex sub-agent role label (`x-openai-subagent`: `"review"` / `"compact"` /
   * `"memory_consolidation"` / `"collab_spawn"`), absent for the main agent / non-request events.
   * A bounded role slug — content-free, same content-safety class as `model`. */
  subagent?: string | null;
  kind: string;
  message: string;
}

/** `read_api.rs::SettingFieldView` — one config field as `GET /api/settings` returns it. The 10
 * `class: "live"` fields carry their CURRENT `RuntimeSettings` value + clamp bounds (`min`/`max`);
 * restart-only/fixed fields are informational only — many have `value: null` (only `default` is
 * known). `admin_token`'s `value` is ALWAYS `null` (presence only — never render an input for it).
 * `kind` selects the control: `"bool"` -> a switch, everything else (`u32`/`secs`/`f64`/`string`)
 * -> a number/text input. */
export interface SettingFieldView {
  key: string;
  label: string;
  description: string;
  value: string | null;
  configured_value: string | null;
  pending_restart: boolean;
  default: string;
  class: "live" | "restart-only" | "fixed";
  kind: "u32" | "secs" | "bool" | "f64" | "string";
  min: number | null;
  max: number | null;
}

/** `read_api.rs::SettingsView` — the complete Settings-page configuration view. */
export interface SettingsView {
  fields: SettingFieldView[];
}

/** `read_api.rs::ApiKeyView` — one `api_keys` row for `GET /api/keys`, redacted: NEVER a `key_hash`
 * or the raw key (that row type doesn't even carry one — see the backend doc). `created_at`/
 * `last_used_at` are unix-epoch seconds (`i64` on the wire), `last_used_at` is `null` until the key
 * authenticates its first request. */
export interface ApiKeyView {
  id: string;
  key_prefix: string;
  label: string | null;
  enabled: boolean;
  created_at: number;
  last_used_at: number | null;
}

/** `read_api.rs::ApiKeysView` — `GET /api/keys` response envelope. */
export interface ApiKeysView {
  keys: ApiKeyView[];
}

/** `write_api.rs::create_key_handler`'s `201` response: `key` is the raw plaintext, returned this
 * ONE time only — never retrievable again, never present in `ApiKeyView`/`GET /api/keys`. Callers
 * must hold this only in transient state for a show-once modal, never in the `["keys"]` query
 * cache (which only ever holds refetched, redacted `ApiKeyView[]` data). */
export interface CreatedApiKey {
  id: string;
  key_prefix: string;
  key: string;
}

export interface ProviderCredentialView {
  id: string;
  provider_id: string;
  label: string;
  enabled: boolean;
  health_status: string;
  routing_weight: number;
  max_concurrency: number | null;
  cooldown_until: number | null;
  last_error_at: number | null;
}

export interface ProviderModelView {
  id: string;
  provider_id: string;
  public_model: string;
  upstream_model: string;
  display_name: string;
  context_window: number | null;
  max_output_tokens: number | null;
  supports_tools: boolean;
  supports_vision: boolean;
  supports_parallel_tool_calls: boolean;
  supports_web_search: boolean;
  supports_reasoning_summaries: boolean;
  reasoning_levels: string[];
  input_per_million: number | null;
  cached_input_per_million: number | null;
  output_per_million: number | null;
  visible_in_codex: boolean;
  visible_in_openai: boolean;
  enabled: boolean;
}

export interface CustomProviderView {
  id: string;
  slug: string;
  display_name: string;
  base_url: string;
  wire_api: string;
  enabled: boolean;
  stateless_responses: boolean;
  allow_private_hosts: boolean;
  connect_timeout_ms: number;
  stream_idle_timeout_ms: number;
  request_max_retries: number;
  max_concurrency: number | null;
  credentials: ProviderCredentialView[];
  models: ProviderModelView[];
}

export interface CreateProviderBody {
  slug: string;
  display_name: string;
  base_url: string;
  stateless_responses?: boolean;
  allow_private_hosts?: boolean;
  connect_timeout_ms?: number;
  stream_idle_timeout_ms?: number;
  request_max_retries?: number;
  max_concurrency?: number;
}

export interface CreateProviderModelBody {
  public_model: string;
  upstream_model: string;
  display_name: string;
  context_window?: number;
  supports_tools?: boolean;
  supports_vision?: boolean;
  supports_parallel_tool_calls?: boolean;
  supports_web_search?: boolean;
  supports_reasoning_summaries?: boolean;
  reasoning_levels?: string[];
  input_per_million?: number;
  cached_input_per_million?: number;
  output_per_million?: number;
  visible_in_codex?: boolean;
  visible_in_openai?: boolean;
}

export interface ProviderTestResult {
  ok: boolean;
  upstream_status: number;
  provider: string;
  model: string;
  credential_id: string | null;
  latency_ms: number;
}

// ---------------------------------------------------------------------------------------------
// Mutation client — write endpoints (queries.ts wraps these in useMutation). Content-free: every
// body field is account metadata (pool/policy/status/alias), never a token or conversation content.
// ---------------------------------------------------------------------------------------------

/** Body for PATCH /api/accounts/{id}. Every field optional — an ABSENT key leaves that attribute
 * unchanged. For `pool` and `alias` (double-option on the backend) an explicit `null` CLEARS and a
 * string sets; `status` is "active"|"paused"; `routing_policy` is "normal"|"burn_first"|"preserve". */
export interface AccountPatchBody {
  pool?: string | null;
  pools?: string[];
  routing_policy?: string;
  status?: string;
  security_work_authorized?: boolean;
  alias?: string | null;
}

/** `{ok:true}` envelope returned by the account PATCH/DELETE mutations. */
export interface OkResponse {
  ok: boolean;
}

export interface OAuthOnboardingStart {
  flow_id: string;
  authorize_url: string;
  expires_at: number;
}

export interface OAuthOnboardingResult {
  status: "completed";
  account_id: string;
}

export function startCodexOnboarding(initialPool?: string): Promise<OAuthOnboardingStart> {
  return fetchJson<OAuthOnboardingStart>("/api/account-onboarding/codex", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ initial_pool: initialPool || null }),
  });
}

export function completeCodexOnboarding(
  flowId: string,
  callbackUrl: string,
): Promise<OAuthOnboardingResult> {
  return fetchJson<OAuthOnboardingResult>(
    `/api/account-onboarding/${encodeURIComponent(flowId)}/callback`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ callback_url: callbackUrl }),
    },
  );
}

export function createPool(slug: string, accountIds: string[]): Promise<OkResponse & { slug: string }> {
  return fetchJson<OkResponse & { slug: string }>("/api/pools", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ slug, account_ids: accountIds }),
  });
}

export function patchAccount(id: string, body: AccountPatchBody): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/accounts/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function deleteAccount(id: string, opts?: { deleteHistory?: boolean }): Promise<OkResponse> {
  const qs = opts?.deleteHistory ? "?delete_history=true" : "";
  return fetchJson<OkResponse>(`/api/accounts/${encodeURIComponent(id)}${qs}`, { method: "DELETE" });
}

/** Body for `PATCH /api/settings`: one or more of the 10 live setting keys, each value typed per
 * that field's `kind` (see `SettingFieldView`) — a JSON number for `u32`/`secs`/`f64` kinds, a
 * JSON boolean for `bool` kinds. Never a string for these — the backend 400s on a wrong JSON type
 * (`write_api.rs::patch_settings_handler`). Validated all-or-nothing server-side: an unknown key
 * or a wrong-typed value rejects the WHOLE patch, no partial apply. */
export function patchSettings(body: Record<string, number | boolean>): Promise<OkResponse> {
  return fetchJson<OkResponse>("/api/settings", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

/** `POST /api/keys` — mint a new client proxy API key (`write_api.rs::create_key_handler`).
 * `label` omitted/undefined ⇒ no label. Returns the raw `key` plaintext exactly once; the caller
 * (`useCreateKey`) hands it straight to the page for a show-once modal — it must never be written
 * into the `["keys"]` query cache. */
export function createKey(label?: string): Promise<CreatedApiKey> {
  return fetchJson<CreatedApiKey>("/api/keys", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ label }),
  });
}

/** `PATCH /api/keys/{id}` — enable/disable a client proxy API key
 * (`write_api.rs::patch_key_handler`). Unknown id → `404` (surfaces as an `ApiError`). */
export function patchKey(id: string, body: { enabled: boolean }): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/keys/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function createProvider(body: CreateProviderBody): Promise<CustomProviderView> {
  return fetchJson<CustomProviderView>("/api/providers", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function createProviderCredential(
  providerId: string,
  body: { label: string; api_key: string; routing_weight?: number; max_concurrency?: number },
): Promise<ProviderCredentialView> {
  return fetchJson<ProviderCredentialView>(
    `/api/providers/${encodeURIComponent(providerId)}/credentials`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
  );
}

export function createProviderModel(
  providerId: string,
  body: CreateProviderModelBody,
): Promise<ProviderModelView> {
  return fetchJson<ProviderModelView>(`/api/providers/${encodeURIComponent(providerId)}/models`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function patchProviderEnabled(id: string, enabled: boolean): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/providers/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ enabled }),
  });
}

export function deleteProvider(id: string): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/providers/${encodeURIComponent(id)}`, { method: "DELETE" });
}

export function testProvider(id: string): Promise<ProviderTestResult> {
  return fetchJson<ProviderTestResult>(`/api/providers/${encodeURIComponent(id)}/test`, {
    method: "POST",
  });
}

export function patchProviderCredentialEnabled(
  id: string,
  enabled: boolean,
): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/provider-credentials/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ enabled }),
  });
}

export function deleteProviderCredential(id: string): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/provider-credentials/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export function patchProviderModel(
  id: string,
  patch: {
    enabled?: boolean;
    visible_in_codex?: boolean;
    visible_in_openai?: boolean;
  },
): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/provider-models/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
}

export function deleteProviderModel(id: string): Promise<OkResponse> {
  return fetchJson<OkResponse>(`/api/provider-models/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

// ---------------------------------------------------------------------------------------------
// Thin per-endpoint helpers (queries.ts wraps these in useQuery).
// ---------------------------------------------------------------------------------------------

export const api = {
  overview: () => fetchJson<OverviewView>("/api/overview"),
  overviewSeries: () => fetchJson<OverviewSeriesView>("/api/overview/series"),
  accounts: () => fetchJson<AccountView[]>("/api/accounts"),
  account: (id: string) => fetchJson<AccountDetailView>(`/api/accounts/${encodeURIComponent(id)}`),
  accountTrends: (id: string) =>
    fetchJson<TrendsView>(`/api/accounts/${encodeURIComponent(id)}/trends`),
  pools: () => fetchJson<PoolView[]>("/api/pools"),
  pace: () => fetchJson<PaceResponse>("/api/pace"),
  requests: (qs: string) => fetchJson<RequestsView>(`/api/requests${qs}`),
  sessions: (qs: string) => fetchJson<SessionsView>(`/api/sessions${qs}`),
  reports: (qs: string) => fetchJson<ReportsView>(`/api/reports${qs}`),
  settings: () => fetchJson<SettingsView>("/api/settings"),
  keys: () => fetchJson<ApiKeysView>("/api/keys"),
  providers: () => fetchJson<CustomProviderView[]>("/api/providers"),
  capabilities: () => fetchJson<CapabilitiesView>("/api/capabilities"),
  whoami: () => fetchJson<WhoamiView>("/api/whoami"),
};

/** `GET /api/pace` (admin-gated pool-wide weekly credit pace). Named alias for `api.pace`, kept as
 * its own export since `usePace` (queries.ts) is written against a `fetchPace()`-shaped fetcher —
 * same underlying `fetchJson` call as every other endpoint above. */
export const fetchPace = api.pace;
