// TanStack Query hooks over the typed API client (./api.ts). Every hook here is a thin wrapper:
// query key + fetchJson call + a refetch policy. Pages consume these instead of calling fetchJson
// directly, so the caching/refetch behavior lives in one place.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  api,
  ApiError,
  createKey,
  deleteAccount,
  patchAccount,
  patchKey,
  patchSettings,
  type AccountDetailView,
  type AccountPatchBody,
  type AccountView,
  type ApiKeysView,
  type CapabilitiesView,
  type CreatedApiKey,
  type OverviewSeriesView,
  type OverviewView,
  type PaceResponse,
  type PoolView,
  type RequestsQueryParams,
  type RequestsView,
  type ReportsView,
  type SessionsQueryParams,
  type SessionsView,
  type SettingsView,
  type TrendsView,
} from "./api";
import { useToast } from "../ui/Toast";

/** How often the landing-page/list views poll for fresh data while mounted. Per the task brief:
 * overview + the account/pool lists refetch every 30s; per-item detail views (account detail,
 * trends) don't — the fetch already happens whenever the user navigates in. */
const LIST_REFETCH_MS = 30_000;

export const queryKeys = {
  overview: ["overview"] as const,
  overviewSeries: ["overview", "series"] as const,
  accounts: ["accounts"] as const,
  account: (id: string) => ["accounts", id] as const,
  accountTrends: (id: string) => ["accounts", id, "trends"] as const,
  pools: ["pools"] as const,
  pace: ["pace"] as const,
  requests: (params: RequestsQueryParams) => ["requests", params] as const,
  sessions: (params: SessionsQueryParams) => ["sessions", params] as const,
  reports: (params: ReportsParams) => ["reports", params] as const,
  settings: ["settings"] as const,
  keys: ["keys"] as const,
  capabilities: ["capabilities"] as const,
};

export function useOverview() {
  return useQuery<OverviewView>({
    queryKey: queryKeys.overview,
    queryFn: api.overview,
    refetchInterval: LIST_REFETCH_MS,
    staleTime: LIST_REFETCH_MS,
  });
}

/** `GET /api/overview/series` — the overview's request-volume chart data. Same refetch/staleness
 * policy as `useOverview` (30s), since it's a landing-page-tile-adjacent series, not a detail view. */
export function useOverviewSeries() {
  return useQuery<OverviewSeriesView>({
    queryKey: queryKeys.overviewSeries,
    queryFn: api.overviewSeries,
    refetchInterval: LIST_REFETCH_MS,
    staleTime: LIST_REFETCH_MS,
  });
}

export function useAccounts() {
  return useQuery<AccountView[]>({
    queryKey: queryKeys.accounts,
    queryFn: api.accounts,
    refetchInterval: LIST_REFETCH_MS,
    staleTime: LIST_REFETCH_MS,
  });
}

export function useAccount(id: string) {
  return useQuery<AccountDetailView>({
    queryKey: queryKeys.account(id),
    queryFn: () => api.account(id),
    enabled: id.length > 0,
  });
}

export function useAccountTrends(id: string) {
  return useQuery<TrendsView>({
    queryKey: queryKeys.accountTrends(id),
    queryFn: () => api.accountTrends(id),
    enabled: id.length > 0,
    // 7-day history (see read_api.rs::TRENDS_LOOKBACK_SECS) — cheap to treat as fairly static
    // within a session; a hard refresh (or navigating away and back) is enough to pick up changes.
    staleTime: 60_000,
  });
}

export function usePools() {
  return useQuery<PoolView[]>({
    queryKey: queryKeys.pools,
    queryFn: api.pools,
    refetchInterval: LIST_REFETCH_MS,
    staleTime: LIST_REFETCH_MS,
  });
}

/** `GET /api/pace` — the pool-wide weekly credit pace forecast (admin-gated; `pace: null` when no
 * eligible/fresh/positive-capacity account exists). A landing-page summary, so it polls on the same
 * 30s cadence as `useOverview`/`usePools`, not the "fetch once, refresh on navigation" detail-view
 * policy `useAccountTrends` uses. */
export function usePace() {
  return useQuery<PaceResponse>({
    queryKey: queryKeys.pace,
    queryFn: api.pace,
    refetchInterval: LIST_REFETCH_MS,
    staleTime: LIST_REFETCH_MS,
  });
}

/** Serializes `RequestsQuery`'s filter/pagination fields into a `?`-prefixed query string,
 * omitting any field left `undefined`. Field names/order match `read_api.rs::RequestsQuery`
 * exactly (`limit,offset,account,provider,status_class,model,transport,since_ts`). */
function buildRequestsQueryString(params: RequestsQueryParams): string {
  const sp = new URLSearchParams();
  const order: Array<keyof RequestsQueryParams> = [
    "limit",
    "offset",
    "account",
    "provider",
    "status_class",
    "model",
    "transport",
    "since_ts",
  ];
  for (const key of order) {
    const value = params[key];
    if (value === undefined) continue;
    sp.set(key, String(value));
  }
  const qs = sp.toString();
  return qs ? `?${qs}` : "";
}

export function useRequests(params: RequestsQueryParams = {}) {
  return useQuery<RequestsView>({
    queryKey: queryKeys.requests(params),
    queryFn: () => api.requests(buildRequestsQueryString(params)),
    refetchInterval: LIST_REFETCH_MS,
    staleTime: LIST_REFETCH_MS,
  });
}

/** Serializes `SessionsQuery`'s pagination fields into a `?`-prefixed query string, omitting any
 * field left `undefined`. Field names/order match `read_api.rs::SessionsQuery` (`limit,offset`). */
function buildSessionsQueryString(params: SessionsQueryParams): string {
  const sp = new URLSearchParams();
  const order: Array<keyof SessionsQueryParams> = ["limit", "offset"];
  for (const key of order) {
    const value = params[key];
    if (value === undefined) continue;
    sp.set(key, String(value));
  }
  const qs = sp.toString();
  return qs ? `?${qs}` : "";
}

/** `GET /api/sessions` — the session→account affinity list. A list view, so it polls on the same
 * 30s cadence as the accounts/pools/requests lists (detail views don't poll). */
export function useSessions(params: SessionsQueryParams = {}) {
  return useQuery<SessionsView>({
    queryKey: queryKeys.sessions(params),
    queryFn: () => api.sessions(buildSessionsQueryString(params)),
    refetchInterval: LIST_REFETCH_MS,
    staleTime: LIST_REFETCH_MS,
  });
}

/** `GET /api/reports` query params — mirrors `read_api.rs::ReportsQuery`'s `range`/`dimension`/
 * `provider`, but `range`/`dimension` are required here (not optional, unlike the backend's own
 * Option<String> fields) since the Reports page's control bar always has a selected value — there
 * is no "absent" state to model client-side, the backend's absent-defaults-to-7d/model behavior is
 * simply never exercised by this hook. */
export interface ReportsParams {
  range: string;
  dimension: string;
  provider?: string;
}

/** Serializes `ReportsParams` into a `?`-prefixed query string, omitting `provider` when unset.
 * Field order matches `read_api.rs::ReportsQuery` (`range,dimension,provider`). */
function buildReportsQueryString(params: ReportsParams): string {
  const sp = new URLSearchParams();
  sp.set("range", params.range);
  sp.set("dimension", params.dimension);
  if (params.provider !== undefined) sp.set("provider", params.provider);
  return `?${sp.toString()}`;
}

/** `GET /api/reports` — the Reports page's composite analytics payload (time series + breakdown +
 * totals). 60s stale/refetch, not the 30s lists' cadence — reports drift slowly (bucketed
 * hourly/daily), so there's no value in polling as often as the live account/request lists do. */
export function useReports(params: ReportsParams) {
  return useQuery<ReportsView>({
    queryKey: queryKeys.reports(params),
    queryFn: () => api.reports(buildReportsQueryString(params)),
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
}

/** `GET /api/settings` — the Settings page's full running-config payload (10 live fields + every
 * restart-only/fixed field, 27 total). 60s stale/refetch — the same cadence `useReports` uses:
 * config drifts only on an admin edit (which invalidates this key directly, see
 * `useUpdateSettings`) or a restart, so there's no value polling it as often as the live
 * account/request lists. */
export function useSettings() {
  return useQuery<SettingsView>({
    queryKey: queryKeys.settings,
    queryFn: api.settings,
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
}

/** `GET /api/keys` — the API-Keys page's list of client proxy keys, redacted (never a hash or raw
 * key — see `ApiKeyView`). Same 60s stale/refetch cadence as `useSettings`: this list only changes
 * on an admin create/enable/disable (which invalidates `["keys"]` directly, see `useCreateKey`/
 * `useUpdateKey`) or another admin's edit landing, so there's no value polling it as often as the
 * live account/request lists. */
export function useKeys() {
  return useQuery<ApiKeysView>({
    queryKey: queryKeys.keys,
    queryFn: api.keys,
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
}

export function useCapabilities() {
  return useQuery<CapabilitiesView>({
    queryKey: queryKeys.capabilities,
    queryFn: api.capabilities,
    // Feature flags sourced from process env at server startup — effectively static for the life
    // of a running server, so never proactively refetch.
    staleTime: Infinity,
  });
}

// ---------------------------------------------------------------------------------------------
// Mutations — the frontend control-plane foundation (Task 5). Both hooks invalidate the account
// queries on success and surface a toast either way; later tasks (kebab menu, action bar) call
// these without touching react-query or the toast wiring directly.
// ---------------------------------------------------------------------------------------------

/** Extracts a human-readable message from a mutation failure: prefers `ApiError.body` when it's a
 * string (the backend's `bad_request` message, e.g. "alias must be 1..=64 characters"), falls back
 * to the HTTP status, then to a plain `Error.message`, then a generic string. */
function mutationErrorText(e: unknown): string {
  if (e instanceof ApiError) {
    return typeof e.body === "string" && e.body.length > 0 ? e.body : `HTTP ${e.status}`;
  }
  return e instanceof Error ? e.message : "unknown error";
}

export function usePatchAccount() {
  const qc = useQueryClient();
  const { toast } = useToast();
  return useMutation({
    mutationFn: (v: { id: string; body: AccountPatchBody }) => patchAccount(v.id, v.body),
    onSuccess: (_r, v) => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      qc.invalidateQueries({ queryKey: queryKeys.account(v.id) });
      toast({ title: "Account updated", variant: "success" });
    },
    onError: (e) => toast({ title: "Update failed", description: mutationErrorText(e), variant: "error" }),
  });
}

export function useDeleteAccount() {
  const qc = useQueryClient();
  const { toast } = useToast();
  return useMutation({
    mutationFn: (v: { id: string; deleteHistory?: boolean }) =>
      deleteAccount(v.id, { deleteHistory: v.deleteHistory }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.accounts });
      toast({ title: "Account deleted", variant: "success" });
    },
    onError: (e) => toast({ title: "Delete failed", description: mutationErrorText(e), variant: "error" }),
  });
}

/** `PATCH /api/settings` — live-edit one or more of the 10 live tunables (Settings page). Same
 * mutation shape as `usePatchAccount`: on success, invalidates `["settings"]` so the page refetches
 * the CLAMPED canonical value the backend actually stored (never just optimistically keeps the raw
 * submitted one — a `9999` submitted for a field clamped to `300` should show `300`, not `9999`),
 * plus a success toast; on error, the toast's description is the backend's 400 validation/clamp
 * message via `mutationErrorText`. */
export function useUpdateSettings() {
  const qc = useQueryClient();
  const { toast } = useToast();
  return useMutation({
    mutationFn: (body: Record<string, number | boolean>) => patchSettings(body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.settings });
      toast({ title: "Settings updated", variant: "success" });
    },
    onError: (e) =>
      toast({ title: "Update failed", description: mutationErrorText(e), variant: "error" }),
  });
}

/** `POST /api/keys` — mint a new client proxy API key (API-Keys page's "Create key" action). On
 * success, invalidates `["keys"]` (so the list picks up the new redacted row) and fires a success
 * toast, same as every other mutation here — but does NOT write the mutation's own result into any
 * query cache. `CreatedApiKey.key` is the raw plaintext, returned this one time only; the caller
 * (`Keys.tsx`) reads it off `mutate`'s `onSuccess` callback / the returned promise to open a
 * show-once modal, holding it in its own transient React state — it never touches `["keys"]`, which
 * only ever holds refetched, redacted `ApiKeyView[]` data. */
export function useCreateKey() {
  const qc = useQueryClient();
  const { toast } = useToast();
  return useMutation<CreatedApiKey, Error, string | undefined>({
    mutationFn: (label?: string) => createKey(label),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.keys });
      toast({ title: "Key created", variant: "success" });
    },
    onError: (e) => toast({ title: "Create failed", description: mutationErrorText(e), variant: "error" }),
  });
}

/** `PATCH /api/keys/{id}` — enable/disable a client proxy API key (per-row toggle). Same
 * mutation shape as `usePatchAccount`: invalidate `["keys"]` + success toast on success, the
 * backend's message via `mutationErrorText` on error (e.g. an unknown id's `404`). */
export function useUpdateKey() {
  const qc = useQueryClient();
  const { toast } = useToast();
  return useMutation({
    mutationFn: (v: { id: string; enabled: boolean }) => patchKey(v.id, { enabled: v.enabled }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: queryKeys.keys });
      toast({ title: "Key updated", variant: "success" });
    },
    onError: (e) => toast({ title: "Update failed", description: mutationErrorText(e), variant: "error" }),
  });
}
