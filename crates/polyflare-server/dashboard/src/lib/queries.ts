// TanStack Query hooks over the typed API client (./api.ts). Every hook here is a thin wrapper:
// query key + fetchJson call + a refetch policy. Pages consume these instead of calling fetchJson
// directly, so the caching/refetch behavior lives in one place.

import { useQuery } from "@tanstack/react-query";

import {
  api,
  type AccountDetailView,
  type AccountView,
  type CapabilitiesView,
  type OverviewSeriesView,
  type OverviewView,
  type PaceResponse,
  type PoolView,
  type RequestsQueryParams,
  type RequestsView,
  type SessionsQueryParams,
  type SessionsView,
  type TrendsView,
} from "./api";

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

export function useCapabilities() {
  return useQuery<CapabilitiesView>({
    queryKey: queryKeys.capabilities,
    queryFn: api.capabilities,
    // Feature flags sourced from process env at server startup — effectively static for the life
    // of a running server, so never proactively refetch.
    staleTime: Infinity,
  });
}
