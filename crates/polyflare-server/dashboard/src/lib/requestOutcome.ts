import type { RequestRowView } from "./api";

/**
 * Native PolyFlare rows carry a real HTTP status. Imported codex-lb rows carry `status = 0`
 * because the source never stored HTTP status, plus a bounded `success`/`error` outcome. Keep
 * that distinction explicit so a sentinel is never presented as HTTP 0 or counted as success.
 */
export function requestOutcomeLabel(row: RequestRowView): string {
  if (row.protocol_outcome != null) return row.protocol_outcome.replace("_", " ");
  if (row.status > 0) return String(row.status);
  if (row.outcome === "success") return "success";
  if (row.outcome === "error") return "error";
  return "unknown";
}

export function requestOutcomeIsFailure(row: RequestRowView): boolean {
  if (row.protocol_outcome != null) return row.protocol_outcome !== "completed";
  return row.status >= 400 || (row.status === 0 && row.outcome === "error");
}

export function requestOutcomeIsSuccess(row: RequestRowView): boolean {
  if (row.protocol_outcome != null) return row.protocol_outcome === "completed";
  return (
    (row.status >= 100 && row.status < 300) ||
    (row.status === 0 && row.outcome === "success")
  );
}

export function requestOutcomeSource(
  row: RequestRowView,
): "protocol" | "http" | "imported" | "unknown" {
  if (row.protocol_outcome != null) return "protocol";
  if (row.status > 0) return "http";
  if (row.outcome !== null) return "imported";
  return "unknown";
}
