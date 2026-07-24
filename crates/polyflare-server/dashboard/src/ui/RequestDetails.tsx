import { useRef, type ReactNode } from "react";
import { Link } from "react-router-dom";
import clsx from "clsx";

import type { RequestRowView } from "../lib/api";
import { accountDisplayLabel } from "../lib/accountDisplay";
import { compactNum, latency, tpsFmt } from "../lib/format";
import {
  requestOutcomeIsFailure,
  requestOutcomeIsSuccess,
  requestOutcomeLabel,
  requestOutcomeSource,
} from "../lib/requestOutcome";
import { ShieldedAccount } from "../privacy/ScreenShield";
import { ChevronRight, Lock, X } from "./icons";
import { ProviderTag } from "./ProviderTag";
import { ServiceTierBadge } from "./ServiceTierBadge";
import { useDialogA11y } from "./useDialogA11y";

function formatFullDateTime(unixSecs: number): string {
  const date = new Date(unixSecs * 1000);
  return `${date.toLocaleDateString()} ${date.toLocaleTimeString(undefined, { hour12: false })}`;
}

function requestStatusClass(row: RequestRowView): string {
  if (requestOutcomeIsFailure(row)) return "border-error/25 bg-error/15 text-error";
  if (requestOutcomeIsSuccess(row)) return "border-success/25 bg-success/15 text-success";
  if (row.status >= 300) return "border-warn/25 bg-warn/15 text-warn";
  return "border-border bg-muted text-fg";
}

function DetailMetric({ label, value, meta }: { label: string; value: string; meta?: string }) {
  return (
    <div className="min-w-0 px-3 py-2.5">
      <div className="text-[8px] font-bold uppercase tracking-[0.12em] text-fg opacity-40">
        {label}
      </div>
      <div className="mt-1 truncate text-[13px] font-semibold tabular-nums text-fg">{value}</div>
      {meta && <div className="mt-0.5 truncate text-[8.5px] text-fg opacity-40">{meta}</div>}
    </div>
  );
}

function DetailField({ label, value, mono }: { label: string; value: ReactNode; mono?: boolean }) {
  return (
    <div className="grid min-w-0 grid-cols-[78px_minmax(0,1fr)] gap-2 py-1 text-[10px]">
      <span className="text-fg opacity-40">{label}</span>
      <span className={clsx("min-w-0 break-words font-medium text-fg", mono && "font-mono")}>
        {value}
      </span>
    </div>
  );
}

export function RequestDetailPanel({
  row,
  accountLabel,
}: {
  row: RequestRowView;
  accountLabel?: string;
}) {
  const tokenValue = row.total_tokens === null ? "—" : compactNum(row.total_tokens);
  const cacheMeta =
    row.cached_input_tokens === null
      ? "cache read not reported"
      : `${compactNum(row.cached_input_tokens)} cached input`;
  const orchestration =
    (row.orchestration_input_tokens ?? 0) + (row.orchestration_output_tokens ?? 0);
  const modelContract = [row.model ?? "—", row.reasoning_effort].filter(Boolean).join(" · ");

  return (
    <div className="flex flex-col gap-3 px-4 py-3">
      <div className="grid grid-cols-2 divide-x divide-y divide-border overflow-hidden rounded-lg border border-border bg-muted/15 sm:grid-cols-4 sm:divide-y-0">
        <DetailMetric label="Duration" value={latency(row.duration_ms)} meta="end to end" />
        <DetailMetric label="First token" value={latency(row.ttft_ms)} meta="observed TTFT" />
        <DetailMetric
          label="Throughput"
          value={tpsFmt(row.tps)}
          meta="output tokens / generation time"
        />
        <DetailMetric label="Tokens" value={tokenValue} meta={cacheMeta} />
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        <section className="rounded-lg border border-border bg-card px-3 py-2.5">
          <div className="mb-1 text-[8.5px] font-bold uppercase tracking-[0.13em] text-signal opacity-65">
            Routing envelope
          </div>
          <DetailField
            label={row.target_kind === "credential" ? "Credential" : "Account"}
            value={
              row.target_kind === "credential" ? (
                <ShieldedAccount
                  id={row.provider_credential_id ?? "unassigned"}
                  label={accountLabel ?? row.provider_credential_id ?? "Unassigned"}
                />
              ) : row.account_id ? (
                <ShieldedAccount
                  id={row.account_id}
                  label={accountLabel ?? accountDisplayLabel(undefined, row.account_id)}
                />
              ) : (
                "Unassigned"
              )
            }
          />
          <DetailField label="Provider" value={row.provider} />
          <DetailField label="Tier" value={<ServiceTierBadge tier={row.service_tier} />} />
          <DetailField label="Model" value={modelContract} />
          <DetailField label="Transport" value={row.transport ?? "—"} />
          {row.upstream_model && row.upstream_model !== row.model && (
            <DetailField label="Upstream model" value={row.upstream_model} mono />
          )}
          <DetailField label="Upstream wire" value={row.upstream_transport ?? "—"} />
          {orchestration > 0 && (
            <DetailField
              label="Orchestration"
              value={`${compactNum(orchestration)} tokens`}
              mono
            />
          )}
        </section>
        <section className="rounded-lg border border-border bg-card px-3 py-2.5">
          <div className="mb-1 text-[8.5px] font-bold uppercase tracking-[0.13em] text-signal opacity-65">
            Request contract
          </div>
          <DetailField label="Request ID" value={row.request_id ?? "Not recorded"} mono />
          <DetailField label="Log row" value={`#${row.id}`} mono />
          <DetailField label="Endpoint" value={`${row.method} ${row.path}`} mono />
          <DetailField label="Requested" value={formatFullDateTime(row.requested_at)} />
          {row.status === 0 && (
            <DetailField
              label="Outcome"
              value={`${requestOutcomeLabel(row)} · ${
                requestOutcomeSource(row) === "imported" ? "imported evidence" : "HTTP status unavailable"
              }`}
            />
          )}
          {row.error_code && <DetailField label="Error code" value={row.error_code} mono />}
          <DetailField label="Agent" value={row.subagent ?? "main"} />
          <DetailField
            label="Session"
            value={
              row.session_key ? (
                <Link
                  to={`/sessions?session_key=${encodeURIComponent(row.session_key)}`}
                  className="font-mono text-accent hover:underline"
                >
                  {row.session_key.slice(0, 12)}
                </Link>
              ) : (
                "Not recorded"
              )
            }
          />
          <DetailField label="Aliased" value={row.aliased ? "yes" : "no"} />
        </section>
      </div>

      <section className="overflow-hidden rounded-lg border border-border bg-card">
        <div className="border-b border-border px-3 py-2">
          <div className="text-[8.5px] font-bold uppercase tracking-[0.13em] text-signal opacity-65">
            Token ledger
          </div>
          <div className="mt-0.5 text-[8.5px] text-fg opacity-40">
            Raw Responses usage and Codex-derived effective usage—subsets are never added twice.
          </div>
        </div>
        <div className="grid grid-cols-2 divide-x divide-y divide-border sm:grid-cols-3 lg:grid-cols-6 lg:divide-y-0">
          <DetailMetric
            label="API total"
            value={tokenValue}
            meta={row.reported_total_tokens === null ? "compatibility fallback" : "upstream reported"}
          />
          <DetailMetric
            label="Input"
            value={row.input_tokens === null ? "—" : compactNum(row.input_tokens)}
            meta={
              row.cached_input_tokens === null
                ? "cache read unknown"
                : `${compactNum(row.cached_input_tokens)} cache read`
            }
          />
          <DetailMetric
            label="Cache write"
            value={
              row.cache_write_input_tokens === null
                ? "—"
                : compactNum(row.cache_write_input_tokens)
            }
            meta="separate input detail"
          />
          <DetailMetric
            label="Output"
            value={row.output_tokens === null ? "—" : compactNum(row.output_tokens)}
            meta={
              row.reasoning_output_tokens === null
                ? "reasoning unknown"
                : `${compactNum(row.reasoning_output_tokens)} reasoning`
            }
          />
          <DetailMetric
            label="Visible output"
            value={
              row.visible_output_tokens === null ? "—" : compactNum(row.visible_output_tokens)
            }
            meta="output − reasoning"
          />
          <DetailMetric
            label="Effective"
            value={row.effective_tokens === null ? "—" : compactNum(row.effective_tokens)}
            meta="uncached input + output"
          />
        </div>
        <div className="flex flex-wrap gap-x-4 gap-y-1 border-t border-border px-3 py-2 text-[8.5px] text-fg opacity-45">
          <span>status: {row.usage_status ?? "unknown"}</span>
          <span>schema: {row.usage_schema ?? "unknown"}</span>
          <span>source: {row.usage_source ?? "unknown"}</span>
        </div>
      </section>

      <div className="flex items-start gap-2 rounded-lg border border-dashed border-border bg-muted/15 px-3 py-2 text-[9.5px] text-fg opacity-55">
        <Lock className="mt-0.5 h-3 w-3 shrink-0" strokeWidth={1.9} />
        <span>
          Content-safe evidence only. PolyFlare stores routing outcomes, timing, and token counts—not
          prompts or responses.
        </span>
      </div>
    </div>
  );
}

export function RequestDetailsDialog({
  row,
  accountLabel,
  explorerHref,
  onClose,
}: {
  row: RequestRowView | null;
  accountLabel?: string;
  explorerHref: string;
  onClose: () => void;
}) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  useDialogA11y(row !== null, onClose, dialogRef, closeRef);

  if (!row) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/65 p-3 backdrop-blur-[2px]"
      onClick={onClose}
    >
      <div
        ref={dialogRef}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-label={`Request ${row.request_id ?? row.id} routing evidence`}
        onClick={(event) => event.stopPropagation()}
        className="flex max-h-[88vh] w-full max-w-2xl flex-col overflow-hidden rounded-xl border border-border bg-card text-fg shadow-2xl outline-none"
      >
        <div className="flex items-start gap-3 border-b border-border px-4 py-3">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <ProviderTag provider={row.provider} />
              <ServiceTierBadge tier={row.service_tier} />
              <span
                className={clsx(
                  "rounded-full border px-2 py-0.5 text-[9px] font-bold",
                  requestStatusClass(row),
                )}
              >
                {requestOutcomeLabel(row)}
              </span>
            </div>
            <h2 className="mt-2 text-[17px] font-semibold tracking-[-0.025em] text-fg">
              Request {row.request_id ? row.request_id.slice(0, 8) : `#${row.id}`}
            </h2>
            <p className="mt-0.5 truncate font-mono text-[9.5px] text-fg opacity-45">
              {row.method} {row.path} · {row.model ?? "model not reported"}
            </p>
          </div>
          <button
            ref={closeRef}
            type="button"
            onClick={onClose}
            aria-label="Close request details"
            className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-border text-fg opacity-60 transition-colors hover:border-accent hover:text-accent hover:opacity-100"
          >
            <X className="h-4 w-4" strokeWidth={1.9} />
          </button>
        </div>

        <div className="min-h-0 overflow-y-auto">
          <RequestDetailPanel row={row} accountLabel={accountLabel} />
        </div>

        <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border px-4 py-3">
          {row.account_id ? (
            <Link
              to={`/accounts/${encodeURIComponent(row.account_id)}`}
              className="text-[10.5px] font-semibold text-fg no-underline opacity-60 hover:text-accent hover:opacity-100"
            >
              Inspect account
            </Link>
          ) : (
            <span />
          )}
          <Link
            to={explorerHref}
            className="inline-flex items-center rounded-lg bg-accent px-3 py-1.5 text-[10.5px] font-semibold text-white no-underline"
          >
            Investigate similar requests
            <ChevronRight className="ml-1 h-3 w-3" strokeWidth={2} />
          </Link>
        </div>
      </div>
    </div>
  );
}
