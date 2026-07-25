import { useEffect, useMemo, useState } from "react";
import clsx from "clsx";

import type { ResetCreditRecommendation, ResetPlanCandidateView } from "../lib/api";
import {
  useRedeemAccountResetCredit,
  useRedeemFleetResetCredits,
  useResetCreditPlan,
} from "../lib/queries";
import {
  getResetCreditRecoveryStorage,
  loadResetCreditRecovery,
  saveResetCreditRecovery,
  type ResetCreditRecovery,
} from "../lib/resetCreditRecovery";
import { Card } from "../ui/Card";
import { ConfirmDialog } from "../ui/ConfirmDialog";
import {
  AlertTriangle,
  CheckCircle2,
  Coins,
  RotateCcw,
} from "../ui/icons";

const ACTIONABLE = new Set<ResetCreditRecommendation>([
  "redeem_now",
  "redeem_before_expiry",
]);

const RECOMMENDATION_COPY: Record<
  ResetCreditRecommendation,
  { label: string; className: string }
> = {
  redeem_now: {
    label: "Redeem now",
    className: "border-accent/35 bg-accent/[0.09] text-accent",
  },
  redeem_before_expiry: {
    label: "Use before expiry",
    className: "border-warn/35 bg-warn/[0.08] text-warn",
  },
  hold: {
    label: "Hold",
    className: "border-signal/25 bg-signal/[0.06] text-signal",
  },
  wait_for_natural_reset: {
    label: "Natural reset soon",
    className: "border-success/25 bg-success/[0.06] text-success",
  },
  low_benefit: {
    label: "Low benefit",
    className: "border-border bg-muted/55 text-fg",
  },
  no_credit: {
    label: "No credit",
    className: "border-border bg-muted/40 text-fg",
  },
  unavailable: {
    label: "Needs fresh data",
    className: "border-error/25 bg-error/[0.06] text-error",
  },
};

function countdown(epoch: number | null, nowMs: number): string {
  if (epoch === null) return "not reported";
  const seconds = Math.max(0, epoch - Math.floor(nowMs / 1000));
  if (seconds < 60) return "< 1m";
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)}h`;
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  return `${days}d ${hours}h`;
}

function compactCredits(value: number): string {
  return new Intl.NumberFormat("en", {
    maximumFractionDigits: value >= 1_000 ? 0 : 1,
    notation: value >= 10_000 ? "compact" : "standard",
  }).format(value);
}

function displayName(candidate: ResetPlanCandidateView): string {
  return candidate.alias?.trim() || candidate.email;
}

export function ResetCredits() {
  const plan = useResetCreditPlan();
  const redeemOne = useRedeemAccountResetCredit();
  const redeemFleet = useRedeemFleetResetCredits();
  const recoveryStorage = useMemo(() => getResetCreditRecoveryStorage(window), []);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [confirm, setConfirm] = useState<{
    ids: string[];
    bestOnly: boolean;
    redeemRequestId: string;
  } | null>(null);
  const [recovery, setRecovery] = useState<ResetCreditRecovery | null>(() =>
    loadResetCreditRecovery(recoveryStorage),
  );
  const [recoveryError, setRecoveryError] = useState<string | null>(() =>
    recoveryStorage === null
      ? "Browser recovery storage is unavailable. Fleet redemption is disabled until storage access is restored."
      : null,
  );
  const [nowMs, setNowMs] = useState(() => Date.now());

  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 5_000);
    return () => clearInterval(id);
  }, []);

  const candidates = plan.data?.candidates ?? [];
  const recommendedIds = useMemo(
    () =>
      candidates
        .filter((candidate) => ACTIONABLE.has(candidate.recommendation))
        .map((candidate) => candidate.account_id),
    [candidates],
  );
  const selectedRows = candidates.filter((candidate) => selected.has(candidate.account_id));
  const recoverableSelected = selectedRows.reduce(
    (total, candidate) => total + candidate.recoverable_credits,
    0,
  );
  const best = candidates.find(
    (candidate) =>
      candidate.available_credits > 0 && ACTIONABLE.has(candidate.recommendation),
  );
  const busy = redeemOne.isPending || redeemFleet.isPending;
  const newOperationBlocked = busy || recovery !== null;
  const newFleetOperationBlocked = newOperationBlocked || recoveryStorage === null;

  function toggle(accountId: string) {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(accountId)) next.delete(accountId);
      else next.add(accountId);
      return next;
    });
  }

  function runConfirmed() {
    if (!confirm) return;
    const ids = confirm.ids;
    if (confirm.bestOnly && ids[0]) {
      redeemOne.mutate(
        {
          accountId: ids[0],
          redeemRequestId: confirm.redeemRequestId,
          requireRecommended: true,
        },
        {
          onSuccess: () => {
            setConfirm(null);
            setSelected(new Set());
          },
        },
      );
      return;
    }
    runFleet(ids, confirm.redeemRequestId);
  }

  function runFleet(ids: string[], redeemRequestId: string) {
    const pendingRecovery: ResetCreditRecovery = {
      ids,
      redeemRequestId,
      response: {
        results: [],
        errors: ids.map((accountId) => ({
          account_id: accountId,
          message: "Awaiting a durable terminal result",
        })),
      },
    };
    if (!saveResetCreditRecovery(recoveryStorage, pendingRecovery)) {
      setRecoveryError(
        "The exact fleet operation could not be saved. Nothing was redeemed; restore browser storage access and try again.",
      );
      setConfirm(null);
      return;
    }
    setRecoveryError(null);
    setRecovery(pendingRecovery);
    setConfirm(null);
    redeemFleet.mutate(
      { accountIds: ids, redeemRequestId },
      {
        onSuccess: (response) => {
          if (response.errors.length > 0) {
            const nextRecovery = { ids, redeemRequestId, response };
            if (saveResetCreditRecovery(recoveryStorage, nextRecovery)) {
              setRecovery(nextRecovery);
              setRecoveryError(null);
            } else {
              setRecoveryError(
                "The latest fleet result could not be saved. Retry the retained exact operation before starting another redemption.",
              );
            }
            return;
          }
          if (saveResetCreditRecovery(recoveryStorage, null)) {
            setRecovery(null);
            setRecoveryError(null);
            setSelected(new Set());
          } else {
            setRecoveryError(
              "The completed operation could not be cleared from recovery storage. Retry the exact operation after storage access is restored.",
            );
          }
        },
      },
    );
  }

  if (plan.isLoading) return <ResetCreditsSkeleton />;

  if (plan.isError || !plan.data) {
    return (
      <div className="flex flex-col gap-3">
        <PageHeader />
        <Card>
          <div className="flex items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4" />
              Reset-credit state could not be loaded.
            </span>
            <button
              type="button"
              onClick={() => plan.refetch()}
              className="rounded-lg border border-border px-3 py-1.5 text-[11px] font-semibold"
            >
              Retry
            </button>
          </div>
        </Card>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <PageHeader
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              disabled={recommendedIds.length === 0 || newOperationBlocked}
              onClick={() => setSelected(new Set(recommendedIds))}
              className="rounded-lg border border-border bg-card px-3 py-1.5 text-[11px] font-semibold text-fg disabled:opacity-35"
            >
              Select recommended
            </button>
            <button
              type="button"
              disabled={!best || newOperationBlocked}
              onClick={() =>
                best &&
                setConfirm({
                  ids: [best.account_id],
                  bestOnly: true,
                  redeemRequestId: crypto.randomUUID(),
                })
              }
              className="flex items-center gap-1.5 rounded-lg bg-accent px-3 py-1.5 text-[11px] font-semibold text-white shadow-[0_0_20px_hsl(var(--accent)/0.18)] disabled:opacity-35"
            >
              <RotateCcw className="h-3.5 w-3.5" />
              Redeem best
            </button>
          </div>
        }
      />

      <section className="relative overflow-hidden rounded-2xl border border-accent/20 bg-[linear-gradient(118deg,hsl(var(--card))_0%,hsl(var(--card))_54%,hsl(var(--accent)/0.09)_100%)] p-5 shadow-[0_18px_46px_hsl(var(--surface-shadow)/0.16)]">
        <div className="pointer-events-none absolute -right-14 -top-20 h-56 w-56 rounded-full border border-accent/15" />
        <div className="pointer-events-none absolute -right-4 -top-10 h-36 w-36 rounded-full border border-signal/15" />
        <div className="relative grid gap-5 lg:grid-cols-[1.35fr_1fr] lg:items-end">
          <div>
            <div className="mb-3 flex items-center gap-2 text-[9px] font-bold uppercase tracking-[0.2em] text-signal">
              <span className="h-1.5 w-1.5 rounded-full bg-signal shadow-[0_0_10px_hsl(var(--signal))]" />
              Fleet reset reserve
            </div>
            <div className="flex items-baseline gap-3">
              <span className="text-5xl font-semibold tracking-[-0.055em] text-fg tabular-nums">
                {plan.data.total_credits}
              </span>
              <span className="max-w-40 text-[12px] leading-relaxed text-fg opacity-55">
                banked resets across {plan.data.accounts_with_credits} accounts
              </span>
            </div>
            <p className="mt-4 max-w-xl text-[12px] leading-relaxed text-fg opacity-65">
              PolyFlare ranks real capacity recovered against the time left until each account
              refills naturally. Credits are spent sequentially and the fleet is measured again
              after every reset.
            </p>
          </div>
          <div className="grid grid-cols-2 gap-px overflow-hidden rounded-xl border border-border/80 bg-border/80">
            <HeroMetric
              label="Recommended now"
              value={String(plan.data.recommended_now)}
              meta="high value or expiring"
              tone="accent"
            />
            <HeroMetric
              label="Selected recovery"
              value={compactCredits(recoverableSelected)}
              meta="weekly capacity credits"
              tone="signal"
            />
          </div>
        </div>
      </section>

      <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_280px]">
        <Card className="p-0">
          <div className="flex items-center justify-between border-b border-border/80 px-4 py-3">
            <div>
              <h2 className="text-[12px] font-semibold text-fg">Account opportunities</h2>
              <p className="mt-0.5 text-[10px] text-fg opacity-45">
                Ranked by useful capacity, expiry, and natural reset timing
              </p>
            </div>
            <span className="text-[10px] tabular-nums text-fg opacity-45">
              updated{" "}
              {Math.max(0, Math.floor(nowMs / 1000) - plan.data.generated_at) < 60
                ? `${Math.max(0, Math.floor(nowMs / 1000) - plan.data.generated_at)}s ago`
                : `${Math.floor(
                    (Math.max(0, Math.floor(nowMs / 1000) - plan.data.generated_at)) / 60,
                  )}m ago`}
            </span>
          </div>
          {candidates.length === 0 ? (
            <div className="px-5 py-10 text-center">
              <CheckCircle2 className="mx-auto h-6 w-6 text-success" />
              <p className="mt-3 text-[12px] font-semibold text-fg">No reset credits discovered</p>
              <p className="mt-1 text-[10.5px] text-fg opacity-50">
                PolyFlare checks eligible Codex accounts once per minute.
              </p>
            </div>
          ) : (
            <div className="divide-y divide-border/70">
              {candidates.map((candidate, index) => (
                <CandidateRow
                  key={candidate.account_id}
                  candidate={candidate}
                  rank={index + 1}
                  checked={selected.has(candidate.account_id)}
                  nowMs={nowMs}
                  interactionDisabled={recovery !== null}
                  onToggle={() => toggle(candidate.account_id)}
                />
              ))}
            </div>
          )}
        </Card>

        <Card className="h-fit">
          <div className="flex items-center gap-2">
            <Coins className="h-4 w-4 text-signal" />
            <h2 className="text-[12px] font-semibold text-fg">Execution plan</h2>
          </div>
          <div className="mt-4 space-y-3">
            <SideMetric label="Accounts selected" value={String(selected.size)} />
            <SideMetric
              label="Capacity recovered"
              value={compactCredits(recoverableSelected)}
            />
            <SideMetric
              label="Natural-reset waits"
              value={String(
                selectedRows.filter(
                  (candidate) => candidate.recommendation === "wait_for_natural_reset",
                ).length,
              )}
              warn
            />
          </div>
          <div className="my-4 h-px bg-border/80" />
          <p className="text-[10.5px] leading-relaxed text-fg opacity-55">
            Selected accounts run one at a time. A failure stops only that account; successful
            resets remain recorded and usage refreshes immediately.
          </p>
          {recoveryError && (
            <div className="mt-4 rounded-lg border border-error/30 bg-error/[0.06] p-3 text-[10px] leading-relaxed text-error">
              {recoveryError}
            </div>
          )}
          {recovery && (
            <div className="mt-4 rounded-lg border border-warn/30 bg-warn/[0.06] p-3">
              <div className="flex items-center gap-1.5 text-[10.5px] font-semibold text-warn">
                <AlertTriangle className="h-3.5 w-3.5" />
                Recovery retained
              </div>
              <p className="mt-1.5 text-[10px] leading-relaxed text-fg opacity-65">
                {recovery.response.results.length} completed; {recovery.response.errors.length}{" "}
                uncertain or failed. Retry uses the exact same request ID and reviewed account
                list, so completed accounts replay safely.
              </p>
              <button
                type="button"
                disabled={busy}
                onClick={() => runFleet(recovery.ids, recovery.redeemRequestId)}
                className="mt-2.5 w-full rounded-md bg-warn px-2.5 py-1.5 text-[10.5px] font-semibold text-white disabled:opacity-35"
              >
                Retry exact operation
              </button>
            </div>
          )}
          <button
            type="button"
            disabled={selected.size === 0 || newFleetOperationBlocked}
            onClick={() =>
              setConfirm({
                ids: [...selected],
                bestOnly: false,
                redeemRequestId: crypto.randomUUID(),
              })
            }
            className="mt-4 w-full rounded-lg bg-accent px-3 py-2 text-[11px] font-semibold text-white disabled:opacity-35"
          >
            Redeem selected
          </button>
          {selected.size > 0 && (
            <button
              type="button"
              disabled={busy}
              onClick={() => setSelected(new Set())}
              className="mt-2 w-full rounded-lg border border-border px-3 py-2 text-[10.5px] font-medium text-fg opacity-60 hover:opacity-100"
            >
              Clear selection
            </button>
          )}
        </Card>
      </div>

      <ConfirmDialog
        open={confirm !== null}
        onOpenChange={(open) => !open && !busy && setConfirm(null)}
        title={confirm?.bestOnly ? "Redeem the best reset?" : "Redeem selected resets?"}
        description={
          confirm ? (
            <>
              This spends {confirm.ids.length} banked reset{" "}
              {confirm.ids.length === 1 ? "credit" : "credits"}. PolyFlare will fetch each
              account&apos;s live credit state again before spending it.
            </>
          ) : undefined
        }
        confirmLabel={busy ? "Redeeming…" : "Redeem credits"}
        busy={busy}
        onConfirm={runConfirmed}
      />
    </div>
  );
}

function PageHeader({ actions }: { actions?: React.ReactNode }) {
  return (
    <header className="flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 className="text-lg font-semibold tracking-tight text-fg">Reset credits</h1>
        <p className="mt-1 text-[11px] text-fg opacity-50">
          Recover capacity where it buys the fleet the most useful time.
        </p>
      </div>
      {actions}
    </header>
  );
}

function HeroMetric({
  label,
  value,
  meta,
  tone,
}: {
  label: string;
  value: string;
  meta: string;
  tone: "accent" | "signal";
}) {
  return (
    <div className="bg-card/75 p-3.5 backdrop-blur">
      <div className="text-[8.5px] font-bold uppercase tracking-[0.15em] text-fg opacity-40">
        {label}
      </div>
      <div
        className={clsx(
          "mt-2 text-2xl font-semibold tracking-tight tabular-nums",
          tone === "accent" ? "text-accent" : "text-signal",
        )}
      >
        {value}
      </div>
      <div className="mt-1 text-[9.5px] text-fg opacity-45">{meta}</div>
    </div>
  );
}

function CandidateRow({
  candidate,
  rank,
  checked,
  nowMs,
  interactionDisabled,
  onToggle,
}: {
  candidate: ResetPlanCandidateView;
  rank: number;
  checked: boolean;
  nowMs: number;
  interactionDisabled: boolean;
  onToggle: () => void;
}) {
  const copy = RECOMMENDATION_COPY[candidate.recommendation];
  const disabled =
    interactionDisabled ||
    candidate.available_credits <= 0 ||
    candidate.recommendation === "unavailable";
  const natural = countdown(candidate.weekly_reset_at, nowMs);
  const expiry = countdown(candidate.earliest_credit_expires_at, nowMs);

  return (
    <label
      className={clsx(
        "grid cursor-pointer gap-3 px-4 py-3.5 transition-colors hover:bg-muted/35 sm:grid-cols-[28px_minmax(180px,1.25fr)_minmax(170px,1fr)_120px] sm:items-center",
        disabled && "cursor-not-allowed opacity-55",
        checked && "bg-accent/[0.035]",
      )}
    >
      <div className="flex items-center gap-2">
        <input
          type="checkbox"
          checked={checked}
          disabled={disabled}
          onChange={onToggle}
          className="h-3.5 w-3.5 accent-[hsl(var(--accent))]"
        />
        <span className="text-[9px] font-semibold tabular-nums text-fg opacity-30">
          {String(rank).padStart(2, "0")}
        </span>
      </div>
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <span className="truncate text-[12px] font-semibold text-fg">
            {displayName(candidate)}
          </span>
          <span className={clsx("rounded-full border px-2 py-0.5 text-[8.5px] font-bold", copy.className)}>
            {copy.label}
          </span>
        </div>
        <div className="mt-1 truncate text-[9.5px] text-fg opacity-45">
          {candidate.alias ? candidate.email : candidate.account_id} · {candidate.plan_type} ·{" "}
          {candidate.pools.length > 0 ? candidate.pools.join(", ") : "unpooled"}
        </div>
        <p className="mt-1.5 text-[9.5px] leading-relaxed text-fg opacity-60">
          {candidate.reason}
        </p>
      </div>
      <div className="grid grid-cols-2 gap-2">
        <ClockCell label="Natural refill" value={natural} tone="success" />
        <ClockCell label="Credit expiry" value={expiry} tone="warn" />
      </div>
      <div className="sm:text-right">
        <div className="text-[17px] font-semibold tracking-tight text-fg tabular-nums">
          {compactCredits(candidate.recoverable_credits)}
        </div>
        <div className="text-[8.5px] uppercase tracking-wide text-fg opacity-40">
          credits recovered
        </div>
        <div className="mt-1 text-[9px] text-signal">
          {candidate.weekly_used_percent.toFixed(0)}% weekly used ·{" "}
          {candidate.available_credits} banked
        </div>
      </div>
    </label>
  );
}

function ClockCell({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "success" | "warn";
}) {
  return (
    <div className="rounded-lg border border-border/75 bg-bg/35 px-2.5 py-2">
      <div className="text-[8px] font-bold uppercase tracking-[0.12em] text-fg opacity-35">
        {label}
      </div>
      <div
        className={clsx(
          "mt-1 text-[11px] font-semibold tabular-nums",
          tone === "success" ? "text-success" : "text-warn",
        )}
      >
        {value}
      </div>
    </div>
  );
}

function SideMetric({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-[10px] text-fg opacity-50">{label}</span>
      <span className={clsx("text-[12px] font-semibold tabular-nums", warn ? "text-warn" : "text-fg")}>
        {value}
      </span>
    </div>
  );
}

function ResetCreditsSkeleton() {
  return (
    <div className="animate-pulse space-y-3">
      <div className="h-10 w-56 rounded-lg bg-muted" />
      <div className="h-52 rounded-2xl bg-card" />
      <div className="h-96 rounded-xl bg-card" />
    </div>
  );
}
