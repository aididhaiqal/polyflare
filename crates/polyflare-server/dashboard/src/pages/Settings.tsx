// The Settings page (`/settings`): surfaces the running config and lets an admin live-edit the 10
// `class: "live"` `SettingFieldView` tunables via `GET`/`PATCH /api/settings` (Task 5, backend —
// see read_api.rs::settings_handler / write_api.rs::patch_settings_handler). The other 17 fields
// (8 restart-only + 9 fixed) are informational — rendered disabled, no PATCH surface for them.
//
// CONTENT-SAFETY: every field here is a config scalar (a count/seconds/percentage/flag/short
// string) — never a token or conversation content. `admin_token` is presence-only (`value` is
// ALWAYS `null` on the wire) and gets NO input control at all, just a static "configured" label.
//
// MUTATION FOUNDATION: reuses the same `useUpdateSettings()` shape the account-controls work
// (Task 7/8) established for `usePatchAccount` — one shared `useMutation` instance for the whole
// page (not one per field), a shared Toast on success/error, and `["settings"]` invalidation on
// success so the page picks up the backend's CLAMPED canonical value, not the raw submitted one.
// `pendingKey` (local state) tracks which field's Save button is in flight so only THAT row shows
// "Saving…" / gets disabled, even though the mutation object itself is shared.
import { useEffect, useState, type ReactNode } from "react";
import clsx from "clsx";

import type { SettingFieldView, SettingsView } from "../lib/api";
import { useSettings, useUpdateSettings } from "../lib/queries";
import { Card } from "../ui/Card";
import { Col, Grid } from "../ui/Grid";
import { AlertTriangle } from "../ui/icons";
import { Switch } from "../ui/Switch";

// ---------------------------------------------------------------------------------------------
// Grouping — the 10 live keys bucketed by area, per the task brief. `SettingsContent` looks each
// key up in the `GET /api/settings` response; a key absent from that response (shouldn't happen —
// the backend always emits all 27 `FIELD_SPECS` rows) is simply skipped, never fabricated.
// ---------------------------------------------------------------------------------------------

interface SectionDef {
  title: string;
  keys: string[];
}

const LIVE_SECTIONS: SectionDef[] = [
  {
    title: "Reliability & routing",
    keys: [
      "max_account_attempts",
      "starvation_wait_budget",
      "starvation_heartbeat",
      "wake_jitter_ms",
      "inflight_penalty_pct",
    ],
  },
  { title: "Streaming", keys: ["stream_idle_timeout", "soft_drain_enabled"] },
  { title: "Retention", keys: ["request_log_retention_days", "usage_history_retention_days"] },
  { title: "Flags", keys: ["live_logs"] },
];

/** Turns a snake_case field key into a display label — word-for-word from the key itself (e.g.
 * `max_account_attempts` -> "Max account attempts"), never an invented/paraphrased name. */
function humanizeKey(key: string): string {
  const words = key.split("_");
  return words.map((w, i) => (i === 0 ? w.charAt(0).toUpperCase() + w.slice(1) : w)).join(" ");
}

/** A short unit suffix shown next to a numeric field's input, derived from the field's own
 * `kind`/`key` — never fabricated. `secs`-kind fields are seconds; `wake_jitter_ms` and the two
 * `*_retention_days` fields spell their unit right in the key name already, so the suffix here
 * just makes it visible next to the control instead of only in the label text. */
function unitHint(field: SettingFieldView): string | null {
  if (field.kind === "secs") return "sec";
  if (field.key === "wake_jitter_ms") return "ms";
  if (field.key === "inflight_penalty_pct") return "%";
  if (field.key.endsWith("_days")) return "days";
  return null;
}

const CLASS_BADGE_CLASS: Record<SettingFieldView["class"], string> = {
  live: "bg-success/15 text-success",
  "restart-only": "bg-warn/15 text-warn",
  fixed: "bg-muted text-fg opacity-60",
};

function ClassBadge({ cls }: { cls: SettingFieldView["class"] }) {
  return (
    <span
      className={clsx(
        "inline-block shrink-0 whitespace-nowrap rounded px-1.5 py-0.5 text-[8px] font-bold uppercase leading-none tracking-wide",
        CLASS_BADGE_CLASS[cls],
      )}
    >
      {cls}
    </span>
  );
}

// ---------------------------------------------------------------------------------------------
// Page root
// ---------------------------------------------------------------------------------------------

export function Settings() {
  const { data, isLoading, isError, error, refetch } = useSettings();
  const updateSettings = useUpdateSettings();
  const [pendingKey, setPendingKey] = useState<string | null>(null);

  function handleSave(key: string, value: number | boolean) {
    setPendingKey(key);
    updateSettings.mutate(
      { [key]: value },
      {
        onSettled: () => setPendingKey(null),
      },
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <PageHeader />

      {isLoading ? (
        <SettingsSkeleton />
      ) : isError ? (
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex items-center gap-2 text-[12px] text-error">
              <AlertTriangle className="h-4 w-4 shrink-0" strokeWidth={1.9} />
              Couldn&apos;t load settings
              {error instanceof Error ? `: ${error.message}` : "."}
            </span>
            <button
              type="button"
              onClick={() => refetch()}
              className="shrink-0 rounded border border-border px-2.5 py-1 text-[11px] text-fg opacity-80 hover:opacity-100"
            >
              Retry
            </button>
          </div>
        </Card>
      ) : !data || data.fields.length === 0 ? (
        <Card>
          <p className="text-[11px] text-fg opacity-50">No settings reported by the server.</p>
        </Card>
      ) : (
        <SettingsContent data={data} pendingKey={pendingKey} onSave={handleSave} />
      )}
    </div>
  );
}

function PageHeader() {
  return (
    <div>
      <h1 className="text-lg font-semibold text-fg">Settings</h1>
      <p className="mt-0.5 text-[11px] text-fg opacity-60">
        The 10 live-editable runtime tunables, plus the full running configuration for reference.
      </p>
    </div>
  );
}

function SettingsSkeleton() {
  return (
    <Grid>
      {[0, 1, 2, 3].map((i) => (
        <Col key={i} span={6}>
          <Card>
            <div className="h-40 animate-pulse rounded bg-muted" />
          </Card>
        </Col>
      ))}
    </Grid>
  );
}

// ---------------------------------------------------------------------------------------------
// Content — 4 live-field sections (2 rows of `span=6`+`span=6`) + one `span=12` read-only section.
// ---------------------------------------------------------------------------------------------

interface SettingsContentProps {
  data: SettingsView;
  pendingKey: string | null;
  onSave: (key: string, value: number | boolean) => void;
}

function SettingsContent({ data, pendingKey, onSave }: SettingsContentProps) {
  const byKey = new Map(data.fields.map((f) => [f.key, f]));
  const restartOnly = data.fields.filter((f) => f.class === "restart-only");
  const fixed = data.fields.filter((f) => f.class === "fixed");

  return (
    <Grid>
      {LIVE_SECTIONS.map((section) => (
        <Col span={6} key={section.title}>
          <SettingsSection title={section.title}>
            {section.keys.map((key) => {
              const field = byKey.get(key);
              return field ? (
                <LiveFieldRow key={key} field={field} pendingKey={pendingKey} onSave={onSave} />
              ) : null;
            })}
          </SettingsSection>
        </Col>
      ))}

      <Col span={12}>
        <Card>
          <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">
            Other configuration
          </div>
          <p className="mt-1 text-[10px] text-fg opacity-45">
            Restart-only fields need a server restart to change; fixed fields are set once at
            startup and can&apos;t be edited here.
          </p>

          <div className="mt-3 grid grid-cols-1 gap-x-6 md:grid-cols-2">
            <div>
              <div className="text-[9px] uppercase tracking-wide text-fg opacity-50">
                Restart-only
              </div>
              <div className="mt-1.5 flex flex-col gap-0.5">
                {restartOnly.map((f) => (
                  <ReadOnlyFieldRow key={f.key} field={f} />
                ))}
              </div>
            </div>
            <div className="mt-4 md:mt-0">
              <div className="text-[9px] uppercase tracking-wide text-fg opacity-50">Fixed</div>
              <div className="mt-1.5 flex flex-col gap-0.5">
                {fixed.map((f) => (
                  <ReadOnlyFieldRow key={f.key} field={f} />
                ))}
              </div>
            </div>
          </div>
        </Card>
      </Col>
    </Grid>
  );
}

function SettingsSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Card>
      <div className="text-[10px] uppercase tracking-wide text-fg opacity-60">{title}</div>
      <div className="mt-2 flex flex-col gap-0.5">{children}</div>
    </Card>
  );
}

// ---------------------------------------------------------------------------------------------
// Live (editable) field row — a number input (u32/secs/f64 kinds) or a `Switch` (bool kind),
// pre-filled with the field's current value, a "live" badge, and a per-field Save button that
// PATCHes just that key. Local edit state (`raw`) resets whenever the server's `field.value`
// changes (a fresh load, or this SAME field's own successful save landing via the `["settings"]`
// invalidation) — so a clamp the backend applied is always reflected, never silently overridden by
// stale local state.
// ---------------------------------------------------------------------------------------------

function LiveFieldRow({
  field,
  pendingKey,
  onSave,
}: {
  field: SettingFieldView;
  pendingKey: string | null;
  onSave: (key: string, value: number | boolean) => void;
}) {
  const isBool = field.kind === "bool";
  const canonical = field.value ?? field.default;
  const [raw, setRaw] = useState(canonical);

  useEffect(() => {
    setRaw(canonical);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [field.value, field.default]);

  const dirty = raw !== canonical;
  const isPending = pendingKey === field.key;
  const unit = unitHint(field);
  const bounds =
    field.min !== null || field.max !== null
      ? `range ${field.min ?? "–"}–${field.max ?? "–"}`
      : undefined;

  function handleSave() {
    if (isBool) {
      onSave(field.key, raw === "true");
      return;
    }
    const n = Number(raw);
    if (raw.trim() === "" || Number.isNaN(n)) return;
    onSave(field.key, n);
  }

  return (
    <div className="flex items-center justify-between gap-2 py-1.5">
      <span className="flex min-w-0 items-center gap-1.5 text-[11px] text-fg opacity-80">
        <span className="truncate">{humanizeKey(field.key)}</span>
        <ClassBadge cls={field.class} />
      </span>
      <div className="flex shrink-0 items-center gap-1.5">
        {isBool ? (
          <Switch
            checked={raw === "true"}
            onCheckedChange={(v) => setRaw(v ? "true" : "false")}
            ariaLabel={humanizeKey(field.key)}
          />
        ) : (
          <div className="flex items-center gap-1" title={bounds}>
            <input
              type="number"
              inputMode="decimal"
              min={field.min ?? undefined}
              max={field.max ?? undefined}
              step={field.kind === "f64" ? "0.1" : "1"}
              value={raw}
              onChange={(e) => setRaw(e.target.value)}
              aria-label={humanizeKey(field.key)}
              className="w-20 rounded border border-border bg-bg px-2 py-1 text-right text-[10.5px] tabular-nums text-fg outline-none hover:border-accent focus:border-accent"
            />
            {unit && <span className="text-[9.5px] text-fg opacity-50">{unit}</span>}
          </div>
        )}
        <button
          type="button"
          onClick={handleSave}
          disabled={!dirty || isPending}
          className={clsx(
            "shrink-0 rounded border px-2 py-1 text-[10px] font-medium",
            dirty && !isPending
              ? "border-accent bg-accent/[0.12] text-accent hover:bg-accent/[0.2]"
              : "cursor-not-allowed border-border text-fg opacity-30",
          )}
        >
          {isPending ? "Saving…" : "Save"}
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------------------------
// Read-only field row — restart-only/fixed fields, disabled, showing `value ?? default` with a
// class badge. `admin_token` is special-cased: its `value` is ALWAYS `null` on the wire and it
// gets NO input control at all (per the content-safety constraint), just a static presence label.
// ---------------------------------------------------------------------------------------------

function ReadOnlyFieldRow({ field }: { field: SettingFieldView }) {
  const display = field.value ?? field.default;
  const isAdminToken = field.key === "admin_token";
  const isBool = field.kind === "bool";

  return (
    <div className="flex items-center justify-between gap-2 py-1">
      <span className="flex min-w-0 items-center gap-1.5 text-[10.5px] text-fg opacity-70">
        <span className="truncate">{humanizeKey(field.key)}</span>
        <ClassBadge cls={field.class} />
      </span>
      {isAdminToken ? (
        <span className="shrink-0 text-[10px] text-fg opacity-50">configured</span>
      ) : isBool ? (
        <Switch
          checked={display === "true"}
          onCheckedChange={() => {}}
          disabled
          ariaLabel={humanizeKey(field.key)}
        />
      ) : (
        <input
          disabled
          readOnly
          value={display === "" ? "—" : display}
          title={display}
          className="w-40 shrink-0 cursor-not-allowed truncate rounded border border-border bg-muted px-2 py-1 text-right text-[10px] tabular-nums text-fg opacity-50 outline-none"
        />
      )}
    </div>
  );
}
