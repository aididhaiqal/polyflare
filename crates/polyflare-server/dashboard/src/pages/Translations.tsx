import { useMemo, useState, type FormEvent } from "react";
import clsx from "clsx";

import type {
  CustomProviderView,
  TranslationMatchKind,
  TranslationReasoningEffort,
  TargetCapacityView,
  TranslationRouteInput,
  TranslationRouteView,
} from "../lib/api";
import { targetCapacityKey } from "../lib/api";
import { latency, relTime } from "../lib/format";
import {
  useDeleteTranslationRoute,
  useSaveTranslationRoute,
  useTestTranslationRoute,
  useTranslationRoutes,
  useProviders,
  useBuiltinModels,
} from "../lib/queries";
import {
  duplicateTranslationRoute,
  emptyTranslationRoute,
  nextTranslationPriority,
  routeInputFrom,
  unservableReason,
} from "../lib/translationRoutes";
import { Card } from "../ui/Card";
import { ConfirmDialog } from "../ui/ConfirmDialog";
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  Copy,
  FlaskConical,
  Pencil,
  Plus,
  Search,
  Trash2,
  X,
} from "../ui/icons";
import { Switch } from "../ui/Switch";

const INPUT =
  "h-9 w-full rounded-lg border border-border bg-bg/65 px-3 text-[11px] text-fg outline-none transition focus:border-accent/60";
const LABEL = "mb-1 block text-[9px] font-bold uppercase tracking-[0.13em] text-fg opacity-50";
const EFFORTS: Array<TranslationReasoningEffort | ""> = [
  "",
  "none",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
];

type EditorState = {
  id?: string;
  draft: TranslationRouteInput;
};

export function Translations() {
  const translations = useTranslationRoutes();
  const save = useSaveTranslationRoute();
  const remove = useDeleteTranslationRoute();
  const testMatch = useTestTranslationRoute();
  const providers = useProviders();
  const builtinModelsQuery = useBuiltinModels();
  const [editor, setEditor] = useState<EditorState | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<TranslationRouteView | null>(null);
  const [testModel, setTestModel] = useState("claude-opus-4-1-20250805");
  const [testProtocol, setTestProtocol] = useState<
    "anthropic_messages" | "openai_responses"
  >("anthropic_messages");

  const routes = translations.data?.routes ?? [];
  const enabledCount = routes.filter((route) => route.enabled).length;
  const nextPriority = useMemo(() => nextTranslationPriority(routes), [routes]);

  const saveDraft = (event: FormEvent) => {
    event.preventDefault();
    if (!editor) return;
    save.mutate(
      { id: editor.id, route: editor.draft },
      { onSuccess: () => setEditor(null) },
    );
  };

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <div className="mb-1 flex items-center gap-2 text-[8px] font-bold uppercase tracking-[0.2em] text-signal">
            <FlaskConical className="h-3.5 w-3.5" />
            Protocol translation
          </div>
          <h1 className="text-lg font-semibold text-fg">Translation routes</h1>
          <p className="mt-0.5 max-w-2xl text-[11px] text-fg opacity-60">
            Match model names on either client protocol, route to a built-in or custom provider,
            and translate the streamed result back to the caller.
          </p>
        </div>
        <button
          type="button"
          onClick={() =>
            setEditor({ draft: emptyTranslationRoute(nextPriority) })
          }
          className="flex items-center gap-2 rounded-lg bg-accent px-3 py-2 text-[11px] font-semibold text-white"
        >
          <Plus className="h-3.5 w-3.5" />
          Add route
        </button>
      </div>

      <RoutingFork enabledCount={enabledCount} />

      {editor && (
        <RouteEditor
          state={editor}
          providers={providers.data ?? []}
          builtinModels={builtinModelsQuery.data?.models ?? {}}
          pending={save.isPending}
          onChange={setEditor}
          onCancel={() => setEditor(null)}
          onSubmit={saveDraft}
        />
      )}

      {translations.isLoading ? (
        <Card>
          <div className="h-40 animate-pulse rounded-lg bg-muted" />
        </Card>
      ) : translations.isError ? (
        <Card>
          <div className="flex items-center gap-2 text-[11px] text-error">
            <AlertTriangle className="h-4 w-4" />
            Couldn&apos;t load translation routes.
          </div>
        </Card>
      ) : (
        <Card className="p-0">
          <div className="flex items-center justify-between border-b border-border/70 px-4 py-3">
            <div>
              <h2 className="text-[12px] font-semibold text-fg">Routing order</h2>
              <p className="mt-0.5 text-[9.5px] text-fg opacity-45">
                {routes.length} configured · {enabledCount} active
              </p>
            </div>
          </div>
          {routes.length ? (
            <div className="divide-y divide-border/60">
              {routes.map((route) => (
                <RouteRow
                  key={route.id}
                  route={route}
                  targetLabel={
                    route.target_kind === "builtin_provider"
                      ? route.target_provider_id
                      : providers.data?.find((provider) => provider.id === route.target_provider_id)
                          ?.display_name ?? "Unavailable custom provider"
                  }
                  capacity={translations.data?.target_capacity?.[targetCapacityKey(route)]}
                  pending={save.isPending || remove.isPending}
                  onToggle={(enabled) =>
                    save.mutate({ id: route.id, route: { ...routeInputFrom(route), enabled } })
                  }
                  onEdit={() => setEditor({ id: route.id, draft: routeInputFrom(route) })}
                  onDuplicate={() => setEditor({ draft: duplicateTranslationRoute(route) })}
                  onDelete={() => setDeleteTarget(route)}
                />
              ))}
            </div>
          ) : (
            <div className="px-4 py-10 text-center text-[11px] text-fg opacity-50">
              No routes yet. Every request keeps its own protocol, and a real Claude client is
              forwarded byte for byte.
            </div>
          )}
        </Card>
      )}

      <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)]">
        <Card className="gap-3">
          <div className="flex items-center gap-2">
            <Search className="h-4 w-4 text-signal" />
            <div>
              <h2 className="text-[12px] font-semibold text-fg">Test a model name</h2>
              <p className="text-[9.5px] text-fg opacity-45">
                Uses the same server-side matcher as live ingress.
              </p>
            </div>
          </div>
          <form
            className="grid grid-cols-1 gap-2 sm:grid-cols-[170px_minmax(0,1fr)_auto]"
            onSubmit={(event) => {
              event.preventDefault();
              testMatch.mutate({ source_protocol: testProtocol, model: testModel });
            }}
          >
            <select
              className={INPUT}
              value={testProtocol}
              onChange={(event) =>
                setTestProtocol(
                  event.target.value as "anthropic_messages" | "openai_responses",
                )
              }
            >
              <option value="anthropic_messages">Anthropic Messages</option>
              <option value="openai_responses">OpenAI Responses</option>
            </select>
            <input
              className={INPUT}
              value={testModel}
              onChange={(event) => setTestModel(event.target.value)}
              placeholder="claude-opus-4-1-20250805"
              required
            />
            <button
              type="submit"
              disabled={testMatch.isPending}
              className="h-9 shrink-0 rounded-lg border border-accent/35 bg-accent/10 px-4 text-[10px] font-semibold text-accent disabled:opacity-40"
            >
              Test match
            </button>
          </form>
          {testMatch.data && (
            <div
              className={clsx(
                "rounded-lg border px-3 py-2.5 text-[10px]",
                testMatch.data.matched
                  ? "border-success/25 bg-success/[0.06] text-success"
                  : "border-border bg-bg/35 text-fg",
              )}
            >
              {testMatch.data.route ? (
                <div className="flex flex-col gap-1.5">
                  <div className="flex items-start gap-2">
                    <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                    <span>
                      Rewritten by <strong>{testMatch.data.route.name}</strong> →{" "}
                      <span className="font-mono">{testMatch.data.route.target_model}</span>
                    </span>
                  </div>
                  {testMatch.data.target_capacity &&
                    testMatch.data.target_capacity.eligible === 0 && (
                      <div className="flex items-start gap-2 text-error">
                        <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                        <span>
                          {unservableReason(testMatch.data.target_capacity)} This request would fail
                          with no eligible account.
                        </span>
                      </div>
                    )}
                </div>
              ) : (
                <div className="flex items-start gap-2">
                  <ArrowRight className="mt-0.5 h-3.5 w-3.5 shrink-0 opacity-50" />
                  <span>
                    Not rewritten. The request keeps its own protocol, and a real Claude client is
                    forwarded byte for byte.
                  </span>
                </div>
              )}
            </div>
          )}
        </Card>

        <RecentTranslations rows={translations.data?.recent_requests ?? []} />
      </div>

      <ConfirmDialog
        open={deleteTarget !== null}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
        title={`Delete ${deleteTarget?.name ?? "translation route"}?`}
        description="Matching requests fall through to the next route, or stop being rewritten at all and pass through on their own protocol."
        confirmLabel="Delete route"
        danger
        busy={remove.isPending}
        onConfirm={() => {
          if (!deleteTarget) return;
          remove.mutate(deleteTarget.id, { onSuccess: () => setDeleteTarget(null) });
        }}
      />
    </div>
  );
}

/**
 * What happens to an incoming request — the page's thesis.
 *
 * Drawn as a fork rather than a pipeline because that is what the router actually does: a request
 * either matches a route and is rewritten into another protocol, or it matches nothing and keeps
 * its own. The second branch is not a gap. For a real Claude client it is the better outcome, since
 * an unrewritten request is forwarded byte for byte, so it gets equal weight here.
 */
function RoutingFork({ enabledCount }: { enabledCount: number }) {
  return (
    <Card className="gap-3 border-accent/20">
      <div className="text-[8px] font-bold uppercase tracking-[0.2em] text-fg opacity-40">
        What happens to an incoming request
      </div>
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        <div className="rounded-lg border border-accent/25 bg-accent/[0.06] px-3 py-2.5">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold text-accent">
            <FlaskConical className="h-3.5 w-3.5" />
            Matches a route
          </div>
          <p className="mt-1 text-[10px] leading-5 text-fg opacity-60">
            Rewritten into the target&apos;s protocol and sent there.{" "}
            {enabledCount === 0
              ? "No routes are enabled, so nothing takes this branch."
              : `${enabledCount} enabled ${enabledCount === 1 ? "route" : "routes"}; lowest priority wins, route ID breaks ties.`}
          </p>
        </div>
        <div className="rounded-lg border border-border bg-bg/35 px-3 py-2.5">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold text-fg">
            <ArrowRight className="h-3.5 w-3.5 opacity-50" />
            Matches nothing
          </div>
          <p className="mt-1 text-[10px] leading-5 text-fg opacity-60">
            Keeps its own protocol. A real Claude client is forwarded byte for byte, with only the
            credential swapped.
          </p>
        </div>
      </div>
    </Card>
  );
}

/** Whether a route's target can serve it, shown where the route is read. */
function CapacityBadge({ capacity }: { capacity: TargetCapacityView | undefined }) {
  if (!capacity) return null;
  if (capacity.eligible > 0) {
    return (
      <span className="text-[9px] text-fg opacity-45">
        {capacity.eligible} can serve
      </span>
    );
  }
  return (
    <span
      className="flex items-center gap-1 rounded bg-error/10 px-1.5 py-0.5 text-[8px] font-bold uppercase tracking-wide text-error"
      title={unservableReason(capacity)}
    >
      <AlertTriangle className="h-3 w-3" />
      Nothing can serve this
    </span>
  );
}

function RouteRow({
  route,
  targetLabel,
  capacity,
  pending,
  onToggle,
  onEdit,
  onDuplicate,
  onDelete,
}: {
  route: TranslationRouteView;
  targetLabel: string;
  capacity: TargetCapacityView | undefined;
  pending: boolean;
  onToggle: (enabled: boolean) => void;
  onEdit: () => void;
  onDuplicate: () => void;
  onDelete: () => void;
}) {
  return (
    <div
      className={clsx(
        "grid grid-cols-1 gap-3 px-4 py-3 transition-colors lg:grid-cols-[76px_minmax(0,1fr)_28px_minmax(0,1fr)_auto] lg:items-center",
        route.enabled ? "bg-transparent" : "bg-muted/20 opacity-65",
      )}
    >
      <div>
        <div className="text-[8px] font-bold uppercase tracking-[0.15em] text-fg opacity-35">
          Priority
        </div>
        <div className="mt-0.5 font-mono text-[12px] font-semibold text-accent">
          {route.priority}
        </div>
      </div>
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <span className="truncate text-[11.5px] font-semibold text-fg">{route.name}</span>
          <span className="rounded bg-signal/10 px-1.5 py-0.5 text-[8px] font-bold uppercase text-signal">
            {route.match_kind}
          </span>
        </div>
        <div className="mt-1 truncate font-mono text-[10px] text-fg opacity-55">
          {route.model_pattern}
        </div>
      </div>
      <ArrowRight className="hidden h-4 w-4 text-fg opacity-25 lg:block" />
      <div className="min-w-0">
        <div className="truncate font-mono text-[10.5px] font-semibold text-fg">
          {route.target_model}
        </div>
        <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-[9px] text-fg opacity-45">
          <span>
            {route.source_protocol === "anthropic_messages"
              ? "Anthropic Messages"
              : "OpenAI Responses"}{" "}
            → {targetLabel}
            {route.reasoning_effort ? ` · ${route.reasoning_effort} effort` : ""}
          </span>
        </div>
        {route.enabled && (
          <div className="mt-1 flex items-center">
            <CapacityBadge capacity={capacity} />
          </div>
        )}
      </div>
      <div className="flex items-center justify-between gap-1 lg:justify-end">
        <Switch
          checked={route.enabled}
          onCheckedChange={onToggle}
          disabled={pending}
          ariaLabel={`${route.enabled ? "Disable" : "Enable"} ${route.name}`}
        />
        <div className="ml-2 flex items-center gap-1">
          <IconButton label={`Edit ${route.name}`} onClick={onEdit}>
            <Pencil className="h-3.5 w-3.5" />
          </IconButton>
          <IconButton label={`Duplicate ${route.name}`} onClick={onDuplicate}>
            <Copy className="h-3.5 w-3.5" />
          </IconButton>
          <IconButton label={`Delete ${route.name}`} onClick={onDelete} danger>
            <Trash2 className="h-3.5 w-3.5" />
          </IconButton>
        </div>
      </div>
    </div>
  );
}

function IconButton({
  label,
  onClick,
  danger,
  children,
}: {
  label: string;
  onClick: () => void;
  danger?: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      aria-label={label}
      title={label}
      onClick={onClick}
      className={clsx(
        "flex h-8 w-8 items-center justify-center rounded-lg border transition-colors",
        danger
          ? "border-error/20 text-error hover:bg-error/10"
          : "border-border text-fg opacity-55 hover:border-accent/30 hover:text-accent hover:opacity-100",
      )}
    >
      {children}
    </button>
  );
}

function RouteEditor({
  state,
  providers,
  builtinModels,
  pending,
  onChange,
  onCancel,
  onSubmit,
}: {
  state: EditorState;
  providers: CustomProviderView[];
  /** Model names per built-in provider id; empty for a provider with no catalog. */
  builtinModels: Record<string, string[]>;
  pending: boolean;
  onChange: (state: EditorState) => void;
  onCancel: () => void;
  onSubmit: (event: FormEvent) => void;
}) {
  const patch = (value: Partial<TranslationRouteInput>) =>
    onChange({ ...state, draft: { ...state.draft, ...value } });
  const targetProtocol =
    state.draft.source_protocol === "anthropic_messages"
      ? "responses"
      : "anthropic_messages";
  const targetOptions = [
    {
      kind: "builtin_provider" as const,
      id: targetProtocol === "responses" ? "codex" : "anthropic",
      label: targetProtocol === "responses" ? "Codex account fleet" : "Anthropic account fleet",
    },
    ...providers
      .filter((provider) => provider.wire_api === targetProtocol)
      .map((provider) => ({
        kind: "custom_provider" as const,
        id: provider.id,
        label: `${provider.display_name}${provider.enabled ? "" : " (disabled)"}`,
      })),
  ];
  const selectedCustomProvider =
    state.draft.target_kind === "custom_provider"
      ? providers.find((provider) => provider.id === state.draft.target_provider_id)
      : undefined;

  // What the chosen target is known to accept. A custom provider resolves a route by EITHER its
  // public or upstream name and only while the model is enabled (see
  // ingress::resolve_translation_custom_target), so both names are offered and disabled ones are
  // not. Built-in names come from the live catalog the proxy already keeps warm.
  const knownModels: string[] = selectedCustomProvider
    ? selectedCustomProvider.models
        .filter((model) => model.enabled)
        .flatMap((model) =>
          model.upstream_model && model.upstream_model !== model.public_model
            ? [model.public_model, model.upstream_model]
            : [model.public_model],
        )
    : (builtinModels[state.draft.target_provider_id] ?? []);
  const typedModel = state.draft.target_model.trim();
  // Advisory only. A model can be valid before this process has seen it — a fresh release, or a
  // catalog that has not synced — so an unrecognized name is flagged, never blocked.
  const modelIsUnknown =
    typedModel.length > 0 && knownModels.length > 0 && !knownModels.includes(typedModel);
  return (
    <Card className="gap-4 border-accent/25">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-[12px] font-semibold text-fg">
            {state.id ? "Edit translation route" : "New translation route"}
          </h2>
          <p className="mt-0.5 text-[9.5px] text-fg opacity-45">
            Changes apply to the next matching request on either protocol; no restart is required.
          </p>
        </div>
        <button type="button" onClick={onCancel} aria-label="Close editor">
          <X className="h-4 w-4 text-fg opacity-45" />
        </button>
      </div>
      <form onSubmit={onSubmit} className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
        <label className="md:col-span-2">
          <span className={LABEL}>Route name</span>
          <input
            className={INPUT}
            value={state.draft.name}
            onChange={(event) => patch({ name: event.target.value })}
            placeholder="Claude Opus to Codex Sol"
            required
            maxLength={128}
          />
        </label>
        <label>
          <span className={LABEL}>Client protocol</span>
          <select
            className={INPUT}
            value={state.draft.source_protocol}
            onChange={(event) => {
              const source = event.target.value as
                | "anthropic_messages"
                | "openai_responses";
              patch({
                source_protocol: source,
                target_kind: "builtin_provider",
                target_provider_id:
                  source === "anthropic_messages" ? "codex" : "anthropic",
                reasoning_effort:
                  source === "anthropic_messages"
                    ? state.draft.reasoning_effort
                    : null,
              });
            }}
          >
            <option value="anthropic_messages">Anthropic Messages</option>
            <option value="openai_responses">OpenAI Responses</option>
          </select>
        </label>
        <label>
          <span className={LABEL}>Match type</span>
          <select
            className={INPUT}
            value={state.draft.match_kind}
            onChange={(event) =>
              patch({ match_kind: event.target.value as TranslationMatchKind })
            }
          >
            <option value="exact">Exact</option>
            <option value="prefix">Prefix</option>
            <option value="contains">Contains</option>
          </select>
        </label>
        <label>
          <span className={LABEL}>Priority</span>
          <input
            className={INPUT}
            type="number"
            min={-1_000_000}
            max={1_000_000}
            value={state.draft.priority}
            onChange={(event) => patch({ priority: Number(event.target.value) })}
            required
          />
        </label>
        <label className="md:col-span-2">
          <span className={LABEL}>Client model pattern</span>
          <input
            className={`${INPUT} font-mono`}
            value={state.draft.model_pattern}
            onChange={(event) => patch({ model_pattern: event.target.value })}
            placeholder="opus"
            required
            maxLength={192}
          />
        </label>
        <label>
          <span className={LABEL}>Target provider</span>
          <select
            className={INPUT}
            value={`${state.draft.target_kind}:${state.draft.target_provider_id}`}
            onChange={(event) => {
              const [target_kind, target_provider_id] = event.target.value.split(":", 2);
              patch({
                target_kind: target_kind as "builtin_provider" | "custom_provider",
                target_provider_id,
              });
            }}
          >
            {targetOptions.map((option) => (
              <option key={`${option.kind}:${option.id}`} value={`${option.kind}:${option.id}`}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span className={LABEL}>Target model</span>
          <input
            className={`${INPUT} font-mono`}
            list={knownModels.length > 0 ? "translation-target-models" : undefined}
            value={state.draft.target_model}
            onChange={(event) => patch({ target_model: event.target.value })}
            placeholder="gpt-5.6-sol"
            required
            maxLength={192}
          />
          {knownModels.length > 0 && (
            <datalist id="translation-target-models">
              {knownModels.map((model) => (
                <option key={model} value={model} />
              ))}
            </datalist>
          )}
          {modelIsUnknown && (
            <span className="mt-1 flex items-start gap-1 text-[9px] text-fg opacity-55">
              <AlertTriangle className="mt-px h-3 w-3 shrink-0" />
              {selectedCustomProvider
                ? "Not an enabled model on this provider. Requests will fail unless it is added or synced."
                : "Not in the current catalog. Fine for a model this proxy has not seen yet."}
            </span>
          )}
          {knownModels.length === 0 && state.draft.target_kind === "builtin_provider" && (
            <span className="mt-1 block text-[9px] text-fg opacity-45">
              No catalog for this provider yet — type the model name.
            </span>
          )}
        </label>
        <label>
          <span className={LABEL}>Reasoning effort</span>
          <select
            className={INPUT}
            disabled={targetProtocol !== "responses"}
            value={state.draft.reasoning_effort ?? ""}
            onChange={(event) =>
              patch({
                reasoning_effort:
                  (event.target.value as TranslationReasoningEffort | "") || null,
              })
            }
          >
            {EFFORTS.map((effort) => (
              <option key={effort || "default"} value={effort}>
                {effort || "Client / model default"}
              </option>
            ))}
          </select>
        </label>
        <div className="flex items-center gap-3 md:col-span-2 xl:col-span-3">
          <Switch
            checked={state.draft.enabled}
            onCheckedChange={(enabled) => patch({ enabled })}
            ariaLabel="Route enabled"
          />
          <div>
            <div className="text-[10.5px] font-semibold text-fg">Enabled</div>
            <div className="text-[9px] text-fg opacity-45">
              Disabled routes remain configured but never match.
            </div>
          </div>
        </div>
        <div className="flex items-end justify-end gap-2">
          <button
            type="button"
            onClick={onCancel}
            className="h-9 rounded-lg border border-border px-3 text-[10px] text-fg opacity-65"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={pending}
            className="h-9 rounded-lg bg-accent px-4 text-[10px] font-semibold text-white disabled:opacity-40"
          >
            Save route
          </button>
        </div>
      </form>
    </Card>
  );
}

function RecentTranslations({
  rows,
}: {
  rows: Array<{
    requested_at: number;
    request_id: string | null;
    path: "/v1/messages" | "/responses";
    provider: string;
    status: number;
    model: string | null;
    reasoning_effort: string | null;
    duration_ms: number;
  }>;
}) {
  return (
    <Card className="p-0">
      <div className="border-b border-border/70 px-4 py-3">
        <h2 className="text-[12px] font-semibold text-fg">Recent translated requests</h2>
        <p className="mt-0.5 text-[9.5px] text-fg opacity-45">
          Verified traffic where a translation route matched.
        </p>
      </div>
      {rows.length ? (
        <div className="divide-y divide-border/60">
          {rows.map((row, index) => (
            <div
              key={row.request_id ?? `${row.requested_at}-${index}`}
              className="grid grid-cols-[minmax(0,1fr)_auto] gap-3 px-4 py-2.5"
            >
              <div className="min-w-0">
                <div className="truncate font-mono text-[10px] font-semibold text-fg">
                  {row.model ?? "unknown target"}
                </div>
                <div className="mt-1 flex flex-wrap gap-x-3 text-[8.5px] text-fg opacity-45">
                  <span>{relTime(row.requested_at)}</span>
                  <span>
                    {row.path === "/v1/messages" ? "Messages → Responses" : "Responses → Messages"}
                  </span>
                  <span>{row.provider}</span>
                  {row.reasoning_effort && <span>{row.reasoning_effort} effort</span>}
                  {row.request_id && <span className="font-mono">{row.request_id.slice(0, 12)}</span>}
                </div>
              </div>
              <div className="text-right">
                <div
                  className={clsx(
                    "text-[9px] font-bold",
                    row.status >= 200 && row.status < 300 ? "text-success" : "text-error",
                  )}
                >
                  HTTP {row.status}
                </div>
                <div className="mt-1 text-[8.5px] text-fg opacity-45">
                  {latency(row.duration_ms)}
                </div>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="px-4 py-8 text-center text-[10px] text-fg opacity-45">
          No translated requests have been recorded yet.
        </div>
      )}
    </Card>
  );
}
