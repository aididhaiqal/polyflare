import {
  useEffect,
  useState,
  type Dispatch,
  type FormEvent,
  type SetStateAction,
} from "react";

import type {
  CreateProviderModelBody,
  ProviderModelDiscoveryResult,
  ProviderModelView,
} from "../lib/api";
import {
  filterDiscoveredModels,
  selectableDiscoveredModelIds,
  type DiscoveryCapabilityFilter,
} from "../lib/providerDiscovery";
import {
  isProviderModelProfile,
  providerProfileTemplate,
} from "../lib/providerProfiles";
import {
  useAddProviderCredential,
  useAddProviderModel,
  useCreateProviderBundle,
  useDiscoverProviderModels,
  useProviderAction,
  useProviders,
  useSyncProviderModels,
  useTestProvider,
  useUpdateProviderModel,
} from "../lib/queries";
import { Card } from "../ui/Card";
import { AlertTriangle, Check, KeyRound, Plus, Route, Search, Trash2, X } from "../ui/icons";

const INPUT =
  "h-9 w-full rounded-lg border border-border bg-bg/65 px-3 text-[11px] text-fg outline-none transition focus:border-accent/60";

export function Providers() {
  const providers = useProviders();
  const create = useCreateProviderBundle();
  const addCredential = useAddProviderCredential();
  const addModel = useAddProviderModel();
  const action = useProviderAction();
  const testConnection = useTestProvider();
  const discoverModels = useDiscoverProviderModels();
  const syncModels = useSyncProviderModels();
  const updateModel = useUpdateProviderModel();
  const [open, setOpen] = useState(false);
  const [addTarget, setAddTarget] = useState<{
    providerId: string;
    kind: "credential" | "model";
    model?: ProviderModelView;
    template?: ProviderModelView;
  } | null>(null);
  const [discoveryTarget, setDiscoveryTarget] = useState<{
    providerId: string;
    providerName: string;
    result: ProviderModelDiscoveryResult;
  } | null>(null);

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold text-fg">Model providers</h1>
          <p className="mt-0.5 text-[11px] text-fg opacity-60">
            Add Responses-compatible providers, credential pools, and models to the main PolyFlare
            catalog.
          </p>
        </div>
        <button
          type="button"
          onClick={() => setOpen((value) => !value)}
          className="flex items-center gap-2 rounded-lg bg-accent px-3 py-2 text-[11px] font-semibold text-white"
        >
          <Plus className="h-3.5 w-3.5" />
          Add provider
        </button>
      </div>

      {open && (
        <ProviderForm
          pending={create.isPending}
          onSubmit={(value) =>
            create.mutate(value, {
              onSuccess: () => setOpen(false),
            })
          }
        />
      )}

      {providers.isLoading ? (
        <Card>
          <div className="h-36 animate-pulse rounded-lg bg-muted" />
        </Card>
      ) : providers.isError ? (
        <Card>
          <div className="flex items-center gap-2 text-[11px] text-error">
            <AlertTriangle className="h-4 w-4" />
            Couldn&apos;t load configured providers.
          </div>
        </Card>
      ) : providers.data?.length ? (
        <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
          {providers.data.map((provider) => (
            <Card key={provider.id} className="gap-4">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <Route className="h-4 w-4 text-accent" />
                    <h2 className="truncate text-[13px] font-semibold text-fg">
                      {provider.display_name}
                    </h2>
                    <span
                      className={`rounded px-1.5 py-0.5 text-[8px] font-bold uppercase ${
                        provider.enabled
                          ? "bg-success/15 text-success"
                          : "bg-muted text-fg opacity-55"
                      }`}
                    >
                      {provider.enabled ? "active" : "disabled"}
                    </span>
                  </div>
                  <div className="mt-1 truncate font-mono text-[9.5px] text-fg opacity-45">
                    {provider.base_url}
                  </div>
                </div>
                <div className="text-right text-[9px] text-fg opacity-45">
                  <div>{provider.request_max_retries} retries</div>
                  <div>{Math.round(provider.stream_idle_timeout_ms / 1000)}s idle</div>
                  <div className="mt-2 flex justify-end gap-1">
                    <button
                      type="button"
                      disabled={testConnection.isPending}
                      onClick={() => testConnection.mutate(provider.id)}
                      className="rounded border border-accent/30 px-1.5 py-0.5 text-[8.5px] text-accent hover:bg-accent/10 disabled:opacity-40"
                    >
                      Test
                    </button>
                    <button
                      type="button"
                      disabled={action.isPending}
                      onClick={() =>
                        action.mutate({
                          kind: "provider_enabled",
                          id: provider.id,
                          enabled: !provider.enabled,
                        })
                      }
                      className="rounded border border-border px-1.5 py-0.5 text-[8.5px] text-fg opacity-75 hover:opacity-100"
                    >
                      {provider.enabled ? "Disable" : "Enable"}
                    </button>
                    <button
                      type="button"
                      disabled={action.isPending}
                      onClick={() => {
                        if (window.confirm(`Delete ${provider.display_name} and its credentials/models?`)) {
                          action.mutate({ kind: "provider_delete", id: provider.id });
                        }
                      }}
                      className="rounded border border-error/30 px-1.5 py-0.5 text-error opacity-75 hover:opacity-100"
                      aria-label={`Delete ${provider.display_name}`}
                    >
                      <Trash2 className="h-3 w-3" />
                    </button>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <section className="rounded-lg border border-border/70 bg-bg/35 p-3">
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <span className="flex items-center gap-1.5 text-[9px] font-bold uppercase tracking-wide text-fg opacity-55">
                      <KeyRound className="h-3.5 w-3.5 text-signal" />
                      Credentials
                    </span>
                    <button
                      type="button"
                      onClick={() => setAddTarget({ providerId: provider.id, kind: "credential" })}
                      className="text-[9px] font-semibold text-accent"
                    >
                      + key
                    </button>
                  </div>
                  <div className="flex flex-col gap-2">
                    {provider.credentials.map((credential) => (
                      <div key={credential.id} className="flex items-center justify-between gap-2">
                        <span className="truncate text-[10.5px] text-fg">{credential.label}</span>
                        <div className="flex items-center gap-1.5">
                          <span
                            className={`text-[8px] font-bold uppercase ${
                              credential.health_status === "healthy" ? "text-success" : "text-warn"
                            }`}
                          >
                            {credential.enabled ? credential.health_status : "disabled"}
                          </span>
                          <button
                            type="button"
                            onClick={() =>
                              action.mutate({
                                kind: "credential_enabled",
                                id: credential.id,
                                enabled: !credential.enabled,
                              })
                            }
                            className="text-[8px] text-accent"
                          >
                            {credential.enabled ? "off" : "on"}
                          </button>
                          <button
                            type="button"
                            onClick={() => {
                              if (window.confirm(`Delete credential ${credential.label}?`)) {
                                action.mutate({ kind: "credential_delete", id: credential.id });
                              }
                            }}
                            className="text-error opacity-70 hover:opacity-100"
                            aria-label={`Delete credential ${credential.label}`}
                          >
                            <Trash2 className="h-2.5 w-2.5" />
                          </button>
                        </div>
                      </div>
                    ))}
                    {!provider.credentials.length && (
                      <span className="text-[10px] text-fg opacity-40">No credentials</span>
                    )}
                  </div>
                </section>

                <section className="rounded-lg border border-border/70 bg-bg/35 p-3">
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <span className="text-[9px] font-bold uppercase tracking-wide text-fg opacity-55">
                      Models
                    </span>
                    <span className="flex items-center gap-2">
                      <button
                        type="button"
                        disabled={discoverModels.isPending || !provider.credentials.length}
                        onClick={() =>
                          discoverModels.mutate(provider.id, {
                            onSuccess: (result) =>
                              setDiscoveryTarget({
                                providerId: provider.id,
                                providerName: provider.display_name,
                                result,
                              }),
                          })
                        }
                        className="text-[9px] font-semibold text-signal disabled:opacity-35"
                      >
                        {discoverModels.isPending ? "Discovering…" : "Discover"}
                      </button>
                      <button
                        type="button"
                        onClick={() => setAddTarget({ providerId: provider.id, kind: "model" })}
                        className="text-[9px] font-semibold text-accent"
                      >
                        + model
                      </button>
                    </span>
                  </div>
                  <div className="flex flex-col gap-2">
                    {provider.models.map((model) => (
                      <div key={model.id}>
                        <div className="flex items-center justify-between gap-2">
                          <span className="truncate font-mono text-[10.5px] text-fg">
                            {model.public_model}
                          </span>
                          <span className="flex items-center gap-1.5 text-[8px] text-fg opacity-40">
                            {model.context_window
                              ? `${Math.round(model.context_window / 1000)}k ctx`
                              : "context unknown"}
                            <button
                              type="button"
                              onClick={() =>
                                setAddTarget({
                                  providerId: provider.id,
                                  kind: "model",
                                  template: model,
                                })
                              }
                              className="text-signal opacity-100"
                            >
                              profile
                            </button>
                            <button
                              type="button"
                              onClick={() =>
                                setAddTarget({
                                  providerId: provider.id,
                                  kind: "model",
                                  model,
                                })
                              }
                              className="text-accent opacity-100"
                            >
                              edit
                            </button>
                            <button
                              type="button"
                              onClick={() =>
                                action.mutate({
                                  kind: "model_enabled",
                                  id: model.id,
                                  enabled: !model.enabled,
                                })
                              }
                              className="text-accent opacity-100"
                            >
                              {model.enabled ? "off" : "on"}
                            </button>
                            <button
                              type="button"
                              onClick={() => {
                                if (window.confirm(`Delete model ${model.public_model}?`)) {
                                  action.mutate({ kind: "model_delete", id: model.id });
                                }
                              }}
                              className="text-error opacity-100"
                              aria-label={`Delete model ${model.public_model}`}
                            >
                              <Trash2 className="h-2.5 w-2.5" />
                            </button>
                          </span>
                        </div>
                        {model.public_model !== model.upstream_model && (
                          <div className="mt-0.5 truncate font-mono text-[8.5px] text-fg opacity-35">
                            upstream: {model.upstream_model}
                          </div>
                        )}
                        <div className="mt-1 text-[8px] text-fg opacity-45">
                          Effort{" "}
                          {model.reasoning_levels.length
                            ? model.reasoning_levels.join(" · ")
                            : "not advertised"}
                          {model.supports_reasoning_summaries ? " · summaries" : ""}
                        </div>
                        {isProviderModelProfile(model) && (
                          <div className="mt-1 inline-flex rounded border border-signal/25 bg-signal/[0.07] px-1.5 py-0.5 text-[8px] font-semibold text-signal">
                            profile · {model.instruction_mode}
                          </div>
                        )}
                        <div className="mt-1 flex items-center gap-1">
                          <span className="mr-1 text-[8px] uppercase tracking-wide text-fg opacity-35">
                            Discoverable
                          </span>
                          <button
                            type="button"
                            onClick={() =>
                              action.mutate({
                                kind: "model_visibility",
                                id: model.id,
                                visible_in_codex: !model.visible_in_codex,
                              })
                            }
                            className={`rounded border px-1.5 py-0.5 text-[8px] ${
                              model.visible_in_codex
                                ? "border-accent/35 bg-accent/10 text-accent"
                                : "border-border text-fg opacity-40"
                            }`}
                          >
                            Codex picker
                          </button>
                          <button
                            type="button"
                            onClick={() =>
                              action.mutate({
                                kind: "model_visibility",
                                id: model.id,
                                visible_in_openai: !model.visible_in_openai,
                              })
                            }
                            className={`rounded border px-1.5 py-0.5 text-[8px] ${
                              model.visible_in_openai
                                ? "border-signal/35 bg-signal/10 text-signal"
                                : "border-border text-fg opacity-40"
                            }`}
                          >
                            OpenAI list
                          </button>
                          {!model.visible_in_codex && !model.visible_in_openai && (
                            <span className="text-[8px] text-warn">route only</span>
                          )}
                        </div>
                      </div>
                    ))}
                    {!provider.models.length && (
                      <span className="text-[10px] text-fg opacity-40">No models</span>
                    )}
                  </div>
                </section>
              </div>
              {discoveryTarget?.providerId === provider.id && (
                <ModelDiscoveryPanel
                  key={`${provider.id}-${discoveryTarget.result.discovered}`}
                  providerName={discoveryTarget.providerName}
                  result={discoveryTarget.result}
                  importing={syncModels.isPending}
                  onClose={() => setDiscoveryTarget(null)}
                  onRefresh={() =>
                    discoverModels.mutate(provider.id, {
                      onSuccess: (result) =>
                        setDiscoveryTarget({
                          providerId: provider.id,
                          providerName: provider.display_name,
                          result,
                        }),
                    })
                  }
                  onImport={(modelIds) =>
                    syncModels.mutate(
                      { providerId: provider.id, modelIds },
                      { onSuccess: () => setDiscoveryTarget(null) },
                    )
                  }
                />
              )}
              {addTarget?.providerId === provider.id && addTarget.kind === "credential" && (
                <CredentialForm
                  pending={addCredential.isPending}
                  onCancel={() => setAddTarget(null)}
                  onSubmit={(credential) =>
                    addCredential.mutate(
                      { providerId: provider.id, credential },
                      { onSuccess: () => setAddTarget(null) },
                    )
                  }
                />
              )}
              {addTarget?.providerId === provider.id && addTarget.kind === "model" && (
                <ModelForm
                  key={`${addTarget.model?.id ?? "new"}-${addTarget.template?.id ?? "blank"}`}
                  initial={addTarget.model}
                  template={addTarget.template}
                  pending={addModel.isPending || updateModel.isPending}
                  onCancel={() => setAddTarget(null)}
                  onSubmit={(model) =>
                    addTarget.model
                      ? updateModel.mutate(
                          {
                            id: addTarget.model.id,
                            patch: {
                              upstream_model: model.upstream_model,
                              display_name: model.display_name,
                              context_window: model.context_window,
                              max_output_tokens: model.max_output_tokens,
                              supports_tools: model.supports_tools,
                              supports_vision: model.supports_vision,
                              supports_parallel_tool_calls: model.supports_parallel_tool_calls,
                              supports_web_search: model.supports_web_search,
                              supports_reasoning_summaries: model.supports_reasoning_summaries,
                              reasoning_levels: model.reasoning_levels,
                              instruction_mode: model.instruction_mode,
                              instruction_text: model.instruction_text,
                              request_overrides: model.request_overrides,
                              visible_in_codex: model.visible_in_codex,
                              visible_in_openai: model.visible_in_openai,
                            },
                          },
                          { onSuccess: () => setAddTarget(null) },
                        )
                      : addModel.mutate(
                          { providerId: provider.id, model },
                          { onSuccess: () => setAddTarget(null) },
                        )
                  }
                />
              )}
            </Card>
          ))}
        </div>
      ) : (
        <Card className="items-center gap-2 py-10 text-center">
          <Route className="h-6 w-6 text-accent opacity-70" />
          <div className="text-[12px] font-semibold text-fg">No custom providers yet</div>
          <div className="max-w-md text-[10px] text-fg opacity-50">
            Add Sakana, an internal gateway, or any OpenAI Responses-compatible service.
          </div>
        </Card>
      )}
    </div>
  );
}

const DISCOVERY_FILTERS: Array<{
  value: DiscoveryCapabilityFilter;
  label: string;
}> = [
  { value: "all", label: "All" },
  { value: "tools", label: "Tools" },
  { value: "vision", label: "Vision" },
  { value: "reasoning", label: "Reasoning" },
  { value: "free", label: "Free" },
];

function ModelDiscoveryPanel({
  providerName,
  result,
  importing,
  onClose,
  onRefresh,
  onImport,
}: {
  providerName: string;
  result: ProviderModelDiscoveryResult;
  importing: boolean;
  onClose: () => void;
  onRefresh: () => void;
  onImport: (modelIds: string[]) => void;
}) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<DiscoveryCapabilityFilter>("all");
  const [selected, setSelected] = useState<string[]>([]);
  useEffect(() => setSelected([]), [result]);
  const selectedSet = new Set(selected);
  const visible = filterDiscoveredModels(result.models, search, filter);
  const selectableVisible = selectableDiscoveredModelIds(visible);
  const allVisibleSelected =
    selectableVisible.length > 0 &&
    selectableVisible.every((modelId) => selectedSet.has(modelId));

  function toggle(modelId: string) {
    setSelected((current) =>
      current.includes(modelId)
        ? current.filter((value) => value !== modelId)
        : [...current, modelId],
    );
  }

  return (
    <section className="rounded-xl border border-signal/30 bg-bg/55 p-3.5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[11px] font-semibold text-fg">Discover from {providerName}</div>
          <p className="mt-0.5 text-[9.5px] text-fg opacity-50">
            Preview only · nothing is added until you import an explicit selection
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onRefresh}
            className="text-[9px] font-semibold text-signal"
          >
            Refresh
          </button>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close model discovery"
            className="text-fg opacity-45 hover:opacity-80"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2">
        <label className="flex min-w-[210px] flex-1 items-center gap-2 rounded-lg border border-border bg-card px-2.5">
          <Search className="h-3.5 w-3.5 shrink-0 text-fg opacity-40" />
          <input
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search model or vendor"
            className="h-8 min-w-0 flex-1 bg-transparent text-[10.5px] text-fg outline-none"
          />
        </label>
        <div className="flex overflow-hidden rounded-lg border border-border bg-card">
          {DISCOVERY_FILTERS.map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => setFilter(option.value)}
              className={`px-2.5 py-2 text-[9px] font-semibold ${
                filter === option.value
                  ? "bg-signal/15 text-signal"
                  : "text-fg opacity-50 hover:opacity-80"
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-[9px] text-fg opacity-55">
        <span>
          {visible.length} shown · {result.discovered} discovered · {selected.length} selected
        </span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            disabled={selectableVisible.length === 0}
            onClick={() =>
              setSelected((current) => {
                if (allVisibleSelected) {
                  const visibleSet = new Set(selectableVisible);
                  return current.filter((modelId) => !visibleSet.has(modelId));
                }
                return [...new Set([...current, ...selectableVisible])];
              })
            }
            className="font-semibold text-accent disabled:opacity-30"
          >
            {allVisibleSelected ? "Clear visible" : "Select visible"}
          </button>
          {selected.length > 0 && (
            <button
              type="button"
              onClick={() => setSelected([])}
              className="font-semibold text-fg opacity-70"
            >
              Clear all
            </button>
          )}
        </div>
      </div>

      <div className="mt-2 max-h-[360px] overflow-y-auto rounded-lg border border-border/70 bg-card/60">
        {visible.map((model) => {
          const selectable = model.state === "available";
          const checked = selectedSet.has(model.upstream_model);
          return (
            <button
              key={model.upstream_model}
              type="button"
              disabled={!selectable}
              onClick={() => toggle(model.upstream_model)}
              className="flex w-full items-start gap-2.5 border-b border-border/45 px-3 py-2.5 text-left last:border-0 hover:bg-muted/45 disabled:cursor-default"
            >
              <span
                className={`mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded border ${
                  checked ? "border-accent bg-accent text-white" : "border-border"
                } ${selectable ? "" : "opacity-25"}`}
              >
                {checked && <Check className="h-3 w-3" strokeWidth={2.5} />}
              </span>
              <span className="min-w-0 flex-1">
                <span className="flex flex-wrap items-center gap-1.5">
                  <span className="truncate font-mono text-[10px] text-fg">
                    {model.upstream_model}
                  </span>
                  <span
                    className={`rounded px-1.5 py-0.5 text-[7.5px] font-bold uppercase ${
                      model.state === "available"
                        ? "bg-success/12 text-success"
                        : model.state === "configured"
                          ? "bg-accent/12 text-accent"
                          : "bg-warn/12 text-warn"
                    }`}
                  >
                    {model.state}
                  </span>
                </span>
                <span className="mt-0.5 block truncate text-[9px] text-fg opacity-55">
                  {model.display_name}
                </span>
                <span className="mt-1 flex flex-wrap gap-1 text-[7.5px] text-fg opacity-45">
                  {model.context_window && (
                    <span>{Math.round(model.context_window / 1000)}k context</span>
                  )}
                  {model.supports_tools && <span>· tools</span>}
                  {model.supports_vision && <span>· vision</span>}
                  {model.supports_reasoning && (
                    <span>
                      ·{" "}
                      {model.reasoning_levels.length > 0
                        ? model.reasoning_levels.join("/")
                        : "reasoning"}
                    </span>
                  )}
                  {model.input_per_million !== null && (
                    <span>· ${model.input_per_million.toFixed(2)}/M input</span>
                  )}
                </span>
                {model.suggested_public_model !== model.upstream_model && (
                  <span className="mt-1 block truncate font-mono text-[7.5px] text-fg opacity-35">
                    Polyflare model: {model.suggested_public_model}
                  </span>
                )}
              </span>
            </button>
          );
        })}
        {visible.length === 0 && (
          <div className="px-3 py-8 text-center text-[10px] text-fg opacity-40">
            No discovered models match these filters.
          </div>
        )}
      </div>

      <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
        <p className="text-[8.5px] text-fg opacity-45">
          Configured and conflicting models are never included in bulk selection.
        </p>
        <button
          type="button"
          disabled={selected.length === 0 || importing}
          onClick={() => onImport(selected)}
          className="rounded-lg bg-accent px-3 py-2 text-[10px] font-semibold text-white disabled:opacity-35"
        >
          {importing
            ? "Importing…"
            : `Import ${selected.length} model${selected.length === 1 ? "" : "s"}`}
        </button>
      </div>
    </section>
  );
}

function CredentialForm({
  pending,
  onSubmit,
  onCancel,
}: {
  pending: boolean;
  onSubmit: (credential: { label: string; api_key: string; routing_weight?: number }) => void;
  onCancel: () => void;
}) {
  const [label, setLabel] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [weight, setWeight] = useState("1");
  return (
    <form
      className="grid gap-2 rounded-lg border border-accent/25 bg-accent/[0.04] p-3 sm:grid-cols-3"
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit({ label, api_key: apiKey, routing_weight: Number(weight) || 1 });
      }}
    >
      <input
        className={INPUT}
        value={label}
        required
        placeholder="Credential label"
        onChange={(event) => setLabel(event.target.value)}
      />
      <input
        className={INPUT}
        value={apiKey}
        required
        type="password"
        autoComplete="off"
        placeholder="API key"
        onChange={(event) => setApiKey(event.target.value)}
      />
      <div className="flex gap-2">
        <input
          className={INPUT}
          value={weight}
          required
          type="number"
          min="0.01"
          step="0.01"
          aria-label="Routing weight"
          onChange={(event) => setWeight(event.target.value)}
        />
        <button
          type="submit"
          disabled={pending}
          className="rounded-lg bg-accent px-3 text-[10px] font-semibold text-white disabled:opacity-45"
        >
          Add
        </button>
        <button type="button" onClick={onCancel} className="px-2 text-[10px] text-fg opacity-55">
          Cancel
        </button>
      </div>
    </form>
  );
}

function ModelForm({
  initial,
  template,
  pending,
  onSubmit,
  onCancel,
}: {
  initial?: ProviderModelView;
  template?: ProviderModelView;
  pending: boolean;
  onSubmit: (model: CreateProviderModelBody) => void;
  onCancel: () => void;
}) {
  const source = initial ?? template;
  const templateDefaults = template ? providerProfileTemplate(template) : null;
  const [publicModel, setPublicModel] = useState(
    initial?.public_model ?? templateDefaults?.publicModel ?? "",
  );
  const [upstreamModel, setUpstreamModel] = useState(source?.upstream_model ?? "");
  const [displayName, setDisplayName] = useState(
    initial?.display_name ?? templateDefaults?.displayName ?? "",
  );
  const [contextWindow, setContextWindow] = useState(
    source?.context_window?.toString() ?? "",
  );
  const [maxOutputTokens, setMaxOutputTokens] = useState(
    source?.max_output_tokens?.toString() ?? "",
  );
  const [reasoningLevels, setReasoningLevels] = useState(
    source?.reasoning_levels.join(",") ?? "",
  );
  const [supportsTools, setSupportsTools] = useState(source?.supports_tools ?? true);
  const [supportsVision, setSupportsVision] = useState(source?.supports_vision ?? false);
  const [supportsParallel, setSupportsParallel] = useState(
    source?.supports_parallel_tool_calls ?? true,
  );
  const [supportsSearch, setSupportsSearch] = useState(
    source?.supports_web_search ?? false,
  );
  const [supportsSummaries, setSupportsSummaries] = useState(
    source?.supports_reasoning_summaries ?? false,
  );
  const [instructionMode, setInstructionMode] = useState<"none" | "append" | "replace">(
    initial?.instruction_mode ?? templateDefaults?.instructionMode ?? "none",
  );
  const [instructionText, setInstructionText] = useState(initial?.instruction_text ?? "");
  const [overrideEffort, setOverrideEffort] = useState(
    initial?.request_overrides.reasoning_effort ?? "",
  );
  const [overrideMaxOutput, setOverrideMaxOutput] = useState(
    initial?.request_overrides.max_output_tokens?.toString() ?? "",
  );
  const [visibleInCodex, setVisibleInCodex] = useState(source?.visible_in_codex ?? true);
  const [visibleInOpenAi, setVisibleInOpenAi] = useState(
    source?.visible_in_openai ?? true,
  );
  const profileEffortOptions = source?.reasoning_levels.length
    ? source.reasoning_levels
    : ["none", "minimal", "low", "medium", "high", "xhigh", "max"];
  return (
    <form
      className="grid gap-2 rounded-lg border border-accent/25 bg-accent/[0.04] p-3 sm:grid-cols-2"
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit({
          public_model: publicModel,
          upstream_model: upstreamModel,
          display_name: displayName,
          context_window: contextWindow ? Number(contextWindow) : undefined,
          max_output_tokens: maxOutputTokens ? Number(maxOutputTokens) : undefined,
          supports_tools: supportsTools,
          supports_vision: supportsVision,
          supports_parallel_tool_calls: supportsParallel,
          supports_web_search: supportsSearch,
          supports_reasoning_summaries: supportsSummaries,
          reasoning_levels: reasoningLevels
            .split(",")
            .map((level) => level.trim())
            .filter(Boolean),
          instruction_mode: instructionMode,
          instruction_text: instructionMode === "none" ? "" : instructionText,
          request_overrides: {
            ...(overrideEffort ? { reasoning_effort: overrideEffort } : {}),
            ...(overrideMaxOutput
              ? { max_output_tokens: Number(overrideMaxOutput) }
              : {}),
          },
          input_per_million: source?.input_per_million ?? undefined,
          cached_input_per_million: source?.cached_input_per_million ?? undefined,
          output_per_million: source?.output_per_million ?? undefined,
          visible_in_codex: visibleInCodex,
          visible_in_openai: visibleInOpenAi,
        });
      }}
    >
      <input
        className={INPUT}
        value={publicModel}
        required
        disabled={Boolean(initial)}
        placeholder="Public model slug"
        onChange={(event) => setPublicModel(event.target.value)}
      />
      <input
        className={INPUT}
        value={upstreamModel}
        required
        placeholder="Upstream model slug"
        onChange={(event) => setUpstreamModel(event.target.value)}
      />
      <input
        className={INPUT}
        value={displayName}
        required
        placeholder="Display name"
        onChange={(event) => setDisplayName(event.target.value)}
      />
      <div className="flex gap-2">
        <input
          className={INPUT}
          value={contextWindow}
          type="number"
          min="1"
          placeholder="Context window"
          onChange={(event) => setContextWindow(event.target.value)}
        />
        <button
          type="submit"
          disabled={pending}
          className="rounded-lg bg-accent px-3 text-[10px] font-semibold text-white disabled:opacity-45"
        >
          {initial ? "Save" : "Add"}
        </button>
        <button type="button" onClick={onCancel} className="px-2 text-[10px] text-fg opacity-55">
          Cancel
        </button>
      </div>
      <input
        className={INPUT}
        value={maxOutputTokens}
        type="number"
        min="1"
        placeholder="Model max output tokens"
        onChange={(event) => setMaxOutputTokens(event.target.value)}
      />
      <input
        className={INPUT}
        value={reasoningLevels}
        placeholder="Reasoning efforts: high,xhigh,max"
        aria-label="Reasoning efforts"
        onChange={(event) => setReasoningLevels(event.target.value)}
      />
      <div className="grid gap-2 rounded-lg border border-signal/20 bg-signal/[0.04] p-2.5 sm:col-span-2 sm:grid-cols-2">
        <label className="text-[9px] font-semibold text-fg">
          Instruction mode
          <select
            className={`${INPUT} mt-1`}
            value={instructionMode}
            onChange={(event) =>
              setInstructionMode(event.target.value as "none" | "append" | "replace")
            }
          >
            <option value="none">None · leave client instructions unchanged</option>
            <option value="append">Append · preserve client instructions</option>
            <option value="replace">Replace · advanced</option>
          </select>
        </label>
        <div className="grid grid-cols-2 gap-2">
          <label className="text-[9px] font-semibold text-fg">
            Reasoning override
            <select
              className={`${INPUT} mt-1`}
              value={overrideEffort}
              onChange={(event) => setOverrideEffort(event.target.value)}
            >
              <option value="">Client value</option>
              {profileEffortOptions.map((effort) => (
                <option key={effort} value={effort}>
                  {effort}
                </option>
              ))}
            </select>
          </label>
          <label className="text-[9px] font-semibold text-fg">
            Output override
            <input
              className={`${INPUT} mt-1`}
              type="number"
              min="1"
              max={maxOutputTokens || undefined}
              value={overrideMaxOutput}
              placeholder="Client value"
              onChange={(event) => setOverrideMaxOutput(event.target.value)}
            />
          </label>
        </div>
        {instructionMode !== "none" && (
          <label className="text-[9px] font-semibold text-fg sm:col-span-2">
            Instruction overlay
            <textarea
              className="mt-1 min-h-28 w-full rounded-lg border border-border bg-bg/65 px-3 py-2 text-[10px] text-fg outline-none transition focus:border-accent/60"
              value={instructionText}
              required
              maxLength={32768}
              placeholder="Instructions applied by PolyFlare before the request reaches the provider."
              onChange={(event) => setInstructionText(event.target.value)}
            />
          </label>
        )}
        {instructionMode === "replace" && (
          <p className="text-[8.5px] text-warn sm:col-span-2">
            Replace removes Codex&apos;s operating and tool instructions. Use only with a complete
            replacement prompt.
          </p>
        )}
        <p className="text-[8.5px] text-fg opacity-45 sm:col-span-2">
          Profiles guide model behavior; they do not enforce security or tool permissions.
        </p>
      </div>
      <div className="flex flex-wrap gap-3 sm:col-span-2">
        {[
          ["Tools", supportsTools, setSupportsTools],
          ["Vision", supportsVision, setSupportsVision],
          ["Parallel tools", supportsParallel, setSupportsParallel],
          ["Web search", supportsSearch, setSupportsSearch],
          ["Reasoning summaries", supportsSummaries, setSupportsSummaries],
        ].map(([label, checked, setChecked]) => (
          <label
            key={label as string}
            className="flex items-center gap-2 text-[9px] text-fg opacity-70"
          >
            <input
              type="checkbox"
              checked={checked as boolean}
              onChange={(event) =>
                (setChecked as Dispatch<SetStateAction<boolean>>)(
                  event.target.checked,
                )
              }
            />
            {label as string}
          </label>
        ))}
        <label className="flex items-center gap-2 text-[9px] text-fg opacity-70">
          <input
            type="checkbox"
            checked={visibleInCodex}
            onChange={(event) => setVisibleInCodex(event.target.checked)}
          />
          Show in Codex picker
        </label>
        <label className="flex items-center gap-2 text-[9px] text-fg opacity-70">
          <input
            type="checkbox"
            checked={visibleInOpenAi}
            onChange={(event) => setVisibleInOpenAi(event.target.checked)}
          />
          Show in OpenAI model list
        </label>
      </div>
    </form>
  );
}

function ProviderForm({
  pending,
  onSubmit,
}: {
  pending: boolean;
  onSubmit: (value: Parameters<ReturnType<typeof useCreateProviderBundle>["mutate"]>[0]) => void;
}) {
  const [form, setForm] = useState({
    providerName: "Sakana",
    providerSlug: "sakana",
    baseUrl: "https://api.sakana.ai/v1",
    apiKey: "",
  });

  function submit(event: FormEvent) {
    event.preventDefault();
    onSubmit({
      provider: {
        slug: form.providerSlug,
        display_name: form.providerName,
        base_url: form.baseUrl,
        stateless_responses: true,
        stream_idle_timeout_ms: form.providerSlug === "sakana" ? 7_200_000 : 300_000,
        request_max_retries: form.providerSlug === "sakana" ? 4 : 1,
      },
      credential: { label: "primary", api_key: form.apiKey, routing_weight: 1 },
    });
  }

  const field = (key: keyof typeof form, label: string, type = "text") => (
    <label className="flex min-w-0 flex-col gap-1.5">
      <span className="text-[9px] font-bold uppercase tracking-wide text-fg opacity-50">
        {label}
      </span>
      <input
        className={INPUT}
        type={type}
        value={form[key]}
        required
        autoComplete={key === "apiKey" ? "off" : undefined}
        onChange={(event) => setForm((current) => ({ ...current, [key]: event.target.value }))}
      />
    </label>
  );

  return (
    <Card className="gap-4">
      <div>
        <div className="text-[12px] font-semibold text-fg">Provider onboarding</div>
        <p className="mt-1 text-[10px] text-fg opacity-48">
          Add the provider and its first encrypted credential, then discover and select models.
          The API key is never returned by PolyFlare.
        </p>
        <div className="mt-2 flex flex-wrap gap-1.5">
          <button
            type="button"
            onClick={() =>
              setForm({
                providerName: "OpenRouter",
                providerSlug: "openrouter",
                baseUrl: "https://openrouter.ai/api/v1",
                apiKey: form.apiKey,
              })
            }
            className="rounded border border-signal/30 px-2 py-1 text-[8.5px] font-semibold text-signal"
          >
            Use OpenRouter preset
          </button>
          <button
            type="button"
            onClick={() =>
              setForm({
                providerName: "Sakana",
                providerSlug: "sakana",
                baseUrl: "https://api.sakana.ai/v1",
                apiKey: form.apiKey,
              })
            }
            className="rounded border border-border px-2 py-1 text-[8.5px] font-semibold text-fg opacity-60"
          >
            Use Sakana preset
          </button>
        </div>
      </div>
      <form onSubmit={submit} className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
        {field("providerName", "Provider name")}
        {field("providerSlug", "Provider slug")}
        {field("baseUrl", "Base URL", "url")}
        {field("apiKey", "API key", "password")}
        <div className="flex items-end md:col-span-2 xl:col-span-4">
          <button
            type="submit"
            disabled={pending}
            className="h-9 rounded-lg bg-accent px-5 text-[11px] font-semibold text-white disabled:opacity-45"
          >
            {pending ? "Adding provider…" : "Add provider and credential"}
          </button>
        </div>
      </form>
    </Card>
  );
}
