import { useState, type FormEvent } from "react";

import type { CreateProviderModelBody } from "../lib/api";
import {
  useAddProviderCredential,
  useAddProviderModel,
  useCreateProviderBundle,
  useProviderAction,
  useProviders,
  useTestProvider,
} from "../lib/queries";
import { Card } from "../ui/Card";
import { AlertTriangle, KeyRound, Plus, Route, Trash2 } from "../ui/icons";

const INPUT =
  "h-9 w-full rounded-lg border border-border bg-bg/65 px-3 text-[11px] text-fg outline-none transition focus:border-accent/60";

export function Providers() {
  const providers = useProviders();
  const create = useCreateProviderBundle();
  const addCredential = useAddProviderCredential();
  const addModel = useAddProviderModel();
  const action = useProviderAction();
  const testConnection = useTestProvider();
  const [open, setOpen] = useState(false);
  const [addTarget, setAddTarget] = useState<{
    providerId: string;
    kind: "credential" | "model";
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
                    <button
                      type="button"
                      onClick={() => setAddTarget({ providerId: provider.id, kind: "model" })}
                      className="text-[9px] font-semibold text-accent"
                    >
                      + model
                    </button>
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
                  pending={addModel.isPending}
                  onCancel={() => setAddTarget(null)}
                  onSubmit={(model) =>
                    addModel.mutate(
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
  pending,
  onSubmit,
  onCancel,
}: {
  pending: boolean;
  onSubmit: (model: CreateProviderModelBody) => void;
  onCancel: () => void;
}) {
  const [publicModel, setPublicModel] = useState("");
  const [upstreamModel, setUpstreamModel] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [contextWindow, setContextWindow] = useState("");
  const [visibleInCodex, setVisibleInCodex] = useState(true);
  const [visibleInOpenAi, setVisibleInOpenAi] = useState(true);
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
          supports_tools: true,
          supports_parallel_tool_calls: true,
          visible_in_codex: visibleInCodex,
          visible_in_openai: visibleInOpenAi,
        });
      }}
    >
      <input
        className={INPUT}
        value={publicModel}
        required
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
          Add
        </button>
        <button type="button" onClick={onCancel} className="px-2 text-[10px] text-fg opacity-55">
          Cancel
        </button>
      </div>
      <div className="flex flex-wrap gap-3 sm:col-span-2">
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
    publicModel: "fugu-ultra",
    upstreamModel: "fugu-ultra-v1.1",
    modelName: "Fugu Ultra",
    contextWindow: "1000000",
    inputPrice: "",
    cachedPrice: "",
    outputPrice: "",
  });
  const [visibleInCodex, setVisibleInCodex] = useState(true);
  const [visibleInOpenAi, setVisibleInOpenAi] = useState(true);

  function submit(event: FormEvent) {
    event.preventDefault();
    const number = (value: string) => (value.trim() ? Number(value) : undefined);
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
      model: {
        public_model: form.publicModel,
        upstream_model: form.upstreamModel,
        display_name: form.modelName,
        context_window: number(form.contextWindow),
        supports_tools: true,
        supports_vision: true,
        supports_parallel_tool_calls: true,
        supports_web_search: true,
        supports_reasoning_summaries: true,
        reasoning_levels: ["high", "xhigh", "max"],
        input_per_million: number(form.inputPrice),
        cached_input_per_million: number(form.cachedPrice),
        output_per_million: number(form.outputPrice),
        visible_in_codex: visibleInCodex,
        visible_in_openai: visibleInOpenAi,
      },
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
        required={key !== "inputPrice" && key !== "cachedPrice" && key !== "outputPrice"}
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
          The API key is encrypted at rest and is never returned by PolyFlare.
        </p>
      </div>
      <form onSubmit={submit} className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
        {field("providerName", "Provider name")}
        {field("providerSlug", "Provider slug")}
        {field("baseUrl", "Base URL", "url")}
        {field("apiKey", "API key", "password")}
        {field("publicModel", "Public model")}
        {field("upstreamModel", "Upstream model")}
        {field("modelName", "Model display name")}
        {field("contextWindow", "Context window", "number")}
        {field("inputPrice", "Input / 1M USD", "number")}
        {field("cachedPrice", "Cached input / 1M USD", "number")}
        {field("outputPrice", "Output / 1M USD", "number")}
        <div className="flex flex-wrap items-end gap-4">
          <label className="flex h-9 items-center gap-2 text-[9px] text-fg opacity-70">
            <input
              type="checkbox"
              checked={visibleInCodex}
              onChange={(event) => setVisibleInCodex(event.target.checked)}
            />
            Codex picker
          </label>
          <label className="flex h-9 items-center gap-2 text-[9px] text-fg opacity-70">
            <input
              type="checkbox"
              checked={visibleInOpenAi}
              onChange={(event) => setVisibleInOpenAi(event.target.checked)}
            />
            OpenAI list
          </label>
        </div>
        <div className="flex items-end">
          <button
            type="submit"
            disabled={pending}
            className="h-9 w-full rounded-lg bg-accent px-3 text-[11px] font-semibold text-white disabled:opacity-45"
          >
            {pending ? "Adding provider…" : "Add provider, key, and model"}
          </button>
        </div>
      </form>
    </Card>
  );
}
