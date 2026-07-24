import type { ProviderModelView } from "./api";

export function isProviderModelProfile(model: ProviderModelView): boolean {
  return (
    model.instruction_mode !== "none" ||
    model.request_overrides.reasoning_effort !== undefined ||
    model.request_overrides.max_output_tokens !== undefined
  );
}

export function providerProfileTemplate(model: ProviderModelView): {
  publicModel: string;
  displayName: string;
  instructionMode: "append";
} {
  return {
    publicModel: `${model.public_model}~profile`,
    displayName: `${model.display_name} · Profile`,
    instructionMode: "append",
  };
}
