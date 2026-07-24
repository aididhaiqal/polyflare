export type BackendRequestKind = "synthetic_usage" | "passthrough";

export interface BackendRequestDisplay {
  kind: BackendRequestKind;
  targetLabel: "ChatGPT backend";
  operationLabel: "Synthetic usage" | "Backend passthrough";
}

export function backendRequestDisplay(row: {
  provider: string;
  path: string;
}): BackendRequestDisplay | null {
  const hasBackendPath =
    row.path.startsWith("chatgpt_backend_synthetic_") ||
    row.path.startsWith("chatgpt_backend_passthrough_");
  if (row.provider !== "chatgpt_backend" && !hasBackendPath) return null;
  if (row.path.startsWith("chatgpt_backend_synthetic_")) {
    return {
      kind: "synthetic_usage",
      targetLabel: "ChatGPT backend",
      operationLabel: "Synthetic usage",
    };
  }
  return {
    kind: "passthrough",
    targetLabel: "ChatGPT backend",
    operationLabel: "Backend passthrough",
  };
}
