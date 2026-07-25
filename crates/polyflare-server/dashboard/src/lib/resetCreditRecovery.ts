import type { FleetResetRedeemResponse } from "./api";

const STORAGE_KEY = "polyflare.reset-credit-recovery.v1";

export interface ResetCreditRecovery {
  ids: string[];
  redeemRequestId: string;
  response: FleetResetRedeemResponse;
}

interface RecoveryStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

interface RecoveryStorageOwner {
  readonly localStorage: RecoveryStorage;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isResult(value: unknown): value is FleetResetRedeemResponse["results"][number] {
  return (
    isRecord(value) &&
    typeof value.account_id === "string" &&
    value.account_id.trim() !== "" &&
    typeof value.code === "string" &&
    value.code.trim() !== "" &&
    typeof value.windows_reset === "number" &&
    Number.isSafeInteger(value.windows_reset) &&
    value.windows_reset >= 0 &&
    (value.redeemed_at === null ||
      (typeof value.redeemed_at === "number" &&
        Number.isSafeInteger(value.redeemed_at) &&
        value.redeemed_at >= 0))
  );
}

function isError(value: unknown): value is FleetResetRedeemResponse["errors"][number] {
  return (
    isRecord(value) &&
    typeof value.account_id === "string" &&
    value.account_id.trim() !== "" &&
    typeof value.message === "string" &&
    value.message.trim() !== ""
  );
}

function isRecovery(value: unknown): value is ResetCreditRecovery {
  if (!isRecord(value) || !Array.isArray(value.ids) || !isRecord(value.response)) return false;
  if (
    typeof value.redeemRequestId !== "string" ||
    value.redeemRequestId.trim() === "" ||
    value.ids.length === 0 ||
    value.ids.some((id) => typeof id !== "string" || id.trim() === "") ||
    new Set(value.ids).size !== value.ids.length
  ) {
    return false;
  }
  const ids = value.ids as string[];
  if (
    !Array.isArray(value.response.results) ||
    !value.response.results.every(isResult) ||
    !Array.isArray(value.response.errors) ||
    !value.response.errors.every(isError)
  ) {
    return false;
  }
  const responseIds = [
    ...value.response.results.map((result) => result.account_id),
    ...value.response.errors.map((error) => error.account_id),
  ];
  return (
    responseIds.length === ids.length &&
    new Set(responseIds).size === responseIds.length &&
    responseIds.every((id) => ids.includes(id))
  );
}

export function getResetCreditRecoveryStorage(
  owner: RecoveryStorageOwner,
): RecoveryStorage | null {
  try {
    return owner.localStorage;
  } catch {
    return null;
  }
}

export function loadResetCreditRecovery(
  storage: RecoveryStorage | null,
): ResetCreditRecovery | null {
  if (storage === null) return null;
  try {
    const raw = storage.getItem(STORAGE_KEY);
    if (raw === null) return null;
    const parsed: unknown = JSON.parse(raw);
    if (isRecovery(parsed)) return parsed;
    storage.removeItem(STORAGE_KEY);
  } catch {
    // Unavailable or malformed browser state must not block the dashboard or invent an operation.
  }
  return null;
}

export function saveResetCreditRecovery(
  storage: RecoveryStorage | null,
  recovery: ResetCreditRecovery | null,
): boolean {
  if (storage === null) return false;
  try {
    if (recovery === null) {
      storage.removeItem(STORAGE_KEY);
      return true;
    }
    storage.setItem(STORAGE_KEY, JSON.stringify(recovery));
    return true;
  } catch {
    return false;
  }
}
