import assert from "node:assert/strict";
import test from "node:test";

import {
  getResetCreditRecoveryStorage,
  loadResetCreditRecovery,
  saveResetCreditRecovery,
  type ResetCreditRecovery,
} from "../src/lib/resetCreditRecovery.ts";

class MemoryStorage {
  private readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

test("exact fleet recovery survives a fresh dashboard component", () => {
  const storage = new MemoryStorage();
  const recovery: ResetCreditRecovery = {
    ids: ["acct-a", "acct-b"],
    redeemRequestId: "fleet-request-1",
    response: {
      results: [{ account_id: "acct-a", code: "reset", windows_reset: 2, redeemed_at: null }],
      errors: [{ account_id: "acct-b", message: "reset-credit upstream unavailable" }],
    },
  };

  assert.equal(saveResetCreditRecovery(storage, recovery), true);
  assert.deepEqual(loadResetCreditRecovery(storage), recovery);
});

test("malformed or incomplete recovery state fails closed", () => {
  const storage = new MemoryStorage();
  storage.setItem("polyflare.reset-credit-recovery.v1", '{"ids":[],"redeemRequestId":"x"}');

  assert.equal(loadResetCreditRecovery(storage), null);
  assert.equal(storage.getItem("polyflare.reset-credit-recovery.v1"), null);
});

test("unavailable browser storage never blocks the reset dashboard", () => {
  const storage = {
    getItem(): string | null {
      throw new Error("storage unavailable");
    },
    setItem(): void {
      throw new Error("storage unavailable");
    },
    removeItem(): void {
      throw new Error("storage unavailable");
    },
  };

  assert.equal(loadResetCreditRecovery(storage), null);
  assert.equal(saveResetCreditRecovery(storage, null), false);
  assert.equal(saveResetCreditRecovery(storage, {
    ids: ["acct-a"],
    redeemRequestId: "fleet-request-1",
    response: { results: [], errors: [] },
  }), false);
});

test("browser storage accessor failures are contained", () => {
  const browser = Object.defineProperty({}, "localStorage", {
    get(): never {
      throw new Error("access denied");
    },
  });

  assert.equal(getResetCreditRecoveryStorage(browser), null);
});

test("recovery entries are validated deeply", () => {
  const storage = new MemoryStorage();
  storage.setItem(
    "polyflare.reset-credit-recovery.v1",
    JSON.stringify({
      ids: ["acct-a"],
      redeemRequestId: "fleet-request-1",
      response: {
        results: [{ account_id: "acct-a", code: 7, windows_reset: 1, redeemed_at: null }],
        errors: [],
      },
    }),
  );

  assert.equal(loadResetCreditRecovery(storage), null);
});
