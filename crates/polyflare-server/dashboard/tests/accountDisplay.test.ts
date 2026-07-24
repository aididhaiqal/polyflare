import assert from "node:assert/strict";
import test from "node:test";

import { accountDisplayLabel, shortenAccountId } from "../src/lib/accountDisplay.ts";

test("accountDisplayLabel prefers nickname, then email", () => {
  assert.equal(
    accountDisplayLabel(
      { id: "account-1", alias: "Primary route", email: "operator@example.com" },
      "account-1",
    ),
    "Primary route",
  );
  assert.equal(
    accountDisplayLabel(
      { id: "account-2", alias: "  ", email: "operator@example.com" },
      "account-2",
    ),
    "operator@example.com",
  );
});

test("accountDisplayLabel never exposes a long full id as its fallback", () => {
  const id = "a79168e7-4948-4b4b-8bc8-daab8d8604fd_488f8c8e";

  assert.equal(accountDisplayLabel(undefined, id), "a79168e7…8c8e");
  assert.equal(shortenAccountId("short-route"), "short-route");
});
