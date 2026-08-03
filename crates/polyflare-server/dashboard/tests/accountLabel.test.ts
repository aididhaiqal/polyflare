import assert from "node:assert/strict";
import { test } from "node:test";

import { accountLabel } from "../src/lib/accountDisplay.ts";

const acc = (id: string, email: string, alias: string | null = null) => ({ id, email, alias });

test("an account is named by nickname, else email — never its internal id", () => {
  assert.equal(accountLabel(acc("a79168e7-4948-4b4b", "sam@example.test")), "sam@example.test");
  assert.equal(
    accountLabel(acc("a79168e7-4948-4b4b", "sam@example.test", "Work seat")),
    "Work seat",
  );
});

test("two seats on one email stay distinguishable", () => {
  // The real case: one gmail address backing two ChatGPT seats, neither nicknamed. Labelling both
  // by email alone would make two different accounts look like one.
  const a = acc("a79168e7-4948-4b4b-8bc8-daab8d8604fd_488", "wm@example.test");
  const b = acc("6da27c17-c575-4282-81ae-0841f6f6caa5_488", "wm@example.test");
  const all = [a, b];
  const first = accountLabel(a, all);
  const second = accountLabel(b, all);
  assert.notEqual(first, second, "collided labels must not be identical");
  assert.ok(first.startsWith("wm@example.test"), `expected the email to lead, got ${first}`);
  assert.ok(second.startsWith("wm@example.test"), `expected the email to lead, got ${second}`);
});

test("nicknaming one of a colliding pair removes the disambiguator", () => {
  const a = acc("a79168e7-4948", "wm@example.test", "Personal");
  const b = acc("6da27c17-c575", "wm@example.test");
  const all = [a, b];
  assert.equal(accountLabel(a, all), "Personal");
  assert.equal(accountLabel(b, all), "wm@example.test", "no collision remains, so no id fragment");
});

test("a unique email is never cluttered with an id", () => {
  const all = [acc("id-1", "one@example.test"), acc("id-2", "two@example.test")];
  assert.equal(accountLabel(all[0], all), "one@example.test");
});

test("an account with neither nickname nor email falls back to a short id", () => {
  const label = accountLabel(acc("codex_754e4ebf-728d-4865-9ae1-63aef73fff5b", ""));
  assert.ok(label.length > 0 && label.length < 25, `expected a short fallback, got ${label}`);
});
