import assert from "node:assert/strict";
import test from "node:test";

import { clampPageOffset, paginationWindow } from "../src/lib/pagination.ts";

test("paginationWindow keeps a stable five-page window around the current page", () => {
  assert.deepEqual(paginationWindow(1, 20), [1, 2, 3, 4, 5]);
  assert.deepEqual(paginationWindow(10, 20), [8, 9, 10, 11, 12]);
  assert.deepEqual(paginationWindow(20, 20), [16, 17, 18, 19, 20]);
});

test("paginationWindow handles short and out-of-range result sets", () => {
  assert.deepEqual(paginationWindow(2, 3), [1, 2, 3]);
  assert.deepEqual(paginationWindow(99, 3), [1, 2, 3]);
  assert.deepEqual(paginationWindow(0, 0), [1]);
});

test("clampPageOffset returns the final valid page after retention shrinks results", () => {
  assert.equal(clampPageOffset(100, 51, 25), 50);
  assert.equal(clampPageOffset(50, 50, 25), 25);
  assert.equal(clampPageOffset(25, 0, 25), 0);
});

test("clampPageOffset preserves valid offsets and rejects invalid inputs", () => {
  assert.equal(clampPageOffset(25, 100, 25), 25);
  assert.equal(clampPageOffset(-10, 100, 25), 0);
  assert.equal(clampPageOffset(25, 100, 0), 0);
});
