import assert from "node:assert/strict";
import test from "node:test";

import { paginationWindow } from "../src/lib/pagination.ts";

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
