export function paginationWindow(
  currentPage: number,
  totalPages: number,
  radius = 2,
): number[] {
  const safeTotal = Math.max(1, Math.floor(totalPages));
  const safeCurrent = Math.min(safeTotal, Math.max(1, Math.floor(currentPage)));
  const safeRadius = Math.max(0, Math.floor(radius));
  const desired = safeRadius * 2 + 1;

  let start = Math.max(1, safeCurrent - safeRadius);
  let end = Math.min(safeTotal, safeCurrent + safeRadius);

  if (end - start + 1 < desired) {
    if (start === 1) end = Math.min(safeTotal, start + desired - 1);
    if (end === safeTotal) start = Math.max(1, end - desired + 1);
  }

  return Array.from({ length: end - start + 1 }, (_, index) => start + index);
}
