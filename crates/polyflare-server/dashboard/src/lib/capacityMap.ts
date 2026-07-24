export interface CapacityMapAccount {
  id: string;
  weekly: {
    used_percent: number;
  } | null;
}

/** Weekly-observed accounts ordered from most constrained to most available. */
export function capacityMapAccounts<T extends CapacityMapAccount>(accounts: T[]): T[] {
  return accounts
    .filter((account) => account.weekly !== null)
    .sort((a, b) => (b.weekly?.used_percent ?? 0) - (a.weekly?.used_percent ?? 0));
}
