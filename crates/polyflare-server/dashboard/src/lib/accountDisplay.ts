import type { AccountView } from "./api";

type AccountIdentity = Pick<AccountView, "id" | "email" | "alias">;

/** Human-readable account identity for dense operational surfaces. */
export function accountDisplayLabel(account: AccountIdentity | undefined, accountId: string): string {
  const alias = account?.alias?.trim();
  if (alias) return alias;

  const email = account?.email.trim();
  if (email) return email;

  return shortenAccountId(accountId);
}

export function shortenAccountId(accountId: string): string {
  if (accountId.length <= 18) return accountId;
  return `${accountId.slice(0, 8)}…${accountId.slice(-4)}`;
}
