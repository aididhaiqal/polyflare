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

/**
 * The display name for one account, disambiguated against the accounts shown beside it.
 *
 * One login email can back SEVERAL ChatGPT seats — this deployment has two accounts on the same
 * gmail address with no alias between them — so labelling by email alone would render two
 * different accounts identically. That is worse than showing an id: it makes them look like one
 * account. Where a label would collide, and only there, a short id fragment is appended.
 *
 * The fix for a collided label is to nickname one of them; this keeps the UI honest until then.
 */
export function accountLabel(
  account: AccountIdentity,
  siblings?: readonly AccountIdentity[],
): string {
  const base = accountDisplayLabel(account, account.id);
  if (!siblings) return base;
  const collides = siblings.some(
    (other) => other.id !== account.id && accountDisplayLabel(other, other.id) === base,
  );
  return collides ? `${base} · ${shortenAccountId(account.id)}` : base;
}

export function shortenAccountId(accountId: string): string {
  if (accountId.length <= 18) return accountId;
  return `${accountId.slice(0, 8)}…${accountId.slice(-4)}`;
}
