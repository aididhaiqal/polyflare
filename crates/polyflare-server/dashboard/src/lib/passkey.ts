// Browser half of the WebAuthn ceremonies.
//
// The server speaks the JSON encoding from the WebAuthn spec (base64url strings for every binary
// field), while `navigator.credentials` wants and returns ArrayBuffers. These helpers do only that
// translation — no credential ever changes meaning here, and no private key is ever visible to
// this code: the authenticator signs, and we forward the signature.

import { fetchJson } from "./api";

function base64urlToBuffer(value: string): ArrayBuffer {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(padded.padEnd(padded.length + ((4 - (padded.length % 4)) % 4), "="));
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
}

function bufferToBase64url(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  for (let i = 0; i < bytes.length; i += 1) binary += String.fromCharCode(bytes[i]);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Whether this browser can do passkeys at all. */
export function passkeysAvailable(): boolean {
  return typeof window !== "undefined" && !!window.PublicKeyCredential;
}

export interface AuthStatus {
  passkey_supported: boolean;
  passkey_registered: boolean;
  authenticated: boolean;
}

export function getAuthStatus(): Promise<AuthStatus> {
  return fetchJson<AuthStatus>("/api/auth/status");
}

/** Registers a new passkey. Must be called from an already-authenticated session. */
export async function registerPasskey(label: string): Promise<void> {
  const start = await fetchJson<{ handle: string; options: { publicKey: PublicKeyCredentialCreationOptions } }>(
    "/api/auth/passkey/register/start",
    { method: "POST" },
  );
  const publicKey = start.options.publicKey as unknown as Record<string, unknown>;
  const request: PublicKeyCredentialCreationOptions = {
    ...(publicKey as unknown as PublicKeyCredentialCreationOptions),
    challenge: base64urlToBuffer(publicKey.challenge as string),
    user: {
      ...(publicKey.user as PublicKeyCredentialUserEntity),
      id: base64urlToBuffer((publicKey.user as unknown as { id: string }).id),
    },
    excludeCredentials: ((publicKey.excludeCredentials as Array<{ id: string; type: string }>) ?? []).map(
      (c) => ({ ...c, id: base64urlToBuffer(c.id) }) as PublicKeyCredentialDescriptor,
    ),
  };

  const credential = (await navigator.credentials.create({ publicKey: request })) as PublicKeyCredential | null;
  if (!credential) throw new Error("passkey creation cancelled");
  const response = credential.response as AuthenticatorAttestationResponse;

  await fetchJson("/api/auth/passkey/register/finish", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      handle: start.handle,
      label,
      credential: {
        id: credential.id,
        rawId: bufferToBase64url(credential.rawId),
        type: credential.type,
        response: {
          attestationObject: bufferToBase64url(response.attestationObject),
          clientDataJSON: bufferToBase64url(response.clientDataJSON),
        },
        extensions: {},
      },
    }),
  });
}

/** Runs an assertion and returns the session token the server minted for it. */
export async function signInWithPasskey(): Promise<string> {
  const start = await fetchJson<{ handle: string; options: { publicKey: PublicKeyCredentialRequestOptions } }>(
    "/api/auth/passkey/login/start",
    { method: "POST" },
  );
  const publicKey = start.options.publicKey as unknown as Record<string, unknown>;
  const request: PublicKeyCredentialRequestOptions = {
    ...(publicKey as unknown as PublicKeyCredentialRequestOptions),
    challenge: base64urlToBuffer(publicKey.challenge as string),
    allowCredentials: ((publicKey.allowCredentials as Array<{ id: string; type: string }>) ?? []).map(
      (c) => ({ ...c, id: base64urlToBuffer(c.id) }) as PublicKeyCredentialDescriptor,
    ),
  };

  const credential = (await navigator.credentials.get({ publicKey: request })) as PublicKeyCredential | null;
  if (!credential) throw new Error("passkey sign-in cancelled");
  const response = credential.response as AuthenticatorAssertionResponse;

  const finished = await fetchJson<{ token: string; expires_at: number }>(
    "/api/auth/passkey/login/finish",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        handle: start.handle,
        credential: {
          id: credential.id,
          rawId: bufferToBase64url(credential.rawId),
          type: credential.type,
          response: {
            authenticatorData: bufferToBase64url(response.authenticatorData),
            clientDataJSON: bufferToBase64url(response.clientDataJSON),
            signature: bufferToBase64url(response.signature),
            userHandle: response.userHandle ? bufferToBase64url(response.userHandle) : null,
          },
          extensions: {},
        },
      }),
    },
  );
  return finished.token;
}

export interface PasskeyView {
  id: string;
  label: string;
  created_at: number;
  last_used_at: number | null;
}

export function listPasskeys(): Promise<PasskeyView[]> {
  return fetchJson<PasskeyView[]>("/api/auth/passkeys");
}

export function deletePasskey(id: string): Promise<{ ok: boolean }> {
  return fetchJson<{ ok: boolean }>(`/api/auth/passkeys/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
}

export function logout(): Promise<{ ok: boolean }> {
  return fetchJson<{ ok: boolean }>("/api/auth/logout", { method: "POST" });
}
