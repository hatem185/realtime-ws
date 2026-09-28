import type { AuthIdentity, TokenAuthenticator } from "../types";

/**
 * Tiny adapter that wraps a plain `verify(token)` function as a
 * {@link TokenAuthenticator}. Saves callers from creating a class
 * just to satisfy the interface.
 *
 * @example
 * ```ts
 * import { JWTManager } from "../app/utils/JWTManager";
 *
 * const authenticator = createFunctionAuthenticator((token) => {
 *   const payload = JWTManager.verifyToken(token);
 *   return { userId: payload.id, ...payload };
 * });
 * ```
 */
export function createFunctionAuthenticator(
    verify: (token: string) => Promise<AuthIdentity> | AuthIdentity,
): TokenAuthenticator {
    return { verify };
}
