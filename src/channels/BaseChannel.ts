import type { AuthIdentity, IChannel } from "../types.ts";

/**
 * Convenience base for channels — provides no-op defaults for every
 * lifecycle hook so subclasses only override what they care about.
 *
 * @example
 * ```ts
 * export class NotificationsChannel extends BaseChannel {
 *   readonly name = "notifications";
 *   override authorize(identity, topic) {
 *     return topic === `user:${identity.userId}`;
 *   }
 * }
 * ```
 */
export abstract class BaseChannel implements IChannel {
    abstract readonly name: string;
    abstract authorize(identity: AuthIdentity, topic: string): Promise<boolean> | boolean;

    onSubscribe(_connectionId: string, _identity: AuthIdentity, _topic: string): void {
        // no-op
    }

    onUnsubscribe(_connectionId: string, _identity: AuthIdentity, _topic: string): void {
        // no-op
    }

    async onMessage(
        _connectionId: string,
        _identity: AuthIdentity,
        _topic: string,
        _payload: unknown,
    ): Promise<{ broadcast?: unknown } | void> {
        // no-op (server-push only by default)
    }

    onDisconnect(_connectionId: string, _identity: AuthIdentity): void {
        // no-op
    }
}
