/**
 * Example: a "notifications" channel where each user can only listen to
 * their own feed. All messages are server-pushed via `hub.publishToUser`
 * — the client never sends anything on this channel.
 */

import { BaseChannel, type AuthIdentity } from "../src";

export class NotificationsChannel extends BaseChannel {
    readonly name = "notifications";

    authorize(identity: AuthIdentity, topic: string): boolean {
        // Topic must be `user:<userId>` and must match the connected user.
        if (!topic.startsWith("user:")) return false;
        return topic.slice("user:".length) === identity.userId;
    }
    // No onMessage — clients can't push to this channel.
}
