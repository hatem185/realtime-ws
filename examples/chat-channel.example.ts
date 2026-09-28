/**
 * Example: a bidirectional chat channel. Subscribers must be members
 * of the target space; clients can both send messages (broadcast to
 * everyone in the topic) and trigger ephemeral typing indicators.
 *
 * The membership / persistence services below are stubs — replace with
 * the equivalents in your custom-api project (e.g. `SpaceService`,
 * `ChatService`).
 */

import { BaseChannel, type AuthIdentity, type RealtimeHub } from "../src";

interface SpaceMembershipChecker {
    isMember(userId: string, spaceId: string): Promise<boolean>;
}

interface ChatPersister {
    save(input: {
        spaceId: string;
        senderId: string;
        type: string;
        content: Record<string, unknown>;
    }): Promise<{ id: string; createdAt: Date }>;
}

export class ChatChannel extends BaseChannel {
    readonly name = "chat";

    constructor(
        private readonly hub: RealtimeHub,
        private readonly membership: SpaceMembershipChecker,
        private readonly persister: ChatPersister,
    ) {
        super();
    }

    private extractSpaceId(topic: string): string | null {
        if (!topic.startsWith("space:")) return null;
        return topic.slice("space:".length) || null;
    }

    async authorize(identity: AuthIdentity, topic: string): Promise<boolean> {
        const spaceId = this.extractSpaceId(topic);
        if (!spaceId) return false;
        return this.membership.isMember(identity.userId, spaceId);
    }

    override async onSubscribe(
        _connectionId: string,
        identity: AuthIdentity,
        topic: string,
    ): Promise<void> {
        await this.hub.publish("chat", topic, {
            chatEvent: "presence",
            userId: identity.userId,
            status: "online",
        });
    }

    override async onMessage(
        _connectionId: string,
        identity: AuthIdentity,
        topic: string,
        payload: unknown,
    ): Promise<{ broadcast?: unknown } | void> {
        if (!payload || typeof payload !== "object") return;
        const msg = payload as Record<string, unknown>;

        // Ephemeral typing indicator
        if (msg.chatAction === "typing" || msg.chatAction === "stopTyping") {
            return {
                broadcast: {
                    chatEvent: msg.chatAction,
                    userId: identity.userId,
                },
            };
        }

        const content = msg.content as Record<string, unknown> | undefined;
        const spaceId = this.extractSpaceId(topic);
        if (!content || !spaceId) return;

        const saved = await this.persister.save({
            spaceId,
            senderId: identity.userId,
            type: (msg.type as string) ?? "text",
            content,
        });

        return {
            broadcast: {
                chatEvent: "newMessage",
                messageId: saved.id,
                senderId: identity.userId,
                content,
                sentAt: saved.createdAt.toISOString(),
            },
        };
    }
}
