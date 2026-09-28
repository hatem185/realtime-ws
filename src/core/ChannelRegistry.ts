import { randomUUID } from "crypto";
import type { AuthIdentity, IChannel, Logger } from "../types";
import type { RedisPubSub } from "./RedisPubSub";

type DeliveryFn = (connectionId: string, message: string) => void;
type GetUserConnsFn = (userId: string) => Set<string>;

interface RedisEnvelope {
    _instanceId?: string;
    message?: string;
}

/**
 * Registry of {@link IChannel} implementations + topic subscription
 * routing. One instance per {@link RealtimeHub}.
 *
 * Cross-instance fan-out is achieved by mirroring publishes onto a
 * Redis Pub/Sub channel (when configured). Each instance tags its
 * own broadcasts with `_instanceId` so the publisher does not
 * re-deliver them locally.
 */
export class ChannelRegistry {
    private readonly channels = new Map<string, IChannel>();
    private readonly topicSubscribers = new Map<string, Set<string>>();
    private readonly redisHandlers = new Map<string, (msg: string) => void>();
    private readonly instanceId = randomUUID();

    private deliverFn: DeliveryFn | null = null;
    private getUserConnsFn: GetUserConnsFn | null = null;

    constructor(
        private readonly pubsub: RedisPubSub,
        private readonly logger: Logger,
    ) {}

    /** Wired by {@link ConnectionManager} during hub bootstrap. */
    setDeliveryHandler(fn: DeliveryFn): void {
        this.deliverFn = fn;
    }

    /** Wired by {@link ConnectionManager} during hub bootstrap. */
    setUserConnectionsResolver(fn: GetUserConnsFn): void {
        this.getUserConnsFn = fn;
    }

    register(channel: IChannel): void {
        if (this.channels.has(channel.name)) {
            this.logger.warn(`Channel "${channel.name}" already registered, replacing.`);
        }
        this.channels.set(channel.name, channel);
        this.logger.info(`Channel "${channel.name}" registered.`);
    }

    unregister(name: string): void {
        this.channels.delete(name);
    }

    getChannel(name: string): IChannel | undefined {
        return this.channels.get(name);
    }

    getChannelNames(): string[] {
        return [...this.channels.keys()];
    }

    private topicKey(channel: string, topic: string): string {
        return `${channel}:${topic}`;
    }

    async subscribe(
        connectionId: string,
        identity: AuthIdentity,
        channelName: string,
        topic: string,
    ): Promise<boolean> {
        const channel = this.channels.get(channelName);
        if (!channel) return false;

        const authorized = await channel.authorize(identity, topic);
        if (!authorized) return false;

        const key = this.topicKey(channelName, topic);
        let subscribers = this.topicSubscribers.get(key);
        const isFirstLocal = !subscribers || subscribers.size === 0;

        if (!subscribers) {
            subscribers = new Set();
            this.topicSubscribers.set(key, subscribers);
        }
        subscribers.add(connectionId);

        if (isFirstLocal && this.pubsub.isAvailable()) {
            const handler = (raw: string) => this.handleRedisMessage(key, raw);
            this.redisHandlers.set(key, handler);
            await this.pubsub.subscribe(key, handler);
        }

        if (channel.onSubscribe) {
            try {
                await channel.onSubscribe(connectionId, identity, topic);
            } catch (err) {
                this.logger.error(`onSubscribe error for ${channelName}:`, err);
            }
        }
        return true;
    }

    async unsubscribe(
        connectionId: string,
        identity: AuthIdentity,
        channelName: string,
        topic: string,
    ): Promise<void> {
        const channel = this.channels.get(channelName);
        const key = this.topicKey(channelName, topic);
        const subscribers = this.topicSubscribers.get(key);

        if (subscribers) {
            subscribers.delete(connectionId);
            if (subscribers.size === 0) {
                this.topicSubscribers.delete(key);
                const handler = this.redisHandlers.get(key);
                if (handler) {
                    await this.pubsub.unsubscribe(key, handler);
                    this.redisHandlers.delete(key);
                }
            }
        }

        if (channel?.onUnsubscribe) {
            try {
                await channel.onUnsubscribe(connectionId, identity, topic);
            } catch (err) {
                this.logger.error(`onUnsubscribe error for ${channelName}:`, err);
            }
        }
    }

    async handleMessage(
        connectionId: string,
        identity: AuthIdentity,
        channelName: string,
        topic: string,
        payload: unknown,
    ): Promise<void> {
        const channel = this.channels.get(channelName);
        if (!channel?.onMessage) return;

        try {
            const result = await channel.onMessage(connectionId, identity, topic, payload);
            if (result && "broadcast" in result && result.broadcast !== undefined) {
                await this.publish(channelName, topic, result.broadcast);
            }
        } catch (err) {
            this.logger.error(`onMessage error for ${channelName}:`, err);
        }
    }

    async handleDisconnect(
        connectionId: string,
        identity: AuthIdentity,
        subscribedKeys: Set<string>,
    ): Promise<void> {
        for (const key of subscribedKeys) {
            const subscribers = this.topicSubscribers.get(key);
            if (!subscribers) continue;
            subscribers.delete(connectionId);
            if (subscribers.size === 0) {
                this.topicSubscribers.delete(key);
                const handler = this.redisHandlers.get(key);
                if (handler) {
                    await this.pubsub.unsubscribe(key, handler);
                    this.redisHandlers.delete(key);
                }
            }
        }

        const notified = new Set<string>();
        for (const key of subscribedKeys) {
            const channelName = key.split(":")[0];
            if (notified.has(channelName)) continue;
            notified.add(channelName);
            const channel = this.channels.get(channelName);
            if (!channel?.onDisconnect) continue;
            try {
                await channel.onDisconnect(connectionId, identity);
            } catch (err) {
                this.logger.error(`onDisconnect error for ${channelName}:`, err);
            }
        }
    }

    /**
     * Publish a payload to every subscriber of `channel:topic`
     * (local + remote via Redis).
     */
    async publish(channelName: string, topic: string, payload: unknown): Promise<void> {
        const key = this.topicKey(channelName, topic);
        const message = JSON.stringify({
            event: "message",
            channel: channelName,
            topic,
            payload,
        });

        this.deliverToLocalSubscribers(key, message);

        if (this.pubsub.isAvailable()) {
            const envelope = JSON.stringify({ _instanceId: this.instanceId, message });
            await this.pubsub.publish(key, envelope);
        }
    }

    /**
     * Publish to every connection of a specific user — useful for
     * notifications and other user-targeted events.
     */
    async publishToUser(
        userId: string,
        channelName: string,
        payload: unknown,
        topic: string = `user:${userId}`,
    ): Promise<void> {
        const message = JSON.stringify({
            event: "message",
            channel: channelName,
            topic,
            payload,
        });

        if (this.deliverFn && this.getUserConnsFn) {
            const conns = this.getUserConnsFn(userId);
            for (const id of conns) {
                try {
                    this.deliverFn(id, message);
                } catch (err) {
                    this.logger.error(`Delivery to ${id} failed:`, err);
                }
            }
        }

        if (this.pubsub.isAvailable()) {
            const key = this.topicKey(channelName, topic);
            const envelope = JSON.stringify({ _instanceId: this.instanceId, message });
            await this.pubsub.publish(key, envelope);
        }
    }

    getSubscriberCount(channelName: string, topic: string): number {
        return this.topicSubscribers.get(this.topicKey(channelName, topic))?.size ?? 0;
    }

    private handleRedisMessage(key: string, raw: string): void {
        try {
            const parsed = JSON.parse(raw) as RedisEnvelope | null;
            if (parsed && typeof parsed === "object" && parsed._instanceId === this.instanceId) {
                return;
            }
            const message =
                parsed && typeof parsed === "object" && typeof parsed.message === "string"
                    ? parsed.message
                    : raw;
            this.deliverToLocalSubscribers(key, message);
        } catch {
            this.deliverToLocalSubscribers(key, raw);
        }
    }

    private deliverToLocalSubscribers(key: string, message: string): void {
        const subscribers = this.topicSubscribers.get(key);
        if (!subscribers || !this.deliverFn) return;
        for (const id of subscribers) {
            try {
                this.deliverFn(id, message);
            } catch (err) {
                this.logger.error(`Delivery to ${id} failed:`, err);
            }
        }
    }
}
