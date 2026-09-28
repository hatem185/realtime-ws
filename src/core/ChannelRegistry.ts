import { randomUUID } from "node:crypto";
import type { AuthIdentity, IChannel, Logger } from "../types.ts";
import { settleWithin } from "../utils/async.ts";
import type { RedisPubSub } from "./RedisPubSub.ts";

type DeliveryFn = (connectionId: string, message: string) => void;
type GetUserConnsFn = (userId: string) => Set<string>;

interface RedisEnvelope {
    _instanceId?: string;
    message?: string;
    userId?: string;
}

/**
 * Pub/Sub channel carrying {@link ChannelRegistry.publishToUser} traffic between instances.
 * Can't collide with a topic key: `#` is always escaped in the channel part of those.
 */
const USER_CHANNEL = "#user";

/** Longest a subscribe waits for Redis to confirm the SUBSCRIBE before acknowledging anyway. */
const REDIS_SUBSCRIBE_WAIT_MS = 2000;

const ESCAPE: Record<string, string> = { "%": "%25", ":": "%3A", "#": "%23" };
const UNESCAPE: Record<string, string> = { "25": "%", "3A": ":", "23": "#" };

/**
 * Key for a channel + topic pair. `%`, `:` and `#` are escaped in the channel name, so the
 * first `:` always separates channel from topic, whatever either contains.
 */
export function topicKey(channel: string, topic: string): string {
    return `${channel.replace(/[%:#]/g, (c) => ESCAPE[c])}:${topic}`;
}

/** Inverse of {@link topicKey}. */
export function parseTopicKey(key: string): { channel: string; topic: string } {
    const i = key.indexOf(":");
    return {
        channel: key.slice(0, i).replace(/%(25|3A|23)/g, (_, hex: string) => UNESCAPE[hex]),
        topic: key.slice(i + 1),
    };
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
    ) {
        void this.pubsub.subscribe(USER_CHANNEL, (raw) => this.handleUserMessage(raw));
    }

    /** Wired by {@link ConnectionManager} during hub bootstrap. */
    setDeliveryHandler(fn: DeliveryFn): void {
        this.deliverFn = fn;
    }

    /** Wired by {@link ConnectionManager} during hub bootstrap. */
    setUserConnectionsResolver(fn: GetUserConnsFn): void {
        this.getUserConnsFn = fn;
    }

    register(channel: IChannel): void {
        if (typeof channel.name !== "string" || !channel.name) {
            throw new Error("Channel name must be a non-empty string");
        }
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

    /** Authorize, then subscribe (see {@link addSubscription}). Throws if `authorize()` throws. */
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

        await this.addSubscription(connectionId, identity, channelName, topic);
        return true;
    }

    /**
     * Record an already-authorized subscription: add the local subscriber, subscribe in Redis
     * for the topic's first local subscriber, run `onSubscribe`, then wait (bounded) for Redis
     * to confirm, so cross-instance messages flow once this resolves.
     */
    async addSubscription(
        connectionId: string,
        identity: AuthIdentity,
        channelName: string,
        topic: string,
    ): Promise<void> {
        const redisReady = this.addSubscriber(topicKey(channelName, topic), connectionId);

        const channel = this.channels.get(channelName);
        if (channel?.onSubscribe) {
            try {
                await channel.onSubscribe(connectionId, identity, topic);
            } catch (err) {
                this.logger.error(`onSubscribe error for ${channelName}:`, err);
            }
        }
        await settleWithin(redisReady, REDIS_SUBSCRIBE_WAIT_MS);
    }

    /** Remove a subscription. `onUnsubscribe` runs only if the connection was subscribed. */
    async unsubscribe(
        connectionId: string,
        identity: AuthIdentity,
        channelName: string,
        topic: string,
    ): Promise<void> {
        if (!this.removeSubscriber(topicKey(channelName, topic), connectionId)) return;

        const channel = this.channels.get(channelName);
        if (channel?.onUnsubscribe) {
            try {
                await channel.onUnsubscribe(connectionId, identity, topic);
            } catch (err) {
                this.logger.error(`onUnsubscribe error for ${channelName}:`, err);
            }
        }
    }

    /** Run the channel's `onMessage` and publish its broadcast. Errors are logged. */
    async handleMessage(
        connectionId: string,
        identity: AuthIdentity,
        channelName: string,
        topic: string,
        payload: unknown,
    ): Promise<void> {
        await this.runMessage(connectionId, identity, channelName, topic, payload);
    }

    /** Like {@link handleMessage}, resolving `false` if `onMessage` threw. */
    async runMessage(
        connectionId: string,
        identity: AuthIdentity,
        channelName: string,
        topic: string,
        payload: unknown,
    ): Promise<boolean> {
        const channel = this.channels.get(channelName);
        if (!channel?.onMessage) return true;

        try {
            const result = await channel.onMessage(connectionId, identity, topic, payload);
            if (result && typeof result === "object" && result.broadcast !== undefined) {
                await this.publish(channelName, topic, result.broadcast);
            }
            return true;
        } catch (err) {
            this.logger.error(`onMessage error for ${channelName}:`, err);
            return false;
        }
    }

    async handleDisconnect(
        connectionId: string,
        identity: AuthIdentity,
        subscribedKeys: Set<string>,
    ): Promise<void> {
        this.removeConnection(connectionId, subscribedKeys);
        await this.notifyDisconnect(connectionId, identity, subscribedKeys);
    }

    /** Synchronously drop a connection from each of the given topic keys. */
    removeConnection(connectionId: string, keys: Iterable<string>): void {
        for (const key of keys) this.removeSubscriber(key, connectionId);
    }

    /** Run `onDisconnect` once per channel among the given topic keys. */
    async notifyDisconnect(
        connectionId: string,
        identity: AuthIdentity,
        keys: Iterable<string>,
    ): Promise<void> {
        const names = new Set<string>();
        for (const key of keys) names.add(parseTopicKey(key).channel);

        for (const name of names) {
            const channel = this.channels.get(name);
            if (!channel?.onDisconnect) continue;
            try {
                await channel.onDisconnect(connectionId, identity);
            } catch (err) {
                this.logger.error(`onDisconnect error for ${name}:`, err);
            }
        }
    }

    /**
     * Publish a payload to every subscriber of `channel:topic`
     * (local + remote via Redis).
     */
    async publish(channelName: string, topic: string, payload: unknown): Promise<void> {
        const key = topicKey(channelName, topic);
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
     * Publish to every connection of a specific user, on every instance, and to nobody else.
     * The topic only labels the delivered event; it isn't used for routing.
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

        this.deliverToUser(userId, message);

        if (this.pubsub.isAvailable()) {
            const envelope = JSON.stringify({ _instanceId: this.instanceId, userId, message });
            await this.pubsub.publish(USER_CHANNEL, envelope);
        }
    }

    getSubscriberCount(channelName: string, topic: string): number {
        return this.topicSubscribers.get(topicKey(channelName, topic))?.size ?? 0;
    }

    /** Number of topics with at least one local subscriber. */
    get topicCount(): number {
        return this.topicSubscribers.size;
    }

    /**
     * Returns the in-flight Redis SUBSCRIBE for the key, if any. Bookkeeping is synchronous, so
     * racing subscribes and unsubscribes can't strand or double up a Redis handler.
     */
    private addSubscriber(key: string, connectionId: string): Promise<void> {
        let subscribers = this.topicSubscribers.get(key);
        if (!subscribers) {
            subscribers = new Set();
            this.topicSubscribers.set(key, subscribers);
        }
        subscribers.add(connectionId);

        if (!this.pubsub.enabled) return Promise.resolve();
        if (this.redisHandlers.has(key)) return this.pubsub.whenSubscribed(key);

        const handler = (raw: string) => this.handleRedisMessage(key, raw);
        this.redisHandlers.set(key, handler);
        return this.pubsub.subscribe(key, handler);
    }

    /** Returns whether the connection was subscribed. */
    private removeSubscriber(key: string, connectionId: string): boolean {
        const subscribers = this.topicSubscribers.get(key);
        if (!subscribers?.delete(connectionId)) return false;

        if (subscribers.size === 0) {
            this.topicSubscribers.delete(key);
            const handler = this.redisHandlers.get(key);
            if (handler) {
                this.redisHandlers.delete(key);
                void this.pubsub.unsubscribe(key, handler);
            }
        }
        return true;
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

    private handleUserMessage(raw: string): void {
        let envelope: RedisEnvelope | null;
        try {
            envelope = JSON.parse(raw) as RedisEnvelope | null;
        } catch {
            this.logger.warn("Dropped malformed user message from Redis.");
            return;
        }
        if (
            !envelope ||
            envelope._instanceId === this.instanceId ||
            typeof envelope.userId !== "string" ||
            typeof envelope.message !== "string"
        ) {
            return;
        }
        this.deliverToUser(envelope.userId, envelope.message);
    }

    private deliverToUser(userId: string, message: string): void {
        if (!this.deliverFn || !this.getUserConnsFn) return;
        for (const id of this.getUserConnsFn(userId)) {
            try {
                this.deliverFn(id, message);
            } catch (err) {
                this.logger.error(`Delivery to ${id} failed:`, err);
            }
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
