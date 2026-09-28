import type { Logger, RedisLike } from "../types";

type Handler = (message: string) => void;

/**
 * Cross-instance Redis Pub/Sub bridge.
 *
 * - Uses an existing ioredis client provided by the host. The client must
 *   support `.duplicate()` because subscriber-mode connections cannot be
 *   used for normal commands.
 * - All operations are guarded so a Redis outage degrades the hub to
 *   single-instance mode rather than crashing it.
 */
export class RedisPubSub {
    private readonly publisher: RedisLike | null;
    private readonly subscriber: RedisLike | null;
    private readonly subscriptions = new Map<string, Set<Handler>>();
    private readonly prefix: string;
    private readonly logger: Logger;
    private connected = false;

    constructor(redis: RedisLike | null, prefix: string, logger: Logger) {
        this.prefix = prefix.endsWith(":") ? prefix : `${prefix}:`;
        this.logger = logger;

        if (!redis) {
            this.publisher = null;
            this.subscriber = null;
            return;
        }

        this.publisher = redis;
        this.subscriber = redis.duplicate();

        this.subscriber.on("error", (err: Error) =>
            this.logger.error("Subscriber error:", err.message),
        );
        this.subscriber.on("message", (channel: string, message: string) => {
            const handlers = this.subscriptions.get(channel);
            if (!handlers) return;
            for (const handler of handlers) {
                try {
                    handler(message);
                } catch (err) {
                    this.logger.error("Subscriber handler threw:", err);
                }
            }
        });

        this.connected = true;
    }

    isAvailable(): boolean {
        if (!this.publisher || !this.subscriber || !this.connected) return false;
        return this.publisher.status === "ready" && this.subscriber.status === "ready";
    }

    /** Returns the publisher (used by SessionManager for SET/EXPIRE/etc). */
    getPublisher(): RedisLike | null {
        return this.publisher;
    }

    private fullChannel(name: string): string {
        return `${this.prefix}${name}`;
    }

    async publish(channelTopic: string, message: string): Promise<void> {
        if (!this.publisher || !this.isAvailable()) return;
        try {
            await this.publisher.publish(this.fullChannel(channelTopic), message);
        } catch (err) {
            this.logger.error("Publish error:", err);
        }
    }

    async subscribe(channelTopic: string, handler: Handler): Promise<void> {
        if (!this.subscriber) return;

        const fullName = this.fullChannel(channelTopic);
        const handlers = this.subscriptions.get(fullName);
        if (handlers) {
            handlers.add(handler);
            return;
        }

        this.subscriptions.set(fullName, new Set([handler]));
        try {
            await this.subscriber.subscribe(fullName);
        } catch (err) {
            this.subscriptions.delete(fullName);
            this.logger.error("Subscribe error:", err);
        }
    }

    async unsubscribe(channelTopic: string, handler: Handler): Promise<void> {
        if (!this.subscriber) return;

        const fullName = this.fullChannel(channelTopic);
        const handlers = this.subscriptions.get(fullName);
        if (!handlers) return;

        handlers.delete(handler);
        if (handlers.size > 0) return;

        this.subscriptions.delete(fullName);
        try {
            await this.subscriber.unsubscribe(fullName);
        } catch (err) {
            this.logger.error("Unsubscribe error:", err);
        }
    }

    async shutdown(): Promise<void> {
        this.connected = false;
        if (this.subscriber) {
            try {
                this.subscriber.removeAllListeners();
                await this.subscriber.quit();
            } catch {
                // ignore
            }
        }
        // Publisher is owned by the host; never quit it here.
        this.subscriptions.clear();
    }
}
