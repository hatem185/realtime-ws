import type { Logger, RedisLike } from "../types";
import { errorMessage, redisUsable, settleWithin } from "../utils/async";

type Handler = (message: string) => void;

/** Max channels per SUBSCRIBE/UNSUBSCRIBE when re-applying subscriptions after a reconnect. */
const RESYNC_BATCH = 500;

/**
 * Cross-instance Redis Pub/Sub bridge.
 *
 * - Uses an existing ioredis client provided by the host. The client must
 *   support `.duplicate()` because subscriber-mode connections cannot be
 *   used for normal commands.
 * - The wanted subscriptions are kept here and re-applied every time the
 *   subscriber connection becomes ready, so subscriptions made while Redis
 *   was down, or lost to a restart/failover, come back on their own.
 * - All operations are guarded so a Redis outage degrades the hub to
 *   single-instance mode rather than crashing it.
 */
export class RedisPubSub {
    private readonly publisher: RedisLike | null;
    private readonly subscriber: RedisLike | null;
    /** Wanted subscriptions: full channel name → handlers. */
    private readonly subscriptions = new Map<string, Set<Handler>>();
    /** SUBSCRIBE commands still waiting for Redis, so later subscribers can wait for them too. */
    private readonly pending = new Map<string, Promise<void>>();
    /**
     * Channels Redis confirmed for the subscriber and hasn't confirmed dropping: the ones it may
     * still deliver, and that the client may re-subscribe after a reconnect. Bounded by what was
     * actually subscribed, so an outage can't grow it.
     */
    private readonly confirmed = new Set<string>();
    /** Unwanted channels that delivered a message and are being unsubscribed. */
    private readonly draining = new Set<string>();
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
        this.subscriber.on("message", (channel: string, message: string) =>
            this.dispatch(channel, message),
        );
        this.subscriber.on("ready", () => this.resync());

        this.connected = true;

        // A lazyConnect duplicate sits in "wait" until something connects it; we own it, so connect it.
        const sub = this.subscriber;
        if (sub.status === "wait" && sub.connect) {
            Promise.resolve()
                .then(() => sub.connect?.())
                .catch((err) => this.logger.warn("Subscriber connect failed:", errorMessage(err)));
        }
    }

    /** Whether Redis pub/sub is configured at all (regardless of connection state). */
    get enabled(): boolean {
        return !!this.subscriber && this.connected;
    }

    isAvailable(): boolean {
        if (!this.publisher || !this.connected) return false;
        return redisUsable(this.publisher) && this.subscriberReady();
    }

    private subscriberReady(): boolean {
        const status = this.subscriber?.status;
        return !!this.subscriber && (status === undefined || status === "ready");
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

    /**
     * Add a handler for a channel. The first handler issues SUBSCRIBE. The returned promise
     * settles once Redis has confirmed the subscription, or right away when the subscriber
     * isn't connected (the subscription is then applied on the next `ready`). Never rejects.
     */
    subscribe(channelTopic: string, handler: Handler): Promise<void> {
        if (!this.enabled) return Promise.resolve();

        const fullName = this.fullChannel(channelTopic);
        const handlers = this.subscriptions.get(fullName);
        if (handlers) {
            handlers.add(handler);
            return this.pending.get(fullName) ?? Promise.resolve();
        }

        this.subscriptions.set(fullName, new Set([handler]));
        return this.command("subscribe", [fullName]);
    }

    /** Settles when a SUBSCRIBE in flight for this channel completes (immediately if none). */
    whenSubscribed(channelTopic: string): Promise<void> {
        return this.pending.get(this.fullChannel(channelTopic)) ?? Promise.resolve();
    }

    /** Remove a handler; the last one issues UNSUBSCRIBE. Never rejects. */
    unsubscribe(channelTopic: string, handler: Handler): Promise<void> {
        if (!this.enabled) return Promise.resolve();

        const fullName = this.fullChannel(channelTopic);
        const handlers = this.subscriptions.get(fullName);
        if (!handlers?.delete(handler) || handlers.size > 0) return Promise.resolve();

        this.subscriptions.delete(fullName);
        // Never confirmed (e.g. subscribed during an outage): nothing to undo in Redis. A SUBSCRIBE
        // still in flight is undone when its confirmation arrives.
        if (!this.confirmed.has(fullName)) return Promise.resolve();
        return this.command("unsubscribe", [fullName]);
    }

    /** Number of channels this instance wants to be subscribed to. */
    get subscriptionCount(): number {
        return this.subscriptions.size;
    }

    async shutdown(): Promise<void> {
        this.connected = false;
        this.subscriptions.clear();
        this.pending.clear();
        this.confirmed.clear();
        this.draining.clear();
        if (this.subscriber) {
            const sub = this.subscriber;
            sub.removeAllListeners();
            sub.on("error", () => {});
            await settleWithin(
                Promise.resolve().then(() => sub.quit()),
                1000,
            );
            // QUIT unanswered (e.g. Redis unreachable): don't leave the socket holding the process open.
            if (sub.status !== "end") sub.disconnect?.();
        }
        // Publisher is owned by the host; never quit it here.
    }

    /**
     * Send (UN)SUBSCRIBE when the subscriber is connected. Otherwise, or if it fails, the
     * wanted state is re-applied by {@link resync} on the next `ready`.
     */
    private command(cmd: "subscribe" | "unsubscribe", names: string[]): Promise<void> {
        const sub = this.subscriber;
        if (!sub || !this.connected || !this.subscriberReady()) return Promise.resolve();

        const done = Promise.resolve()
            .then(() => sub[cmd](...names))
            .then(
                () => {
                    if (cmd === "unsubscribe") {
                        for (const name of names) {
                            this.confirmed.delete(name);
                            this.draining.delete(name);
                        }
                        return;
                    }
                    const dropped: string[] = [];
                    for (const name of names) {
                        this.confirmed.add(name);
                        if (!this.subscriptions.has(name)) dropped.push(name); // left while in flight
                    }
                    if (dropped.length && this.connected) void this.command("unsubscribe", dropped);
                },
                (err) =>
                    this.logger.warn(
                        `Redis ${cmd} failed for ${names.length} channel(s); retried on reconnect:`,
                        errorMessage(err),
                    ),
            );

        if (cmd === "subscribe") {
            for (const name of names) this.pending.set(name, done);
            void done.then(() => {
                for (const name of names) {
                    if (this.pending.get(name) === done) this.pending.delete(name);
                }
            });
        }
        return done;
    }

    /** Re-apply wanted subscriptions after a (re)connect and drop ones nothing wants any more. */
    private resync(): void {
        if (!this.connected) return;

        const wanted = [...this.subscriptions.keys()];
        const unwanted = [...new Set([...this.confirmed, ...this.draining])].filter(
            (name) => !this.subscriptions.has(name),
        );

        for (let i = 0; i < wanted.length; i += RESYNC_BATCH) {
            void this.command("subscribe", wanted.slice(i, i + RESYNC_BATCH));
        }
        for (let i = 0; i < unwanted.length; i += RESYNC_BATCH) {
            void this.command("unsubscribe", unwanted.slice(i, i + RESYNC_BATCH));
        }

        if (wanted.length || unwanted.length) {
            this.logger.info(
                `Redis subscriber ready: restored ${wanted.length} subscription(s), dropped ${unwanted.length}.`,
            );
        }
    }

    private dispatch(channel: string, message: string): void {
        const handlers = this.subscriptions.get(channel);
        if (!handlers) {
            // Delivered on a channel nothing here wants (e.g. re-subscribed by the client after a
            // reconnect): drop it once.
            if (this.connected && !this.draining.has(channel)) {
                this.draining.add(channel);
                void this.command("unsubscribe", [channel]);
            }
            return;
        }
        for (const handler of handlers) {
            try {
                handler(message);
            } catch (err) {
                this.logger.error("Subscriber handler threw:", err);
            }
        }
    }
}
