import type { WebSocket } from "ws";
import type {
    AuthIdentity,
    IChannel,
    Logger,
    RealtimeHubOptions,
    ResolvedHubOptions,
} from "../types";
import { defaultLogger } from "../utils/logger";
import { ChannelRegistry } from "./ChannelRegistry";
import { ConnectionManager, LIMIT_DEFAULTS } from "./ConnectionManager";
import { RedisPubSub } from "./RedisPubSub";
import { SessionManager } from "./SessionManager";

/**
 * The single object the host application interacts with.
 *
 * Lifecycle:
 *  1. `const hub = createRealtimeHub({ ... })`
 *  2. `hub.registerChannel(new MyChannel())`
 *  3. Attach to an HTTP server (Express adapter or your own).
 *  4. From REST controllers / services: `hub.publish(...)`,
 *     `hub.publishToUser(...)`, `hub.isOnline(...)`.
 *  5. On shutdown: `await hub.shutdown()`.
 */
export class RealtimeHub {
    public readonly options: ResolvedHubOptions;
    public readonly logger: Logger;
    public readonly pubsub: RedisPubSub;
    public readonly sessions: SessionManager;
    public readonly registry: ChannelRegistry;
    public readonly connections: ConnectionManager;
    private readonly shutdownHooks = new Set<() => void>();
    private shutdownPromise: Promise<void> | null = null;

    constructor(opts: RealtimeHubOptions) {
        this.logger = opts.logger ?? defaultLogger;

        this.options = {
            authenticator: opts.authenticator,
            redis: opts.redis ?? null,
            redisPrefix: opts.redisPrefix ?? "realtime",
            heartbeatMs: opts.heartbeatMs ?? 30_000,
            maxConnectionsPerUser: opts.maxConnectionsPerUser ?? 10,
            maxMessageSizeBytes: opts.maxMessageSizeBytes ?? 64 * 1024,
            maxSubscriptionsPerConnection:
                opts.maxSubscriptionsPerConnection ?? LIMIT_DEFAULTS.maxSubscriptionsPerConnection,
            maxTopicLength: opts.maxTopicLength ?? LIMIT_DEFAULTS.maxTopicLength,
            requestTimeoutMs: opts.requestTimeoutMs ?? LIMIT_DEFAULTS.requestTimeoutMs,
            rateLimitWindowMs: opts.rateLimitWindowMs ?? 1000,
            rateLimitMaxMessages: opts.rateLimitMaxMessages ?? 20,
            backpressureThresholdBytes: opts.backpressureThresholdBytes ?? 128 * 1024,
            sessionTtlSeconds: opts.sessionTtlSeconds ?? 120,
            shutdownTimeoutMs: opts.shutdownTimeoutMs ?? LIMIT_DEFAULTS.shutdownTimeoutMs,
            logger: this.logger,
        };

        if (
            this.options.redis &&
            this.options.sessionTtlSeconds * 1000 < 2 * this.options.heartbeatMs
        ) {
            this.logger.warn(
                `sessionTtlSeconds (${this.options.sessionTtlSeconds}s) is less than 2x heartbeatMs ` +
                    `(${this.options.heartbeatMs}ms); presence may expire between heartbeat refreshes.`,
            );
        }

        const prefix = this.options.redisPrefix.endsWith(":")
            ? this.options.redisPrefix
            : `${this.options.redisPrefix}:`;

        this.pubsub = new RedisPubSub(this.options.redis, prefix, this.logger);
        this.sessions = new SessionManager(
            this.options.redis,
            prefix,
            this.options.sessionTtlSeconds,
            this.logger,
        );
        this.registry = new ChannelRegistry(this.pubsub, this.logger);
        this.connections = new ConnectionManager(this.options, this.registry, this.sessions);

        this.connections.init();
    }

    /** Register a channel implementation. Idempotent (replace-on-duplicate). */
    registerChannel(channel: IChannel): this {
        this.registry.register(channel);
        return this;
    }

    /** Register many channels at once. */
    registerChannels(channels: IChannel[]): this {
        channels.forEach((c) => this.registry.register(c));
        return this;
    }

    /**
     * Accept an already-upgraded WebSocket. Used by the Express adapter
     * or by any custom HTTP-server integration.
     */
    async acceptSocket(socket: WebSocket, identity: AuthIdentity): Promise<string> {
        return this.connections.register(socket, identity);
    }

    /** Broadcast a payload to all subscribers of `channel:topic`. */
    async publish(channelName: string, topic: string, payload: unknown): Promise<void> {
        await this.registry.publish(channelName, topic, payload);
    }

    /** Push a payload to every connection of a specific user. */
    async publishToUser(
        userId: string,
        channelName: string,
        payload: unknown,
        topic?: string,
    ): Promise<void> {
        await this.registry.publishToUser(userId, channelName, payload, topic);
    }

    /** Cross-instance presence check. */
    async isOnline(userId: string): Promise<boolean> {
        return this.sessions.isOnline(userId);
    }

    /** Bulk presence check (single Redis pipeline). */
    async areUsersOnline(userIds: string[]): Promise<Record<string, boolean>> {
        return this.sessions.areUsersOnline(userIds);
    }

    /** Bulk last-active timestamps in ms (Redis-only). */
    async getLastActiveBulk(userIds: string[]): Promise<Record<string, number | null>> {
        return this.sessions.getLastActiveBulk(userIds);
    }

    /** Quick stats — useful for `/health` endpoints. */
    stats(): {
        localConnections: number;
        channels: string[];
        instanceId: string;
        redisAvailable: boolean;
        droppedMessages: number;
    } {
        return {
            localConnections: this.connections.connectionCount,
            channels: this.registry.getChannelNames(),
            instanceId: this.sessions.instanceId,
            redisAvailable: this.pubsub.isAvailable(),
            droppedMessages: this.connections.droppedMessages,
        };
    }

    /** True once {@link shutdown} has been called. */
    get isShuttingDown(): boolean {
        return this.shutdownPromise !== null;
    }

    /**
     * Run `hook` synchronously when shutdown starts. Adapters use it to stop accepting new
     * connections (e.g. {@link attachWsServer} detaches its upgrade listener). Returns an
     * unregister function.
     */
    onShutdown(hook: () => void): () => void {
        this.shutdownHooks.add(hook);
        return () => this.shutdownHooks.delete(hook);
    }

    /**
     * Graceful shutdown — call from your SIGTERM handler. Stops accepting connections, closes
     * every connection with 1001 (going away), removes presence from Redis and quits the
     * subscriber connection. Bounded by `shutdownTimeoutMs`, plus at most 1 s to quit the
     * subscriber; repeated calls share one run.
     */
    shutdown(): Promise<void> {
        if (!this.shutdownPromise) {
            // Set before any hook runs, so hooks already see isShuttingDown and a nested
            // shutdown() call gets this same promise.
            this.shutdownPromise = Promise.resolve().then(async () => {
                for (const hook of this.shutdownHooks) {
                    try {
                        hook();
                    } catch (err) {
                        this.logger.error("Shutdown hook error:", err);
                    }
                }
                await this.connections.shutdown();
                await this.pubsub.shutdown();
                this.logger.info("Hub shut down.");
            });
        }
        return this.shutdownPromise;
    }
}

/** Convenience factory — keeps construction concise at the call site. */
export function createRealtimeHub(opts: RealtimeHubOptions): RealtimeHub {
    return new RealtimeHub(opts);
}
