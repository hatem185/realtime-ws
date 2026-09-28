import type { WebSocket } from "ws";

/**
 * Authenticated identity attached to a WebSocket connection.
 * `userId` is the only required field — everything else is application-defined.
 */
export interface AuthIdentity {
    userId: string;
    [key: string]: unknown;
}

/**
 * A channel implements the per-feature contract for real-time topics
 * (chat, notifications, task updates, etc.).
 *
 * Topics inside a channel are arbitrary strings (e.g. `space:123`,
 * `user:abc`). The channel decides who is allowed in and what to do
 * with inbound messages.
 */
export interface IChannel {
    /** Unique channel identifier (e.g. "chat", "notifications"). */
    readonly name: string;

    /** Decide whether `identity` is allowed to subscribe to `topic`. */
    authorize(identity: AuthIdentity, topic: string): Promise<boolean> | boolean;

    /** Called after a successful subscription. */
    onSubscribe?(
        connectionId: string,
        identity: AuthIdentity,
        topic: string,
    ): Promise<void> | void;

    /** Called after a client unsubscribes from a topic. */
    onUnsubscribe?(
        connectionId: string,
        identity: AuthIdentity,
        topic: string,
    ): Promise<void> | void;

    /**
     * Called when a client sends a message on this channel.
     * Return `{ broadcast: payload }` to fan-out the payload to every
     * subscriber of `topic` (local + remote via Redis pub/sub).
     */
    onMessage?(
        connectionId: string,
        identity: AuthIdentity,
        topic: string,
        payload: unknown,
    ): Promise<{ broadcast?: unknown } | void> | { broadcast?: unknown } | void;

    /** Called once per channel when the connection drops. */
    onDisconnect?(
        connectionId: string,
        identity: AuthIdentity,
    ): Promise<void> | void;
}

/**
 * Inbound client message envelope.
 *
 * ```json
 * { "action": "subscribe", "channel": "chat", "topic": "space:123" }
 * { "action": "message",   "channel": "chat", "topic": "space:123", "payload": { ... } }
 * { "action": "ping" }
 * ```
 */
export interface ClientMessage {
    action: "ping" | "pong" | "subscribe" | "unsubscribe" | "message" | string;
    channel?: string;
    topic?: string;
    payload?: unknown;
}

/** Outbound server event envelope. */
export interface ServerEvent {
    event: string;
    channel?: string;
    topic?: string;
    payload?: unknown;
    [key: string]: unknown;
}

/**
 * Pluggable token authenticator. The host application implements this
 * against its own JWT/session library and hands an instance to the hub.
 */
export interface TokenAuthenticator {
    /**
     * Verify a raw token string and return the authenticated identity.
     * Throw or reject to deny the upgrade.
     */
    verify(token: string): Promise<AuthIdentity> | AuthIdentity;
}

/** Minimal logger contract — `console` satisfies it out of the box. */
export interface Logger {
    debug?(message: string, ...meta: unknown[]): void;
    info(message: string, ...meta: unknown[]): void;
    warn(message: string, ...meta: unknown[]): void;
    error(message: string, ...meta: unknown[]): void;
}

/** Subset of ioredis' API that the plugin actually uses. */
export interface RedisLike {
    status?: string;
    publish(channel: string, message: string): Promise<number> | number;
    subscribe(...channels: string[]): Promise<unknown> | unknown;
    unsubscribe(...channels: string[]): Promise<unknown> | unknown;
    /** ioredis `lazyConnect` support: used to connect the hub's own subscriber connection. */
    connect?(): Promise<unknown> | unknown;
    /** Used to drop the hub's own subscriber connection if QUIT goes unanswered at shutdown. */
    disconnect?(): unknown;
    on(event: string, listener: (...args: any[]) => void): unknown;
    removeAllListeners(): unknown;
    quit(): Promise<unknown> | unknown;
    duplicate(): RedisLike;
    setex(key: string, ttl: number, value: string): Promise<unknown> | unknown;
    sadd(key: string, ...values: string[]): Promise<unknown> | unknown;
    srem(key: string, ...values: string[]): Promise<unknown> | unknown;
    scard(key: string): Promise<number> | number;
    expire(key: string, ttl: number): Promise<unknown> | unknown;
    exists(key: string): Promise<number> | number;
    del(...keys: string[]): Promise<unknown> | unknown;
    get(key: string): Promise<string | null> | string | null;
    multi(): RedisPipelineLike;
}

export interface RedisPipelineLike {
    exists(key: string): RedisPipelineLike;
    get(key: string): RedisPipelineLike;
    exec(): Promise<Array<[Error | null, unknown]> | null>;
}

/** Configuration object passed to {@link createRealtimeHub}. */
export interface RealtimeHubOptions {
    /** Required: token verifier used during the WebSocket upgrade. */
    authenticator: TokenAuthenticator;

    /**
     * Optional ioredis-compatible client used for cross-instance pub/sub
     * and presence. When omitted, the hub runs in single-instance mode
     * (in-memory only) — perfectly fine for dev and small deployments.
     */
    redis?: RedisLike;

    /** Prefix for all Redis keys / pub-sub channels. Defaults to `realtime`. */
    redisPrefix?: string;

    /** Heartbeat (server-initiated ping) interval. Default: 30s. */
    heartbeatMs?: number;

    /** Max concurrent connections per user. Oldest gets evicted. Default: 10. */
    maxConnectionsPerUser?: number;

    /** Hard cap on inbound message size in bytes. Default: 64 KB. */
    maxMessageSizeBytes?: number;

    /**
     * Max topics one connection may be subscribed to at once; further subscribes get
     * `SUBSCRIBE_FAILED`. Bounds memory (local and in Redis) on permissive channels. Default: 1000.
     */
    maxSubscriptionsPerConnection?: number;

    /** Longest accepted topic, in characters; longer subscribes get `SUBSCRIBE_FAILED`. Default: 256. */
    maxTopicLength?: number;

    /**
     * Longest one message may take to handle (authorize(), channel hooks, Redis confirmation).
     * A connection stuck on a message for longer is closed with 1013 at the next heartbeat, so a
     * hook that never settles can't wedge it forever. Default: 30000.
     */
    requestTimeoutMs?: number;

    /** Per-connection rate limit window in ms. Default: 1000. */
    rateLimitWindowMs?: number;

    /** Max inbound messages per window. Default: 20. */
    rateLimitMaxMessages?: number;

    /**
     * Drop outbound writes when the socket's send buffer exceeds this many
     * bytes (backpressure protection). Default: 128 KB.
     */
    backpressureThresholdBytes?: number;

    /**
     * Session TTL in seconds (Redis layer only). Refreshed by the heartbeat, so keep it
     * comfortably above `heartbeatMs` (at least 2x). Default: 120.
     */
    sessionTtlSeconds?: number;

    /**
     * Upper bound for {@link RealtimeHub.shutdown}: how long to wait for close handshakes
     * and cleanup before terminating what's left. Keep it below your orchestrator's
     * termination grace period. Default: 10000.
     */
    shutdownTimeoutMs?: number;

    /** Optional logger. Defaults to a thin `console` wrapper. */
    logger?: Logger;
}

/** Options added after 1.0; optional in {@link ResolvedHubOptions} so hand-built options still compile. */
type LaterOptions =
    "shutdownTimeoutMs" | "maxSubscriptionsPerConnection" | "maxTopicLength" | "requestTimeoutMs";

export interface ResolvedHubOptions
    extends
        Required<Omit<RealtimeHubOptions, "redis" | "logger" | LaterOptions>>,
        Pick<RealtimeHubOptions, LaterOptions> {
    redis: RedisLike | null;
    logger: Logger;
}

/** Internal record kept for every active connection on this instance. */
export interface ConnectionInfo {
    connectionId: string;
    identity: AuthIdentity;
    socket: WebSocket;
    /** `${channel}:${topic}` keys this connection has subscribed to. */
    channels: Set<string>;
    connectedAt: number;
    lastPongAt: number;
    missedPongs: number;
    messageCount: number;
    messageWindowStart: number;
}
