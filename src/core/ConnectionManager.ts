import { randomUUID } from "crypto";
import type { RawData, WebSocket } from "ws";
import type {
    AuthIdentity,
    ClientMessage,
    ConnectionInfo,
    Logger,
    ResolvedHubOptions,
} from "../types";
import { settleWithin } from "../utils/async";
import { type ChannelRegistry, topicKey } from "./ChannelRegistry";
import type { SessionManager } from "./SessionManager";

const MISSED_PONG_LIMIT = 2;
/** Longest `connected` waits for the user's presence to reach Redis. */
const PRESENCE_WAIT_MS = 2000;
/** Connections handled per event-loop turn by the heartbeat. */
const HEARTBEAT_SLICE = 250;
/** Connections cleaned up at once during shutdown. */
const SHUTDOWN_CONCURRENCY = 100;
const PING_EVENT = JSON.stringify({ event: "ping" });
const PONG_EVENT = JSON.stringify({ event: "pong" });
/** Stands in for a frame that wasn't valid JSON. */
const INVALID_JSON = Symbol("invalid JSON");

/** Defaults for options added after 1.0 (also applied when options are built by hand). */
export const LIMIT_DEFAULTS = {
    shutdownTimeoutMs: 10_000,
    maxSubscriptionsPerConnection: 1000,
    maxTopicLength: 256,
    requestTimeoutMs: 30_000,
};

/** WebSocket close codes used by the hub. */
export const CloseCodes = {
    /** Closed by the server via `unregister()`. */
    NORMAL: 1000,
    /** The server is shutting down; reconnect (to another instance). */
    GOING_AWAY: 1001,
    /** Frame larger than `maxMessageSizeBytes` (sent by `ws`). */
    MESSAGE_TOO_BIG: 1009,
    /** A message took longer than `requestTimeoutMs` to handle; reconnect with backoff. */
    TRY_AGAIN_LATER: 1013,
    /** Replaced by a newer connection of the same user (`maxConnectionsPerUser`). Don't auto-reconnect. */
    EVICTED: 4001,
} as const;

/** Internal per-connection state on top of the public {@link ConnectionInfo}. */
interface Connection extends ConnectionInfo {
    closed: boolean;
    /** Tail of this connection's message queue: its messages are handled one at a time, in order. */
    queue: Promise<void>;
    /** Messages accepted into the queue and not yet handled. */
    queued: number;
    /** When the message being handled started (0 when idle). */
    busySince: number;
    /** Sent something other than a ping since the last heartbeat (drives `lastActive`). */
    active: boolean;
}

/**
 * Owns every WebSocket connection that lives on this server instance.
 *
 * Responsibilities:
 *  - Welcome handshake & registration.
 *  - Heartbeat (ping/pong with miss tracking) and presence refresh.
 *  - Per-connection rate limiting, message-size cap and in-order processing.
 *  - Per-user max-connection cap (oldest evicted).
 *  - Backpressure-aware send (drops writes when buffer is too full).
 *  - Inbound message parsing → dispatching to the {@link ChannelRegistry}.
 */
export class ConnectionManager {
    private readonly connections = new Map<string, Connection>();
    private readonly userConnections = new Map<string, Set<string>>();
    private heartbeatTimer: NodeJS.Timeout | null = null;
    private heartbeatRunning = false;
    /** Heartbeat presence refreshes that Redis hasn't answered yet. */
    private refreshesInFlight = 0;
    private readonly logger: Logger;
    private shuttingDown = false;
    private dropped = 0;
    private readonly limits: typeof LIMIT_DEFAULTS;

    constructor(
        private readonly opts: ResolvedHubOptions,
        private readonly registry: ChannelRegistry,
        private readonly sessions: SessionManager,
    ) {
        this.logger = opts.logger;
        this.limits = {
            shutdownTimeoutMs: opts.shutdownTimeoutMs ?? LIMIT_DEFAULTS.shutdownTimeoutMs,
            maxSubscriptionsPerConnection:
                opts.maxSubscriptionsPerConnection ?? LIMIT_DEFAULTS.maxSubscriptionsPerConnection,
            maxTopicLength: opts.maxTopicLength ?? LIMIT_DEFAULTS.maxTopicLength,
            requestTimeoutMs: opts.requestTimeoutMs ?? LIMIT_DEFAULTS.requestTimeoutMs,
        };
    }

    /** Wire delivery handler back into the registry & start the heartbeat. */
    init(): void {
        this.registry.setDeliveryHandler((id, msg) => this.sendToConnection(id, msg));
        this.registry.setUserConnectionsResolver((uid) => this.getConnectionsForUser(uid));
        this.startHeartbeat();
        this.logger.info("ConnectionManager initialized.");
    }

    /**
     * Register an open socket. The cap check, bookkeeping and every socket listener happen
     * synchronously, so no frame, error or close can arrive before the connection is ready for it.
     * `connected` is sent once the user's presence is in Redis (bounded wait), so a client that has
     * seen it is online on every instance; messages arriving meanwhile wait in the queue behind it.
     */
    async register(socket: WebSocket, identity: AuthIdentity): Promise<string> {
        if (this.shuttingDown) {
            // Refuse without registering. The listener keeps a malformed frame from crashing
            // the process while the socket closes.
            socket.on("error", () => {});
            closeSocket(socket, CloseCodes.GOING_AWAY, "Server shutting down");
            return randomUUID();
        }

        // Numeric ids (e.g. from an untyped JWT payload) are used as strings, like everywhere else.
        const rawUserId: unknown = identity.userId;
        if (typeof rawUserId === "number") identity = { ...identity, userId: String(rawUserId) };
        const userId = identity.userId;

        // Free a slot before taking one, in the same tick, so concurrent registrations can't
        // all pass the check and exceed the cap. unregister() detaches synchronously.
        const existing = this.userConnections.get(userId);
        while (existing && existing.size > 0 && existing.size >= this.opts.maxConnectionsPerUser) {
            const oldest = this.findOldest(existing);
            if (!oldest) break;
            this.unregister(oldest, CloseCodes.EVICTED, "Evicted: too many connections").catch(
                (err) => this.logger.error("Eviction error:", err),
            );
        }

        const connectionId = randomUUID();
        const now = Date.now();
        const info: Connection = {
            connectionId,
            identity,
            socket,
            channels: new Set(),
            connectedAt: now,
            lastPongAt: now,
            missedPongs: 0,
            messageCount: 0,
            messageWindowStart: now,
            closed: false,
            queue: Promise.resolve(),
            queued: 0,
            busySince: 0,
            active: false,
        };

        this.connections.set(connectionId, info);
        const set = this.userConnections.get(userId) ?? new Set<string>();
        set.add(connectionId);
        this.userConnections.set(userId, set);

        socket.on("message", (raw) => this.onFrame(info, raw));
        socket.on("pong", () => {
            info.missedPongs = 0;
            info.lastPongAt = Date.now();
        });
        socket.on("ping", () => {
            // ws has already queued its automatic pong, without looking at the send buffer: a
            // client that stops reading and keeps pinging would make us buffer pongs without bound.
            if (socket.bufferedAmount > this.opts.backpressureThresholdBytes) {
                this.terminate(connectionId);
            }
        });
        socket.on("close", () => {
            this.unregister(connectionId).catch((err) =>
                this.logger.error("Unregister on close error:", err),
            );
        });
        socket.on("error", (err) => {
            this.logger.warn(`Socket error for ${connectionId}: ${err.message}`);
            // close handler will run after this
        });

        if (socket.readyState !== socket.OPEN) {
            // Closed before we got it: no 'close' event is coming.
            await this.unregister(connectionId);
            return connectionId;
        }

        const presence = this.sessions
            .register(connectionId, userId)
            .catch((err) => this.logger.error("Session register error:", err));
        info.queue = settleWithin(presence, PRESENCE_WAIT_MS).then(() =>
            this.sendRaw(info, JSON.stringify({ event: "connected", connectionId })),
        );

        this.logger.info(
            `Connection ${connectionId} registered for user ${userId}. Total: ${this.connections.size}`,
        );
        return connectionId;
    }

    /**
     * Close and clean up a connection. Idempotent: the connection leaves every map synchronously,
     * so a repeated or concurrent call is a no-op. Presence is removed before the channels'
     * `onDisconnect` hooks run, so they already see the user's updated presence.
     */
    async unregister(
        connectionId: string,
        code: number = CloseCodes.NORMAL,
        reason = "Connection closed by server",
    ): Promise<void> {
        const info = this.detach(connectionId);
        if (!info) return;
        closeSocket(info.socket, code, reason);
        await this.cleanup(info);
    }

    /** Send a raw, serialized message to a specific local connection. */
    sendToConnection(connectionId: string, message: string): void {
        const info = this.connections.get(connectionId);
        if (info) this.sendRaw(info, message);
    }

    getConnectionsForUser(userId: string): Set<string> {
        return this.userConnections.get(userId) ?? new Set();
    }

    get connectionCount(): number {
        return this.connections.size;
    }

    /** Outbound messages dropped because a client wasn't reading (backpressure). */
    get droppedMessages(): number {
        return this.dropped;
    }

    /**
     * Close every connection with 1001 and clean them up. Waits at most `timeoutMs` for
     * close handshakes and cleanup; sockets still open after that are terminated.
     */
    async shutdown(timeoutMs: number = this.limits.shutdownTimeoutMs): Promise<void> {
        this.shuttingDown = true;
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        const infos: Connection[] = [];
        for (const id of [...this.connections.keys()]) {
            const info = this.detach(id);
            if (!info) continue;
            closeSocket(info.socket, CloseCodes.GOING_AWAY, "Server shutting down");
            infos.push(info);
        }

        // Everyone's presence goes at once, so users who don't reconnect elsewhere go offline even
        // when slow onDisconnect hooks keep the cleanup below from reaching them in time.
        const presence = infos.map((info) => this.removePresence(info));
        let next = 0;
        const worker = async () => {
            while (next < infos.length) {
                const i = next++;
                await this.cleanup(infos[i], presence[i]);
            }
        };
        const cleanups = Array.from(
            { length: Math.min(SHUTDOWN_CONCURRENCY, infos.length) },
            worker,
        );
        await settleWithin(
            Promise.all([...cleanups, ...infos.map((i) => socketClosed(i.socket))]),
            timeoutMs,
        );

        for (const info of infos) {
            if (info.socket.readyState !== info.socket.CLOSED) info.socket.terminate();
        }
        this.logger.info(`ConnectionManager shut down (${infos.length} connection(s)).`);
    }

    // ── private ──────────────────────────────────────────────────────

    /** Synchronously remove a connection from every map. Returns it, or undefined if already gone. */
    private detach(connectionId: string): Connection | undefined {
        const info = this.connections.get(connectionId);
        if (!info) return undefined;

        info.closed = true;
        this.connections.delete(connectionId);
        const set = this.userConnections.get(info.identity.userId);
        if (set) {
            set.delete(connectionId);
            if (set.size === 0) this.userConnections.delete(info.identity.userId);
        }
        this.registry.removeConnection(connectionId, info.channels);
        return info;
    }

    private async cleanup(info: Connection, presence = this.removePresence(info)): Promise<void> {
        // Bounded: local presence is already gone, and anything a hook then sends on the same
        // Redis client queues behind these commands anyway.
        await settleWithin(presence, PRESENCE_WAIT_MS);
        await this.registry.notifyDisconnect(info.connectionId, info.identity, info.channels);

        this.logger.info(
            `Connection ${info.connectionId} unregistered. Total: ${this.connections.size}`,
        );
    }

    private removePresence(info: Connection): Promise<void> {
        return this.sessions
            .unregister(info.connectionId)
            .catch((err) => this.logger.error("Session unregister error:", err));
    }

    /** Every outbound write goes through here. */
    private sendRaw(info: Connection, data: string): void {
        const socket = info.socket;
        if (socket.readyState !== socket.OPEN) return;

        // Slow consumer: drop rather than buffer without bound. A client that stops reading
        // also stops answering pings, so the heartbeat closes it.
        if (socket.bufferedAmount > this.opts.backpressureThresholdBytes) {
            this.dropped++;
            return;
        }

        try {
            socket.send(data);
        } catch (err) {
            this.logger.error(`Send error to ${info.connectionId}:`, err);
        }
    }

    private sendError(
        info: Connection,
        code: string,
        message: string,
        target?: { channel: string; topic: string },
    ): void {
        this.sendRaw(info, JSON.stringify({ event: "error", code, message, ...target }));
    }

    /**
     * Cheap checks run on arrival, before anything else; accepted messages join the connection's
     * queue, which holds at most `rateLimitMaxMessages`. Pings are answered at once, so liveness
     * checks aren't held up by slow handlers.
     */
    private onFrame(info: Connection, raw: RawData): void {
        if (info.closed) return;

        const now = Date.now();
        const data = toBuffer(raw);
        if (data.length > this.opts.maxMessageSizeBytes) {
            this.sendError(
                info,
                "MESSAGE_TOO_LARGE",
                `Message exceeds maximum size of ${this.opts.maxMessageSizeBytes} bytes`,
            );
            return;
        }

        if (now - info.messageWindowStart > this.opts.rateLimitWindowMs) {
            info.messageCount = 0;
            info.messageWindowStart = now;
        }
        info.messageCount++;
        if (info.messageCount > this.opts.rateLimitMaxMessages) {
            // A pong still counts: a busy client mustn't be dropped as dead because of its own
            // traffic. Only tiny object frames are parsed here, so a flood stays cheap to refuse.
            if (data.length <= 64 && data[0] === 0x7b && isAction(parseJson(data), "pong")) {
                info.missedPongs = 0;
                info.lastPongAt = now;
            } else {
                this.sendError(info, "RATE_LIMITED", "Too many messages, slow down");
            }
            return;
        }

        const parsed = parseJson(data);
        if (isAction(parsed, "ping")) {
            this.sendRaw(info, PONG_EVENT);
            return;
        }
        if (isAction(parsed, "pong")) {
            // An app-level pong answers our {"event":"ping"}, so like a ws pong it proves the client
            // reads. Other traffic doesn't: a client that stops reading can keep sending.
            info.missedPongs = 0;
            info.lastPongAt = now;
            return;
        }
        info.active = true;

        if (info.queued >= this.opts.rateLimitMaxMessages) {
            this.sendError(info, "RATE_LIMITED", "Too many messages in flight, slow down");
            return;
        }

        info.queued++;
        info.queue = info.queue
            .then(() => {
                info.busySince = Date.now();
                return this.handleMessage(info, parsed);
            })
            .catch((err) => this.logger.error("Message handling error:", err))
            .then(() => {
                info.busySince = 0;
                info.queued--;
            });
    }

    private async handleMessage(info: Connection, parsed: unknown): Promise<void> {
        if (info.closed) return;

        if (parsed === INVALID_JSON) {
            this.sendError(info, "INVALID_JSON", "Message must be valid JSON");
            return;
        }
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
            this.sendError(info, "INVALID_MESSAGE", "Message must be a JSON object");
            return;
        }
        const message = parsed as ClientMessage;

        if (typeof message.action !== "string" || !message.action) {
            this.sendError(info, "MISSING_ACTION", "Message must include an 'action' field");
            return;
        }

        switch (message.action) {
            case "pong":
                return;
            case "subscribe":
                return this.dispatchSubscribe(info, message);
            case "unsubscribe":
                return this.dispatchUnsubscribe(info, message);
            case "message":
                return this.dispatchMessage(info, message);
            default:
                this.sendError(
                    info,
                    "UNKNOWN_ACTION",
                    `Unknown action: ${message.action.slice(0, 64)}`,
                );
        }
    }

    /** `channel` and `topic` as non-empty strings, or an error reply and null. */
    private target(
        info: Connection,
        msg: ClientMessage,
        action: string,
    ): { channel: string; topic: string } | null {
        const { channel, topic } = msg;
        if (typeof channel !== "string" || !channel || typeof topic !== "string" || !topic) {
            this.sendError(info, "MISSING_FIELDS", `${action} requires 'channel' and 'topic'`);
            return null;
        }
        return { channel, topic };
    }

    private async dispatchSubscribe(info: Connection, msg: ClientMessage): Promise<void> {
        const target = this.target(info, msg, "Subscribe");
        if (!target) return;
        const { channel: channelName, topic } = target;

        if (topic.length > this.limits.maxTopicLength) {
            this.sendError(
                info,
                "SUBSCRIBE_FAILED",
                `Topic exceeds ${this.limits.maxTopicLength} characters`,
            );
            return;
        }

        const channel = this.registry.getChannel(channelName);
        const key = topicKey(channelName, topic);
        const ack = () => this.sendRaw(info, JSON.stringify({ event: "subscribed", ...target }));
        const fail = (reason = `Cannot subscribe to ${channelName}:${topic}`) =>
            this.sendError(info, "SUBSCRIBE_FAILED", reason, target);

        if (!channel) return fail();
        if (info.channels.has(key)) return ack();
        if (info.channels.size >= this.limits.maxSubscriptionsPerConnection) {
            return fail(
                `Subscription limit of ${this.limits.maxSubscriptionsPerConnection} reached`,
            );
        }

        let authorized = false;
        try {
            authorized = await channel.authorize(info.identity, topic);
        } catch (err) {
            this.logger.error(`authorize error for ${channelName}:`, err);
        }

        // Left while authorize() was pending: record nothing, run no hooks.
        if (info.closed) return;
        if (!authorized) return fail();

        // Recorded before anything else awaits, so a disconnect from here on cleans it up.
        info.channels.add(key);
        await this.registry.addSubscription(info.connectionId, info.identity, channelName, topic);
        if (info.closed) return;

        await this.sessions.updateSubscriptions(info.connectionId, [...info.channels]);
        ack();
    }

    private async dispatchUnsubscribe(info: Connection, msg: ClientMessage): Promise<void> {
        const target = this.target(info, msg, "Unsubscribe");
        if (!target) return;

        const key = topicKey(target.channel, target.topic);
        // Idempotent: unsubscribing from something you're not in is acknowledged, and runs no hooks.
        if (info.channels.delete(key)) {
            await this.registry.unsubscribe(
                info.connectionId,
                info.identity,
                target.channel,
                target.topic,
            );
            if (info.closed) return;
            await this.sessions.updateSubscriptions(info.connectionId, [...info.channels]);
        }
        this.sendRaw(info, JSON.stringify({ event: "unsubscribed", ...target }));
    }

    /** Only subscribers may post to a topic: the subscription is the proof of authorization. */
    private async dispatchMessage(info: Connection, msg: ClientMessage): Promise<void> {
        const target = this.target(info, msg, "Message");
        if (!target) return;

        if (!info.channels.has(topicKey(target.channel, target.topic))) {
            this.sendError(
                info,
                "NOT_SUBSCRIBED",
                `Subscribe to ${target.channel}:${target.topic} before sending messages`,
                target,
            );
            return;
        }

        const ok = await this.registry.runMessage(
            info.connectionId,
            info.identity,
            target.channel,
            target.topic,
            msg.payload,
        );
        if (!ok) this.sendError(info, "MESSAGE_FAILED", "Message could not be processed", target);
    }

    private startHeartbeat(): void {
        if (this.heartbeatTimer) return;
        this.heartbeatTimer = setInterval(() => {
            if (this.heartbeatRunning) return; // the previous pass hasn't finished
            this.heartbeatRunning = true;
            this.heartbeat()
                .catch((err) => this.logger.error("Heartbeat error:", err))
                .finally(() => {
                    this.heartbeatRunning = false;
                });
        }, this.opts.heartbeatMs);
        this.heartbeatTimer.unref?.();
    }

    /**
     * One pass over every connection, in slices with a yield between them: pinging thousands of
     * sockets (and queuing their presence refresh) in one go stalls the event loop for hundreds
     * of milliseconds.
     */
    private async heartbeat(): Promise<void> {
        const now = Date.now();
        // Redis hasn't answered the previous pass's refreshes (e.g. a stalled connection that
        // hasn't errored yet): don't queue another round of commands behind them.
        const refresh = this.refreshesInFlight === 0;
        const ids = [...this.connections.keys()];
        for (let i = 0; i < ids.length; i += HEARTBEAT_SLICE) {
            if (i > 0) await new Promise((resolve) => setImmediate(resolve));
            const alive: string[] = [];
            const activeUsers = new Set<string>();
            for (const id of ids.slice(i, i + HEARTBEAT_SLICE)) {
                const info = this.connections.get(id);
                if (!info) continue; // closed since the pass started
                if (info.socket.readyState !== info.socket.OPEN) {
                    this.terminate(id);
                    continue;
                }
                if (info.missedPongs >= MISSED_PONG_LIMIT) {
                    this.logger.info(
                        `Connection ${id} missed ${MISSED_PONG_LIMIT} pongs, closing.`,
                    );
                    this.terminate(id);
                    continue;
                }
                if (info.busySince && now - info.busySince > this.limits.requestTimeoutMs) {
                    // A handler that never settles would otherwise wedge the connection for good.
                    this.logger.warn(
                        `Connection ${id}: a message has been processing for over ${this.limits.requestTimeoutMs} ms, closing.`,
                    );
                    this.unregister(id, CloseCodes.TRY_AGAIN_LATER, "Request timed out").catch(
                        (err) => this.logger.error("Unregister error:", err),
                    );
                    continue;
                }
                // Answered a ping since the last tick: keep its presence alive.
                if (info.missedPongs === 0) {
                    alive.push(id);
                    if (info.active) activeUsers.add(info.identity.userId);
                }
                info.active = false;
                info.missedPongs++;
                try {
                    // Native ws ping frame — counterpart auto-replies with pong
                    info.socket.ping();
                } catch {
                    // ignore
                }
                // Also send an app-level ping for clients that don't speak ws ping frames
                this.sendRaw(info, PING_EVENT);
            }
            if (refresh && alive.length > 0) {
                this.refreshesInFlight++;
                this.sessions
                    .refresh(alive, activeUsers)
                    .catch((err) => this.logger.error("Presence refresh error:", err))
                    .finally(() => this.refreshesInFlight--);
            }
        }
    }

    /** Drop an unresponsive connection without waiting for a close handshake. */
    private terminate(connectionId: string): void {
        const info = this.detach(connectionId);
        if (!info) return;
        info.socket.terminate();
        this.cleanup(info).catch((err) => this.logger.error("Cleanup error:", err));
    }

    private findOldest(ids: Set<string>): string | undefined {
        let oldestId: string | undefined;
        let oldestAt = Infinity;
        for (const id of ids) {
            const info = this.connections.get(id);
            if (info && info.connectedAt < oldestAt) {
                oldestAt = info.connectedAt;
                oldestId = id;
            }
        }
        return oldestId;
    }
}

function parseJson(data: Buffer): unknown {
    try {
        return JSON.parse(data.toString("utf8"));
    } catch {
        return INVALID_JSON;
    }
}

function isAction(message: unknown, action: string): boolean {
    return (
        !!message &&
        typeof message === "object" &&
        (message as { action?: unknown }).action === action
    );
}

function toBuffer(raw: RawData): Buffer {
    if (Buffer.isBuffer(raw)) return raw;
    if (Array.isArray(raw)) return Buffer.concat(raw);
    return Buffer.from(raw);
}

function closeSocket(socket: WebSocket, code: number, reason: string): void {
    try {
        if (socket.readyState === socket.OPEN || socket.readyState === socket.CONNECTING) {
            socket.close(code, reason);
        }
    } catch {
        // already closing
    }
}

function socketClosed(socket: WebSocket): Promise<void> {
    if (socket.readyState === socket.CLOSED) return Promise.resolve();
    return new Promise((resolve) => socket.once("close", () => resolve()));
}
