import { randomUUID } from "crypto";
import type { RawData, WebSocket } from "ws";
import type {
    AuthIdentity,
    ClientMessage,
    ConnectionInfo,
    Logger,
    ResolvedHubOptions,
} from "../types";
import type { ChannelRegistry } from "./ChannelRegistry";
import type { SessionManager } from "./SessionManager";

const MISSED_PONG_LIMIT = 2;

/**
 * Owns every WebSocket connection that lives on this server instance.
 *
 * Responsibilities:
 *  - Welcome handshake & registration.
 *  - Heartbeat (ping/pong with miss tracking).
 *  - Per-connection rate limiting and message-size cap.
 *  - Per-user max-connection cap (oldest evicted).
 *  - Backpressure-aware send (drops writes when buffer is too full).
 *  - Inbound message parsing → dispatching to the {@link ChannelRegistry}.
 */
export class ConnectionManager {
    private readonly connections = new Map<string, ConnectionInfo>();
    private readonly userConnections = new Map<string, Set<string>>();
    private heartbeatTimer: NodeJS.Timeout | null = null;
    private readonly logger: Logger;

    constructor(
        private readonly opts: ResolvedHubOptions,
        private readonly registry: ChannelRegistry,
        private readonly sessions: SessionManager,
    ) {
        this.logger = opts.logger;
    }

    /** Wire delivery handler back into the registry & start the heartbeat. */
    init(): void {
        this.registry.setDeliveryHandler((id, msg) => this.sendToConnection(id, msg));
        this.registry.setUserConnectionsResolver((uid) => this.getConnectionsForUser(uid));
        this.startHeartbeat();
        this.logger.info("ConnectionManager initialized.");
    }

    async register(socket: WebSocket, identity: AuthIdentity): Promise<string> {
        const userId = identity.userId;

        const existing = this.userConnections.get(userId);
        if (existing && existing.size >= this.opts.maxConnectionsPerUser) {
            const oldest = this.findOldest(existing);
            if (oldest) await this.unregister(oldest);
        }

        const connectionId = randomUUID();
        const now = Date.now();
        const info: ConnectionInfo = {
            connectionId,
            identity,
            socket,
            channels: new Set(),
            connectedAt: now,
            lastPongAt: now,
            missedPongs: 0,
            messageCount: 0,
            messageWindowStart: now,
        };

        this.connections.set(connectionId, info);
        const set = this.userConnections.get(userId) ?? new Set<string>();
        set.add(connectionId);
        this.userConnections.set(userId, set);

        await this.sessions.register(connectionId, userId);

        socket.on("message", (raw) => {
            this.handleMessage(connectionId, raw).catch((err) =>
                this.logger.error(`Message handling error:`, err),
            );
        });

        socket.on("pong", () => {
            const c = this.connections.get(connectionId);
            if (!c) return;
            c.missedPongs = 0;
            c.lastPongAt = Date.now();
        });

        socket.on("close", () => {
            this.unregister(connectionId).catch((err) =>
                this.logger.error("Unregister on close error:", err),
            );
        });

        socket.on("error", (err) => {
            this.logger.warn(`Socket error for ${connectionId}:`, err.message);
            // close handler will run after this
        });

        this.send(socket, { event: "connected", connectionId });

        this.logger.info(
            `Connection ${connectionId} registered for user ${userId}. Total: ${this.connections.size}`,
        );
        return connectionId;
    }

    async unregister(connectionId: string): Promise<void> {
        const info = this.connections.get(connectionId);
        if (!info) return;

        await this.registry.handleDisconnect(connectionId, info.identity, info.channels);

        try {
            if (info.socket.readyState === info.socket.OPEN || info.socket.readyState === info.socket.CONNECTING) {
                info.socket.close(1000, "Connection closed by server");
            }
        } catch {
            // already closing
        }

        this.connections.delete(connectionId);
        const set = this.userConnections.get(info.identity.userId);
        if (set) {
            set.delete(connectionId);
            if (set.size === 0) this.userConnections.delete(info.identity.userId);
        }

        await this.sessions.unregister(connectionId);

        this.logger.info(
            `Connection ${connectionId} unregistered. Total: ${this.connections.size}`,
        );
    }

    /** Send a raw, serialized message to a specific local connection. */
    sendToConnection(connectionId: string, message: string): void {
        const info = this.connections.get(connectionId);
        if (!info || info.socket.readyState !== info.socket.OPEN) return;

        if (info.socket.bufferedAmount > this.opts.backpressureThresholdBytes) {
            // Drop write — slow consumer
            return;
        }

        try {
            info.socket.send(message);
        } catch (err) {
            this.logger.error(`Send error to ${connectionId}:`, err);
        }
    }

    getConnectionsForUser(userId: string): Set<string> {
        return this.userConnections.get(userId) ?? new Set();
    }

    get connectionCount(): number {
        return this.connections.size;
    }

    async shutdown(): Promise<void> {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
        const ids = [...this.connections.keys()];
        for (const id of ids) await this.unregister(id);
        this.logger.info("ConnectionManager shut down.");
    }

    // ── private ──────────────────────────────────────────────────────

    private send(socket: WebSocket, data: Record<string, unknown>): void {
        if (socket.readyState !== socket.OPEN) return;
        try {
            socket.send(JSON.stringify(data));
        } catch {
            // ignore
        }
    }

    private async handleMessage(connectionId: string, raw: RawData): Promise<void> {
        const info = this.connections.get(connectionId);
        if (!info) return;

        // Any inbound traffic proves liveness
        info.missedPongs = 0;
        info.lastPongAt = Date.now();
        this.sessions.refreshTTL(connectionId).catch(() => {});

        // Coerce ws's RawData to a string
        const text = typeof raw === "string" ? raw : raw.toString("utf8");

        if (Buffer.byteLength(text, "utf8") > this.opts.maxMessageSizeBytes) {
            this.send(info.socket, {
                event: "error",
                code: "MESSAGE_TOO_LARGE",
                message: `Message exceeds maximum size of ${this.opts.maxMessageSizeBytes} bytes`,
            });
            return;
        }

        const now = Date.now();
        if (now - info.messageWindowStart > this.opts.rateLimitWindowMs) {
            info.messageCount = 0;
            info.messageWindowStart = now;
        }
        info.messageCount++;
        if (info.messageCount > this.opts.rateLimitMaxMessages) {
            this.send(info.socket, {
                event: "error",
                code: "RATE_LIMITED",
                message: "Too many messages, slow down",
            });
            return;
        }

        let message: ClientMessage;
        try {
            message = JSON.parse(text);
        } catch {
            this.send(info.socket, {
                event: "error",
                code: "INVALID_JSON",
                message: "Message must be valid JSON",
            });
            return;
        }

        if (!message.action) {
            this.send(info.socket, {
                event: "error",
                code: "MISSING_ACTION",
                message: "Message must include an 'action' field",
            });
            return;
        }

        switch (message.action) {
            case "ping":
                this.send(info.socket, { event: "pong" });
                return;
            case "pong":
                return;
            case "subscribe":
                return this.dispatchSubscribe(info, message);
            case "unsubscribe":
                return this.dispatchUnsubscribe(info, message);
            case "message":
                return this.dispatchMessage(info, message);
            default:
                this.send(info.socket, {
                    event: "error",
                    code: "UNKNOWN_ACTION",
                    message: `Unknown action: ${message.action}`,
                });
        }
    }

    private async dispatchSubscribe(info: ConnectionInfo, msg: ClientMessage): Promise<void> {
        if (!msg.channel || !msg.topic) {
            this.send(info.socket, {
                event: "error",
                code: "MISSING_FIELDS",
                message: "Subscribe requires 'channel' and 'topic'",
            });
            return;
        }

        const ok = await this.registry.subscribe(
            info.connectionId,
            info.identity,
            msg.channel,
            msg.topic,
        );

        if (!ok) {
            this.send(info.socket, {
                event: "error",
                code: "SUBSCRIBE_FAILED",
                message: `Cannot subscribe to ${msg.channel}:${msg.topic}`,
            });
            return;
        }

        const key = `${msg.channel}:${msg.topic}`;
        info.channels.add(key);
        await this.sessions.updateSubscriptions(info.connectionId, [...info.channels]);
        this.send(info.socket, {
            event: "subscribed",
            channel: msg.channel,
            topic: msg.topic,
        });
    }

    private async dispatchUnsubscribe(info: ConnectionInfo, msg: ClientMessage): Promise<void> {
        if (!msg.channel || !msg.topic) {
            this.send(info.socket, {
                event: "error",
                code: "MISSING_FIELDS",
                message: "Unsubscribe requires 'channel' and 'topic'",
            });
            return;
        }

        const key = `${msg.channel}:${msg.topic}`;
        info.channels.delete(key);
        await this.registry.unsubscribe(
            info.connectionId,
            info.identity,
            msg.channel,
            msg.topic,
        );
        await this.sessions.updateSubscriptions(info.connectionId, [...info.channels]);
        this.send(info.socket, {
            event: "unsubscribed",
            channel: msg.channel,
            topic: msg.topic,
        });
    }

    private async dispatchMessage(info: ConnectionInfo, msg: ClientMessage): Promise<void> {
        if (!msg.channel || !msg.topic) {
            this.send(info.socket, {
                event: "error",
                code: "MISSING_FIELDS",
                message: "Message requires 'channel' and 'topic'",
            });
            return;
        }
        await this.registry.handleMessage(
            info.connectionId,
            info.identity,
            msg.channel,
            msg.topic,
            msg.payload,
        );
    }

    private startHeartbeat(): void {
        if (this.heartbeatTimer) return;
        this.heartbeatTimer = setInterval(() => {
            for (const [id, info] of this.connections) {
                if (info.socket.readyState !== info.socket.OPEN) {
                    this.unregister(id).catch(() => {});
                    continue;
                }
                if (info.missedPongs >= MISSED_PONG_LIMIT) {
                    this.logger.info(
                        `Connection ${id} missed ${MISSED_PONG_LIMIT} pongs, closing.`,
                    );
                    this.unregister(id).catch(() => {});
                    continue;
                }
                info.missedPongs++;
                try {
                    // Native ws ping frame — counterpart auto-replies with pong
                    info.socket.ping();
                } catch {
                    // ignore
                }
                // Also send an app-level ping for clients that don't speak ws ping frames
                this.send(info.socket, { event: "ping" });
            }
        }, this.opts.heartbeatMs);
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
