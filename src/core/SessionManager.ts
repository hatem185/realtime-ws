import { randomUUID } from "crypto";
import type { Logger, RedisLike } from "../types";

const LAST_ACTIVE_TTL_SECONDS = 90 * 24 * 60 * 60;

interface SessionData {
    connectionId: string;
    userId: string;
    instanceId: string;
    connectedAt: number;
    subscribedChannels: string[];
}

/**
 * Dual-layer presence/session manager.
 *
 * - **In-memory** (always active): O(1) local lookups.
 * - **Redis** (optional): when supplied, replicates session/online keys
 *   so cross-instance "is X online?" queries work. Falls back silently
 *   to memory-only on any Redis failure — connections never break.
 */
export class SessionManager {
    public readonly instanceId = randomUUID();

    private readonly localSessions = new Map<string, SessionData>();
    private readonly userSessions = new Map<string, Set<string>>();

    constructor(
        private readonly redis: RedisLike | null,
        private readonly prefix: string,
        private readonly ttlSeconds: number,
        private readonly logger: Logger,
    ) {}

    private redisAvailable(): boolean {
        return !!this.redis && this.redis.status === "ready";
    }

    private sessionKey(userId: string, connectionId: string): string {
        return `${this.prefix}session:${userId}:${connectionId}`;
    }

    private onlineKey(userId: string): string {
        return `${this.prefix}online:${userId}`;
    }

    private lastActiveKey(userId: string): string {
        return `${this.prefix}lastActive:${userId}`;
    }

    async register(connectionId: string, userId: string): Promise<void> {
        const session: SessionData = {
            connectionId,
            userId,
            instanceId: this.instanceId,
            connectedAt: Date.now(),
            subscribedChannels: [],
        };

        this.localSessions.set(connectionId, session);
        const set = this.userSessions.get(userId) ?? new Set<string>();
        set.add(connectionId);
        this.userSessions.set(userId, set);

        if (!this.redis || !this.redisAvailable()) return;
        try {
            const r = this.redis;
            await r.setex(
                this.sessionKey(userId, connectionId),
                this.ttlSeconds,
                JSON.stringify(session),
            );
            await r.sadd(this.onlineKey(userId), connectionId);
            await r.expire(this.onlineKey(userId), this.ttlSeconds);
            await r.setex(
                this.lastActiveKey(userId),
                LAST_ACTIVE_TTL_SECONDS,
                Date.now().toString(),
            );
        } catch (err) {
            this.logger.error("Redis register error:", err);
        }
    }

    async unregister(connectionId: string): Promise<void> {
        const session = this.localSessions.get(connectionId);
        if (!session) return;

        this.localSessions.delete(connectionId);
        const set = this.userSessions.get(session.userId);
        if (set) {
            set.delete(connectionId);
            if (set.size === 0) this.userSessions.delete(session.userId);
        }

        if (!this.redis || !this.redisAvailable()) return;
        try {
            const r = this.redis;
            await r.del(this.sessionKey(session.userId, connectionId));
            await r.srem(this.onlineKey(session.userId), connectionId);
            const remaining = await r.scard(this.onlineKey(session.userId));
            if (remaining === 0) await r.del(this.onlineKey(session.userId));
            await r.setex(
                this.lastActiveKey(session.userId),
                LAST_ACTIVE_TTL_SECONDS,
                Date.now().toString(),
            );
        } catch (err) {
            this.logger.error("Redis unregister error:", err);
        }
    }

    async refreshTTL(connectionId: string): Promise<void> {
        const session = this.localSessions.get(connectionId);
        if (!session || !this.redis || !this.redisAvailable()) return;
        try {
            const r = this.redis;
            await r.expire(this.sessionKey(session.userId, connectionId), this.ttlSeconds);
            await r.expire(this.onlineKey(session.userId), this.ttlSeconds);
            await r.setex(
                this.lastActiveKey(session.userId),
                LAST_ACTIVE_TTL_SECONDS,
                Date.now().toString(),
            );
        } catch {
            // non-critical
        }
    }

    async updateSubscriptions(connectionId: string, channels: string[]): Promise<void> {
        const session = this.localSessions.get(connectionId);
        if (!session) return;
        session.subscribedChannels = channels;

        if (!this.redis || !this.redisAvailable()) return;
        try {
            await this.redis.setex(
                this.sessionKey(session.userId, connectionId),
                this.ttlSeconds,
                JSON.stringify(session),
            );
        } catch {
            // non-critical
        }
    }

    getLocalSession(connectionId: string): SessionData | undefined {
        return this.localSessions.get(connectionId);
    }

    getLocalConnectionsForUser(userId: string): Set<string> {
        return this.userSessions.get(userId) ?? new Set();
    }

    isOnlineLocally(userId: string): boolean {
        const set = this.userSessions.get(userId);
        return !!set && set.size > 0;
    }

    /** O(1) presence check: memory first, then Redis. */
    async isOnline(userId: string): Promise<boolean> {
        if (this.isOnlineLocally(userId)) return true;
        if (!this.redis || !this.redisAvailable()) return false;
        try {
            const exists = await this.redis.exists(this.onlineKey(userId));
            return exists > 0;
        } catch {
            return false;
        }
    }

    async areUsersOnline(userIds: string[]): Promise<Record<string, boolean>> {
        const unique = [...new Set(userIds)];
        const result: Record<string, boolean> = {};

        const needRedis: string[] = [];
        for (const uid of unique) {
            if (this.isOnlineLocally(uid)) result[uid] = true;
            else needRedis.push(uid);
        }

        if (needRedis.length === 0 || !this.redis || !this.redisAvailable()) {
            for (const uid of needRedis) result[uid] = false;
            return result;
        }

        try {
            const pipeline = this.redis.multi();
            for (const uid of needRedis) pipeline.exists(this.onlineKey(uid));
            const replies = await pipeline.exec();
            for (let i = 0; i < needRedis.length; i++) {
                const reply = replies?.[i];
                const value = Array.isArray(reply) ? reply[1] : 0;
                result[needRedis[i]] = typeof value === "number" ? value > 0 : false;
            }
        } catch {
            for (const uid of needRedis) result[uid] = false;
        }

        return result;
    }

    async getLastActiveBulk(userIds: string[]): Promise<Record<string, number | null>> {
        const unique = [...new Set(userIds)];
        const result: Record<string, number | null> = {};

        if (!this.redis || !this.redisAvailable()) {
            for (const uid of unique) result[uid] = null;
            return result;
        }

        try {
            const pipeline = this.redis.multi();
            for (const uid of unique) pipeline.get(this.lastActiveKey(uid));
            const replies = await pipeline.exec();
            for (let i = 0; i < unique.length; i++) {
                const reply = replies?.[i];
                const value = Array.isArray(reply) ? (reply[1] as string | null) : null;
                if (value != null) {
                    const ts = parseInt(value, 10);
                    result[unique[i]] = Number.isNaN(ts) ? null : ts;
                } else {
                    result[unique[i]] = null;
                }
            }
        } catch {
            for (const uid of unique) result[uid] = null;
        }

        return result;
    }

    get localConnectionCount(): number {
        return this.localSessions.size;
    }
}
