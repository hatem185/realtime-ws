import { randomUUID } from "node:crypto";
import type { Logger, RedisLike } from "../types.ts";
import { redisUsable } from "../utils/async.ts";

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
 *   so cross-instance "is X online?" queries work. Keys are kept alive by
 *   {@link refresh} from the heartbeat. Redis failures are logged and the
 *   hub carries on memory-only — connections never break.
 *
 * Commands for one operation are issued together (pipelined on the
 * connection) instead of one round trip each; Redis runs them in order.
 */
export class SessionManager {
    public readonly instanceId = randomUUID();

    private readonly localSessions = new Map<string, SessionData>();
    private readonly userSessions = new Map<string, Set<string>>();
    /** Sessions whose JSON in Redis is out of date (changed, or the key was lost). */
    private readonly stale = new Set<string>();

    constructor(
        private readonly redis: RedisLike | null,
        private readonly prefix: string,
        private readonly ttlSeconds: number,
        private readonly logger: Logger,
    ) {}

    private redisAvailable(): boolean {
        return !!this.redis && redisUsable(this.redis);
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
        const r = this.redis;
        const online = this.onlineKey(userId);
        try {
            await Promise.all([
                r.setex(
                    this.sessionKey(userId, connectionId),
                    this.ttlSeconds,
                    JSON.stringify(session),
                ),
                r.sadd(online, connectionId),
                r.expire(online, this.ttlSeconds),
                r.setex(this.lastActiveKey(userId), LAST_ACTIVE_TTL_SECONDS, Date.now().toString()),
            ]);
        } catch (err) {
            this.logger.error("Redis register error:", err);
        }
    }

    async unregister(connectionId: string): Promise<void> {
        const session = this.localSessions.get(connectionId);
        if (!session) return;

        this.localSessions.delete(connectionId);
        this.stale.delete(connectionId);
        const set = this.userSessions.get(session.userId);
        if (set) {
            set.delete(connectionId);
            if (set.size === 0) this.userSessions.delete(session.userId);
        }

        if (!this.redis || !this.redisAvailable()) return;
        const r = this.redis;
        try {
            // No SCARD + DEL afterwards: Redis deletes the set when its last member goes, and a
            // DEL could wipe a member another connection of this user added in between.
            await Promise.all([
                r.del(this.sessionKey(session.userId, connectionId)),
                r.srem(this.onlineKey(session.userId), connectionId),
                r.setex(
                    this.lastActiveKey(session.userId),
                    LAST_ACTIVE_TTL_SECONDS,
                    Date.now().toString(),
                ),
            ]);
        } catch (err) {
            this.logger.error("Redis unregister error:", err);
        }
    }

    /**
     * Heartbeat refresh for live connections: re-add each connection to its user's online set
     * (which also restores presence after expiry or a Redis restart) and extend the TTLs. A
     * session's JSON is rewritten only when it changed or Redis lost the key; `lastActive` is
     * bumped only for `activeUsers` (every user when omitted).
     */
    async refresh(
        connectionIds: Iterable<string>,
        activeUsers?: ReadonlySet<string>,
    ): Promise<void> {
        if (!this.redis || !this.redisAvailable()) return;
        const r = this.redis;
        const now = Date.now().toString();
        const users = new Set<string>();
        const ops: Promise<unknown>[] = [];

        for (const connectionId of connectionIds) {
            const session = this.localSessions.get(connectionId);
            if (!session) continue;
            const key = this.sessionKey(session.userId, connectionId);
            if (this.stale.delete(connectionId)) {
                const write = Promise.resolve(
                    r.setex(key, this.ttlSeconds, JSON.stringify(session)),
                );
                ops.push(write.catch((err) => this.markStale(connectionId, err)));
            } else {
                const touch = Promise.resolve(r.expire(key, this.ttlSeconds));
                ops.push(touch.then((found) => found === 0 && this.markStale(connectionId)));
            }

            const online = this.onlineKey(session.userId);
            ops.push(Promise.resolve(r.sadd(online, connectionId)));
            if (!users.has(session.userId)) {
                users.add(session.userId);
                ops.push(Promise.resolve(r.expire(online, this.ttlSeconds)));
                if (!activeUsers || activeUsers.has(session.userId)) {
                    const lastActive = this.lastActiveKey(session.userId);
                    ops.push(Promise.resolve(r.setex(lastActive, LAST_ACTIVE_TTL_SECONDS, now)));
                }
            }
        }

        try {
            await Promise.all(ops);
        } catch (err) {
            this.logger.error("Redis presence refresh error:", err);
        }
    }

    private markStale(connectionId: string, err?: unknown): void {
        if (this.localSessions.has(connectionId)) this.stale.add(connectionId);
        if (err) throw err;
    }

    /** Refresh one connection's presence keys and its user's `lastActive` (see {@link refresh}). */
    async refreshTTL(connectionId: string): Promise<void> {
        await this.refresh([connectionId]);
    }

    /** Local only; the session key in Redis picks the new list up on the next {@link refresh}. */
    async updateSubscriptions(connectionId: string, channels: string[]): Promise<void> {
        const session = this.localSessions.get(connectionId);
        if (!session) return;
        session.subscribedChannels = channels;
        this.stale.add(connectionId);
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
