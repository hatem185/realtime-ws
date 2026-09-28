import { clearTimeout, setTimeout } from "node:timers";
import type { RedisLike } from "../types.ts";

/** Longest delay setTimeout supports; larger values would fire after 1 ms. */
const MAX_TIMER_MS = 2 ** 31 - 1;

/**
 * Unref'd timer calling `fn` after `ms`; returns undefined (no timer) when `ms` isn't finite,
 * so `Infinity` means "no timeout".
 */
export function startTimer(fn: () => void, ms: number): NodeJS.Timeout | undefined {
    if (!Number.isFinite(ms)) return undefined;
    const timer = setTimeout(fn, Math.min(Math.max(ms, 0), MAX_TIMER_MS));
    timer.unref?.();
    return timer;
}

/** Resolves once `promise` settles or `ms` elapse, whichever comes first. Never rejects. */
export function settleWithin(promise: Promise<unknown>, ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
        const timer = startTimer(resolve, ms);
        const done = () => {
            clearTimeout(timer);
            resolve();
        };
        promise.then(done, done);
    });
}

/**
 * Whether commands can be issued on a Redis client. `wait` is ioredis' `lazyConnect` state (the
 * first command connects it); clients without a `status` field are assumed usable.
 */
export function redisUsable(client: RedisLike): boolean {
    const status = client.status;
    return status === undefined || status === "ready" || status === "wait";
}

export const errorMessage = (err: unknown): string =>
    err instanceof Error ? err.message : String(err);
