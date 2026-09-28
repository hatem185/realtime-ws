import type { AuthIdentity } from "../types";
import { BaseChannel } from "./BaseChannel";

/**
 * Server-push-only channel. Authorizes everyone for every topic and
 * silently ignores any inbound client message. Use for purely
 * server-originated streams (notifications, system events, etc.).
 *
 * @example
 * ```ts
 * hub.registerChannel(new BroadcastChannel("system"));
 * await hub.publish("system", "global", { kind: "maintenance", at: ... });
 * ```
 */
export class BroadcastChannel extends BaseChannel {
    constructor(public readonly name: string) {
        super();
    }

    authorize(_identity: AuthIdentity, _topic: string): boolean {
        return true;
    }
}
