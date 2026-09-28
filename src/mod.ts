/**
 * realtime-ws entry for Deno and other hosts without Express: everything in
 * `index.ts` except the Express health router. Deno imports this file directly
 * (see "Deno / epic-api plugin" in `README.md`).
 */

// Public types & contracts
export type {
    AuthIdentity,
    ClientMessage,
    ConnectionInfo,
    IChannel,
    Logger,
    RealtimeHubOptions,
    RedisLike,
    RedisPipelineLike,
    ResolvedHubOptions,
    ServerEvent,
    TokenAuthenticator,
} from "./types.ts";

// Core (framework-agnostic)
export { RealtimeHub, createRealtimeHub } from "./core/RealtimeHub.ts";
export { ConnectionManager, CloseCodes } from "./core/ConnectionManager.ts";
export { ChannelRegistry } from "./core/ChannelRegistry.ts";
export { SessionManager } from "./core/SessionManager.ts";
export { RedisPubSub } from "./core/RedisPubSub.ts";

// Channel helpers
export { BaseChannel } from "./channels/BaseChannel.ts";
export { BroadcastChannel } from "./channels/BroadcastChannel.ts";

// Auth helpers
export { createFunctionAuthenticator } from "./auth/TokenAuthenticator.ts";

// Logger
export { defaultLogger } from "./utils/logger.ts";

// HTTP upgrade adapter (any node:http server)
export { attachWsServer, type AttachOptions } from "./express/attachWsServer.ts";
