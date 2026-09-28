/**
 * realtime-ws — framework-agnostic WebSocket plugin.
 *
 * See `README.md` for an architecture overview and integration guide.
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
} from "./types";

// Core (framework-agnostic)
export { RealtimeHub, createRealtimeHub } from "./core/RealtimeHub";
export { ConnectionManager } from "./core/ConnectionManager";
export { ChannelRegistry } from "./core/ChannelRegistry";
export { SessionManager } from "./core/SessionManager";
export { RedisPubSub } from "./core/RedisPubSub";

// Channel helpers
export { BaseChannel } from "./channels/BaseChannel";
export { BroadcastChannel } from "./channels/BroadcastChannel";

// Auth helpers
export { createFunctionAuthenticator } from "./auth/TokenAuthenticator";

// Logger
export { defaultLogger } from "./utils/logger";

// Express adapter (optional)
export { attachWsServer, type AttachOptions } from "./express/attachWsServer";
export { createWsHealthRouter } from "./express/createWsHealthRouter";
