# realtime-ws

A reusable, framework-agnostic WebSocket plugin for Node.js APIs. Designed to drop into any backend that follows the architecture in [`docs/custom-api.md`](../../docs/custom-api.md) (Express + TypeScript + Mongoose + Redis), with zero coupling to a specific framework, ORM, or auth library.

This package is **internal** — it is not published to npm. Consume it as a local workspace dependency.

---

## 1. Why a plugin?

The host API only needs four things from a real-time layer:

1. A pluggable **token verifier** (e.g. wrapping the host's `JWTManager`).
2. An optional **Redis client** for cross-instance fan-out + presence.
3. A way to register **channels** (chat, notifications, task updates, …).
4. A simple `publish(channel, topic, payload)` method to call from any controller / service.

Everything else — connection lifecycle, heartbeats, rate limiting, backpressure, presence tracking, multi-instance pub/sub — is the plugin's job.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          RealtimeHub                            │  ← single facade
├─────────────────────────────────────────────────────────────────┤
│  ConnectionManager   ChannelRegistry   SessionManager           │
│  • per-conn rate     • topic routing   • presence (mem+redis)   │
│  • heartbeat         • pub/sub         • last-active            │
│  • backpressure      • lifecycle hooks                          │
├─────────────────────────────────────────────────────────────────┤
│                        RedisPubSub (optional)                   │
└─────────────────────────────────────────────────────────────────┘
        ▲                                  ▲
        │                                  │
   Express adapter                Channel implementations
   (attachWsServer +              (BaseChannel / IChannel)
    createWsHealthRouter)
        ▲
        │
   http.Server (from app.listen)
```

### Separation of concerns

| Concern                          | Owner               | Notes                                                 |
| -------------------------------- | ------------------- | ----------------------------------------------------- |
| Connection lifecycle             | `ConnectionManager` | register/unregister, heartbeat, eviction, dispatch    |
| Topic subscriptions & routing    | `ChannelRegistry`   | one place where a "topic" becomes a real fan-out      |
| Per-feature behaviour            | `IChannel` impl     | authorize / onSubscribe / onMessage / onDisconnect    |
| Presence (single & cross-instance)| `SessionManager`   | dual layer: memory always; Redis when configured      |
| Cross-instance fan-out           | `RedisPubSub`       | de-dups self-published messages via `_instanceId`     |
| Token verification               | `TokenAuthenticator`| host-supplied; the plugin never reads `process.env`   |
| Logging                          | `Logger`            | `console` by default; bring your own (pino, winston…) |
| HTTP upgrade & health routes     | Express adapter     | thin, opt-in; the core works without Express          |

### Wire protocol

Every client message is a JSON object:

```jsonc
{ "action": "subscribe",   "channel": "chat", "topic": "space:abc" }
{ "action": "unsubscribe", "channel": "chat", "topic": "space:abc" }
{ "action": "message",     "channel": "chat", "topic": "space:abc", "payload": { ... } }
{ "action": "ping" }
```

Server events:

```jsonc
{ "event": "connected",    "connectionId": "uuid" }
{ "event": "subscribed",   "channel": "chat", "topic": "space:abc" }
{ "event": "unsubscribed", "channel": "chat", "topic": "space:abc" }
{ "event": "message",      "channel": "chat", "topic": "space:abc", "payload": { ... } }
{ "event": "pong" }
{ "event": "error",        "code": "RATE_LIMITED", "message": "..." }
```

### Built-in safety

- **Heartbeat** (default 30 s) — closes connections after 2 missed pongs.
- **Per-user cap** (default 10) — oldest connection evicted on overflow.
- **Message-size cap** (default 64 KB) — oversize messages get a `MESSAGE_TOO_LARGE` event.
- **Per-connection rate limit** (default 20 msg / 1 s) — overflow gets `RATE_LIMITED`.
- **Backpressure** (default 128 KB buffered) — drops writes to slow consumers instead of OOM-ing.

All limits are configurable via `RealtimeHubOptions`.

---

## 3. Installation (in a custom-api host project)

Add a workspace / file dependency. Example with npm workspaces:

```jsonc
// host package.json
{
  "dependencies": {
    "realtime-ws": "file:../realtime-ws",
    "ws": "^8.16.0",
    "ioredis": "^5.3.0"
  }
}
```

Then `npm install` and you can `import { ... } from "realtime-ws"`.

> The package ships TypeScript sources, so the host's `tsc` will pick them up directly. If you prefer a pre-built artefact, run `npm run build` inside `packages/realtime-ws` first.

---

## 4. Integrating with custom-api

The full reference snippets are in [`examples/integration.example.ts`](./examples/integration.example.ts). The summary:

### 4.1 Wrap the hub as a Service

Following §7 of `docs/custom-api.md`, every shared resource is a `Service` registered in `ServiceProvider.init()`.

```ts
// src/app/services/RealtimeService.ts
import { Service } from "@core/Service";
import { ServiceProvider } from "@core/ServiceProvider";
import { createRealtimeHub, createFunctionAuthenticator, type RealtimeHub } from "realtime-ws";
import { JWTManager } from "@app/utils/JWTManager";
import { RedisClient } from "@app/config/RedisClient";

class RealtimeService extends Service {
    public static get instance(): RealtimeService {
        return ServiceProvider.instance.get(RealtimeService.name) as RealtimeService;
    }

    public readonly hub: RealtimeHub;

    constructor() {
        super();
        this.hub = createRealtimeHub({
            authenticator: createFunctionAuthenticator((token) => {
                const payload = JWTManager.verifyToken(token);
                return { userId: payload.id, jti: payload.jti, ...payload };
            }),
            redis: RedisClient.getInstance(),
            redisPrefix: `myapp:${process.env.NODE_ENV ?? "dev"}:ws`,
        });
    }
}
export default RealtimeService;
```

### 4.2 Register it

```ts
// src/core/ServiceProvider.ts
ServiceProvider.instance.put(RealtimeService.name, new RealtimeService());
```

### 4.3 Skip auth middleware on the upgrade path

The upgrade handler verifies the token itself, so add the WS path to the exclusion list:

```ts
// src/app/App.ts
this.app.use(authMiddleware({
    exclude: ["/api/v1/user/auth/login", "/api/v1/user/auth/signup", "/api/v1/ws"],
}));
```

### 4.4 Attach to the HTTP server after `app.listen`

```ts
// src/Server.ts
import { attachWsServer, createWsHealthRouter } from "realtime-ws";
import RealtimeService from "./app/services/RealtimeService";

const server = await app.listen();
const hub = RealtimeService.instance.hub;

attachWsServer(server, hub, { path: "/api/v1/ws" });
app.express.use("/api/v1/ws", createWsHealthRouter(hub));

process.on("SIGTERM", async () => {
    await hub.shutdown();
    process.exit(0);
});
```

### 4.5 Build a channel

```ts
// src/app/realtime/NotificationsChannel.ts
import { BaseChannel, type AuthIdentity } from "realtime-ws";

export class NotificationsChannel extends BaseChannel {
    readonly name = "notifications";
    authorize(identity: AuthIdentity, topic: string): boolean {
        return topic === `user:${identity.userId}`;
    }
}
```

Register it in `RealtimeService`:

```ts
this.hub.registerChannel(new NotificationsChannel());
```

### 4.6 Publish from any controller / service

```ts
// inside a controller method
await RealtimeService.instance.hub.publishToUser(
    targetUserId,
    "notifications",
    { title: "Hello", body: "..." },
);

// or to a topic
await RealtimeService.instance.hub.publish(
    "tasks",
    `space:${spaceId}`,
    { event: "taskCreated", taskId },
);
```

---

## 5. Client connection examples

```ts
// Node / Deno
const ws = new WebSocket("wss://api.example.com/api/v1/ws?token=" + jwt);

// Browser — pass the token via subprotocol because custom headers are not allowed
const ws = new WebSocket("wss://api.example.com/api/v1/ws", ["bearer", jwt]);
```

```ts
ws.onmessage = (e) => {
    const evt = JSON.parse(e.data);
    if (evt.event === "connected") {
        ws.send(JSON.stringify({ action: "subscribe", channel: "notifications", topic: `user:${myId}` }));
    }
};
```

---

## 6. Project layout

```
packages/realtime-ws/
├── README.md                       ← this document
├── package.json                    ← peer deps: ws, ioredis (opt), express (opt)
├── tsconfig.json
├── src/
│   ├── index.ts                    ← public barrel
│   ├── types.ts                    ← all public TS contracts
│   ├── core/
│   │   ├── RealtimeHub.ts          ← the facade users import
│   │   ├── ConnectionManager.ts    ← connection lifecycle, heartbeat, rate limit
│   │   ├── ChannelRegistry.ts      ← topic routing + lifecycle dispatch
│   │   ├── SessionManager.ts       ← presence (memory + redis)
│   │   └── RedisPubSub.ts          ← cross-instance fan-out
│   ├── auth/
│   │   └── TokenAuthenticator.ts   ← function → authenticator helper
│   ├── channels/
│   │   ├── BaseChannel.ts          ← no-op defaults for IChannel
│   │   └── BroadcastChannel.ts     ← server-push-only convenience
│   ├── express/
│   │   ├── attachWsServer.ts       ← upgrade handler for http.Server
│   │   └── createWsHealthRouter.ts ← /health, /online, /online/bulk
│   └── utils/
│       └── logger.ts
└── examples/
    ├── integration.example.ts
    ├── notifications-channel.example.ts
    └── chat-channel.example.ts
```

---

## 7. Configuration reference

Everything passed to `createRealtimeHub({...})`:

| Option                       | Type                  | Default              | Description                                              |
| ---------------------------- | --------------------- | -------------------- | -------------------------------------------------------- |
| `authenticator` *(required)* | `TokenAuthenticator`  | —                    | Verifies the token presented during the upgrade.         |
| `redis`                      | `RedisLike \| null`   | `null`               | ioredis-compatible client; enables cross-instance mode.  |
| `redisPrefix`                | `string`              | `"realtime"`         | Namespace for all Redis keys + pub/sub channels.         |
| `heartbeatMs`                | `number`              | `30000`              | Server-initiated ping interval.                          |
| `maxConnectionsPerUser`      | `number`              | `10`                 | Oldest connection evicted on overflow.                   |
| `maxMessageSizeBytes`        | `number`              | `65536`              | Rejected with `MESSAGE_TOO_LARGE`.                       |
| `rateLimitWindowMs`          | `number`              | `1000`               | Rolling window for inbound rate limit.                   |
| `rateLimitMaxMessages`       | `number`              | `20`                 | Per-window cap; over → `RATE_LIMITED`.                   |
| `backpressureThresholdBytes` | `number`              | `131072`             | Drop writes when send buffer exceeds this.               |
| `sessionTtlSeconds`          | `number`              | `120`                | Redis session/online key TTL (refreshed on activity).    |
| `logger`                     | `Logger`              | `console` wrapper    | Bring your own pino/winston/etc.                         |

---

## 8. Operational notes

- **Single instance, no Redis** → omit `redis`. Everything works in-memory; presence is local-only.
- **Multi-instance** → pass the same Redis client used elsewhere in the host. The plugin will `.duplicate()` it for the subscriber connection (ioredis requirement). All publishes are echoed across instances; the publishing node skips its own loopback via `_instanceId` tagging.
- **Graceful shutdown** → call `hub.shutdown()` from `SIGTERM`. It closes every connection with code `1000` and quits the subscriber connection. The publisher connection (owned by the host) is left alone.
- **Authorization** → channel `authorize(identity, topic)` runs on every subscribe. The plugin never calls into the host's permission system directly; you control it inside your channel code (typically by calling a service from `ServiceProvider`).

---

## 9. Caveats / non-goals

- The plugin is **TypeScript-first**. JavaScript consumers work but lose type safety on `IChannel`.
- It uses the Node [`ws`](https://www.npmjs.com/package/ws) package — no Socket.IO compatibility.
- It does **not** persist messages. That is the channel's responsibility (call your `ChatService` etc. from `onMessage`).
- It does **not** ship a chat service, push-notification integration, or domain-specific channels — those belong in the host project (the original Tasker `helpers/channels/*` are good references).
