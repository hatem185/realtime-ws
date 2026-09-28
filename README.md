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
{ "event": "error",        "code": "RATE_LIMITED", "message": "...", "channel": "...", "topic": "..." }
```

Protocol rules:

- A connection's messages are handled **one at a time, in the order sent**, and their replies come back in that order. Two exceptions are sent immediately and may overtake replies to earlier messages: `pong` (so liveness checks aren't held up by slow handlers) and the refusals `RATE_LIMITED` / `MESSAGE_TOO_LARGE`, which carry no `channel`/`topic`. Messages sent right after the socket opens are processed too (you don't have to wait for `connected`).
- `subscribe` runs the channel's `authorize()`. Subscribing again to a topic you hold is acknowledged without re-authorizing.
- `message` is accepted only on a topic the connection is subscribed to (the subscription is the authorization); otherwise `NOT_SUBSCRIBED`. Success has no ack; if the channel's `onMessage` returns `{ broadcast }`, subscribers (including the sender) receive it.
- `unsubscribe` is always acknowledged; `onUnsubscribe` runs only if the connection was subscribed.
- Every malformed or refused request gets exactly one `error` event. `channel`/`topic` are included when they identify the request.

| Error code          | When                                                                  |
| ------------------- | --------------------------------------------------------------------- |
| `INVALID_JSON`      | Not valid JSON                                                        |
| `INVALID_MESSAGE`   | JSON that isn't an object (`null`, numbers, strings, arrays)          |
| `MISSING_ACTION`    | No `action`, or not a string                                          |
| `UNKNOWN_ACTION`    | `action` isn't one of the actions above                               |
| `MISSING_FIELDS`    | `channel`/`topic` missing, empty or not strings                       |
| `SUBSCRIBE_FAILED`  | Unknown channel, `authorize()` denied or threw, topic too long, subscription limit reached |
| `NOT_SUBSCRIBED`    | `message` on a topic the connection isn't subscribed to               |
| `MESSAGE_FAILED`    | The channel's `onMessage` threw                                       |
| `RATE_LIMITED`      | Over the per-connection rate, or too many messages still being handled |
| `MESSAGE_TOO_LARGE` | Over `maxMessageSizeBytes` (only reachable without `attachWsServer`, see below) |

Close codes:

| Code   | Meaning                                                                      | Client should |
| ------ | ---------------------------------------------------------------------------- | ------------- |
| `1000` | Closed by the server (`connections.unregister()`)                            | —             |
| `1001` | Server shutting down (`hub.shutdown()`)                                      | Reconnect (another instance will take it) |
| `1009` | Frame larger than `maxMessageSizeBytes`                                      | Send smaller messages |
| `1013` | A message took longer than `requestTimeoutMs` to handle (e.g. a hook hung)   | Reconnect with backoff |
| `4001` | Evicted: the user opened more than `maxConnectionsPerUser` connections       | **Not** auto-reconnect (it would evict the newer tab in turn) |
| `1006` | No close frame: e.g. the heartbeat dropped an unresponsive connection        | Reconnect with backoff |

The codes are exported as `CloseCodes`.

### Built-in safety

- **Heartbeat** (default 30 s) — terminates connections after 2 missed pongs, and refreshes the presence of connections that answered.
- **Per-user cap** (default 10) — oldest connection evicted (close `4001`) on overflow. The cap is per instance.
- **Message-size cap** (default 64 KB) — `attachWsServer` sets `ws`'s `maxPayload`, so an oversized frame is refused at its header (close `1009`) before it is buffered.
- **Per-connection rate limit** (default 20 msg / 1 s) — overflow gets `RATE_LIMITED`. At most that many messages can be waiting to be handled at once; the rest are refused the same way.
- **Backpressure** (default 128 KB buffered) — every outbound write, including error replies, pongs and acks, is dropped for a client that stopped reading instead of OOM-ing; `stats().droppedMessages` counts them. `ws` answers protocol-level pings by itself, so a client that keeps pinging while it isn't reading is disconnected. Only pongs (the ws-level pong, or an app-level `{"action":"pong"}` answering the server's `ping` event) count as liveness, so a client that stops reading is closed by the heartbeat even if it keeps sending.
- **Subscription limits** — at most 1000 topics per connection and 256 characters per topic, checked before `authorize()`.

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

Then `npm install` and you can `import { ... } from "realtime-ws"` (CommonJS or ESM).

> The package is consumed from its compiled `dist/` (`main`/`types`/`exports` point there). `npm install` inside the package builds it (the `prepare` script), and so does `npm pack`, whose tarball you can install instead of a `file:` path. Only the package root (and `realtime-ws/package.json`) is exported: import everything from `"realtime-ws"`.
>
> `express` is optional: the package loads without it; only `createWsHealthRouter()` needs it. TypeScript hosts without Express need `skipLibCheck: true` (the default in most setups) or `@types/express`, because the router's declaration file refers to Express types.
>
> **Deno 2** works through its Node compatibility layer: use `node:http` with `ws` (and `ioredis`) from npm, exactly as above. Sockets from `Deno.serve()` + `Deno.upgradeWebSocket()` aren't supported, because the hub needs the `ws` socket API. For `deno check`, add `@types/node` to the host project.

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

The upgrade handler verifies the token itself, so add the WS path to the exclusion list. If you mount `createWsHealthRouter` under the same prefix (below), make sure the exclusion doesn't also cover its routes (or mount it elsewhere): the router does no authentication of its own, and `/online/*` answers presence for any user id.

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
    await hub.shutdown(); // stops upgrades, closes clients with 1001, cleans presence; bounded (shutdownTimeoutMs)
    process.exit(0);
});
```

`attachWsServer` matches `path` exactly (a trailing `/` is tolerated). Other upgrade requests — other paths, or non-WebSocket upgrades such as `h2c` — are answered with `404` and closed when `attachWsServer` is the server's only `upgrade` listener, and left alone otherwise. A handshake that takes longer than `handshakeTimeoutMs` (default 10 s; token extraction + `verify()` + `onUpgrade`) is answered with `503`.

`createWsHealthRouter(hub)` serves `GET /health`, `GET /online/:userId` and `POST /online/bulk` (body `{ "userIds": [...] }`, at most 1,000 ids per request, `400` otherwise). It does no authentication of its own (see 4.3).

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

`publishToUser` reaches every connection of that user on every instance, and nobody else, whether or not those connections subscribed to anything. The optional `topic` (default `user:<id>`) only labels the delivered event.

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

ws.onclose = (e) => {
    if (e.code === 4001) return; // evicted by a newer connection of the same user: don't fight it
    setTimeout(connect, backoffMs()); // 1001 (instance going away), 1006 (network): reconnect with backoff
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
│       ├── async.ts                ← timers and bounded waits
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
| `maxConnectionsPerUser`      | `number`              | `10`                 | Oldest connection evicted (close `4001`) on overflow.    |
| `maxMessageSizeBytes`        | `number`              | `65536`              | Larger frames are refused with close `1009`.             |
| `maxSubscriptionsPerConnection` | `number`           | `1000`               | Further subscribes get `SUBSCRIBE_FAILED`.               |
| `maxTopicLength`             | `number`              | `256`                | Longer topics get `SUBSCRIBE_FAILED`.                    |
| `rateLimitWindowMs`          | `number`              | `1000`               | Rolling window for inbound rate limit.                   |
| `rateLimitMaxMessages`       | `number`              | `20`                 | Per-window cap; over → `RATE_LIMITED`.                   |
| `backpressureThresholdBytes` | `number`              | `131072`             | Drop writes when send buffer exceeds this.               |
| `sessionTtlSeconds`          | `number`              | `120`                | Redis session/online key TTL, refreshed by the heartbeat; keep it ≥ 2× `heartbeatMs`. |
| `shutdownTimeoutMs`          | `number`              | `10000`              | How long `hub.shutdown()` waits for clients and cleanup; quitting the Redis subscriber adds at most 1 s. Keep the total below your orchestrator's grace period. |
| `requestTimeoutMs`           | `number`              | `30000`              | A connection stuck on one message longer than this (hung `authorize()`/hook) is closed with `1013` at the next heartbeat. |
| `logger`                     | `Logger`              | `console` wrapper    | Bring your own pino/winston/etc.                         |

`attachWsServer(server, hub, options)` takes `path` (default `/api/v1/ws`), `extractToken`, `onUpgrade` and `handshakeTimeoutMs` (default `10000`). The authenticator's `userId` may be a string or a finite number (used as a string). Timeouts set to `Infinity` mean "no timeout".

---

## 8. Operational notes

- **Single instance, no Redis** → omit `redis`. Everything works in-memory; presence is local-only.
- **Multi-instance** → pass the same Redis client used elsewhere in the host. The plugin will `.duplicate()` it for the subscriber connection (ioredis requirement). All publishes are echoed across instances; the publishing node skips its own loopback via `_instanceId` tagging.
- **Graceful shutdown** → call `hub.shutdown()` from `SIGTERM`. It detaches the upgrade listener, closes every connection with code `1001` (clients reconnect to another instance), removes everyone's presence from Redis at once, runs `onDisconnect` hooks and quits the subscriber connection — within `shutdownTimeoutMs` (quitting the subscriber adds at most 1 s), after which remaining sockets are terminated. Repeated calls return the same promise. The publisher connection (owned by the host) is left alone.
- **Presence** → a connection's presence lives in Redis for `sessionTtlSeconds` and is refreshed by the heartbeat while the client answers pings (idle clients stay online). It is removed when the connection closes, before the channels' `onDisconnect` hooks run (waiting at most 2 s for Redis), so those hooks see up-to-date presence. If an instance crashes without cleanup, its users can appear online for up to `sessionTtlSeconds` after they've actually left. `lastActive` is written on connect, on disconnect, and at most once per heartbeat for users who sent something other than pings. The per-connection `session:*` keys are rewritten at the next heartbeat after a change.
- **Redis outages** → the hub keeps serving locally; topic subscriptions made while Redis is down (or lost in a restart/failover) are re-applied when the subscriber reconnects, and presence is re-written by the next heartbeat. `lazyConnect` clients are supported.
- **Rolling upgrades** → topic keys and message envelopes are unchanged for ordinary channel names, so old and new instances interoperate, except `publishToUser`, until every instance runs the new version: messages published by an old instance still take the old topic-based route, and users connected to an old instance don't receive `publishToUser` messages published by an upgraded one (old instances don't listen on the new user channel). Keep the rollout short.
- **Authorization** → channel `authorize(identity, topic)` runs on every subscribe. The plugin never calls into the host's permission system directly; you control it inside your channel code (typically by calling a service from `ServiceProvider`).

---

## 9. Caveats / non-goals

- The plugin is **TypeScript-first**. JavaScript consumers work but lose type safety on `IChannel`.
- It uses the Node [`ws`](https://www.npmjs.com/package/ws) package — no Socket.IO compatibility.
- It does **not** persist messages. That is the channel's responsibility (call your `ChatService` etc. from `onMessage`).
- It does **not** ship a chat service, push-notification integration, or domain-specific channels — those belong in the host project (the original Tasker `helpers/channels/*` are good references).
