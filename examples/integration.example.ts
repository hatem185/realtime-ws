/**
 * Reference: dropping realtime-ws into a custom-api project.
 *
 * The snippets below assume the folder layout described in
 * `docs/custom-api.md`. Copy what you need into your project; this file
 * is illustrative only and is excluded from the package build.
 *
 * ---------------------------------------------------------------------
 * Step 1 — Wire the hub as a Service so it can be resolved from the DI
 *           container, the same way every other service is.
 * ---------------------------------------------------------------------
 */

// src/app/services/RealtimeService.ts
/*
import { Service } from "@core/Service";
import { ServiceProvider } from "@core/ServiceProvider";
import {
    createRealtimeHub,
    createFunctionAuthenticator,
    type RealtimeHub,
} from "realtime-ws";
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
            heartbeatMs: 30_000,
            maxConnectionsPerUser: 10,
        });

        // Register your channels here
        // this.hub.registerChannel(new ChatChannel());
        // this.hub.registerChannel(new NotificationsChannel());
    }
}

export default RealtimeService;
*/

/**
 * ---------------------------------------------------------------------
 * Step 2 — Register the service in `ServiceProvider.init()`.
 * ---------------------------------------------------------------------
 */

// src/core/ServiceProvider.ts
/*
import RealtimeService from "@app/services/RealtimeService";

static init() {
    // ...existing services
    ServiceProvider.instance.put(RealtimeService.name, new RealtimeService());
}
*/

/**
 * ---------------------------------------------------------------------
 * Step 3 — Attach the WebSocket upgrade handler & health router after
 *           `app.listen(...)`. The health endpoints are mounted at
 *           `/api/v1/ws/health` to match the `/api/v1` invariant.
 * ---------------------------------------------------------------------
 */

// src/Server.ts
/*
import "reflect-metadata";
import dotenv from "dotenv";
dotenv.config();

import { ServiceProvider } from "./core/ServiceProvider";
ServiceProvider.init();

import App from "./app/App";
import { attachWsServer, createWsHealthRouter } from "realtime-ws";
import RealtimeService from "./app/services/RealtimeService";

const PORT = process.env.PORT ? +process.env.PORT : 3000;
const app = new App(PORT);

(async () => {
    await app.init();
    const server = await app.listen();   // returns the Node http.Server

    const hub = RealtimeService.instance.hub;
    attachWsServer(server, hub, { path: "/api/v1/ws" });
    app.express.use("/api/v1/ws", createWsHealthRouter(hub));

    process.on("SIGTERM", async () => {
        await hub.shutdown();
        process.exit(0);
    });
})();
*/

/**
 * ---------------------------------------------------------------------
 * Step 4 — Add an exclusion in `authMiddleware` so the WS upgrade route
 *           skips the bearer-header check (the upgrade handler verifies
 *           the token itself).
 * ---------------------------------------------------------------------
 */

// src/app/App.ts (excerpt)
/*
this.app.use(authMiddleware({
    exclude: [
        "/api/v1/user/auth/login",
        "/api/v1/user/auth/signup",
        "/api/v1/ws",          // <— add this
    ],
}));
*/

/**
 * ---------------------------------------------------------------------
 * Step 5 — Publish from any controller / service. Resolve the hub from
 *           the DI container — never construct a new one.
 * ---------------------------------------------------------------------
 */

// src/app/controllers/NotificationController.ts (excerpt)
/*
@Post("/")
@RequirePermission("notification.create")
public create() {
    const Schema = VS(z.object({ to: z.string(), title: z.string(), body: z.string() }));
    return asyncContextHandler(async (ctx) => {
        const data = Schema.validate(ctx.req.body);
        await this.notifications.persist(data);
        await RealtimeService.instance.hub.publishToUser(
            data.to,
            "notifications",
            { title: data.title, body: data.body },
        );
        return { statusCode: 201, status: true, message: "Notification queued" };
    });
}
*/

export {}; // ensure this file is treated as a module
