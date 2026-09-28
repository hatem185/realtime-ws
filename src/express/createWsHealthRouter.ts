import type { Request, Response, Router } from "express";
import type { RealtimeHub } from "../core/RealtimeHub";

/** Most ids accepted by `POST /online/bulk` in one request. */
const MAX_BULK_IDS = 1000;

/**
 * Express router exposing read-only WebSocket diagnostics, ready to be
 * mounted under any path you prefer (e.g. `/api/v1/ws`):
 *
 * | Method | Path                | Description                              |
 * |--------|---------------------|------------------------------------------|
 * | GET    | `/health`           | Hub status + local connection count      |
 * | GET    | `/online/:userId`   | Single-user presence check               |
 * | POST   | `/online/bulk`      | Bulk presence (`{ userIds: [] }`, ≤1000) |
 *
 * Mount with the project's standard `asyncContextHandler` if you want
 * the unified response shape — see `examples/integration.example.ts`.
 *
 * The router is intentionally framework-light so your own auth/rate-limit
 * middleware can wrap it. It does no authentication itself: mount it behind
 * your auth middleware, or anyone can query anyone's presence.
 *
 * Express is loaded when this is called, so the rest of the package works
 * without Express installed.
 */
export function createWsHealthRouter(hub: RealtimeHub): Router {
    const express = require("express") as typeof import("express");
    const router = express.Router();

    router.get("/health", (_req: Request, res: Response) => {
        res.status(200).json({
            statusCode: 200,
            status: true,
            message: "Realtime hub healthy",
            data: hub.stats(),
        });
    });

    router.get("/online/:userId", async (req: Request, res: Response) => {
        const online = await hub.isOnline(req.params.userId);
        res.status(200).json({
            statusCode: 200,
            status: true,
            message: "Presence resolved",
            data: { userId: req.params.userId, online },
        });
    });

    router.post("/online/bulk", async (req: Request, res: Response) => {
        const ids = Array.isArray(req.body?.userIds) ? (req.body.userIds as unknown[]) : [];
        if (ids.length > MAX_BULK_IDS) {
            res.status(400).json({
                statusCode: 400,
                status: false,
                message: `At most ${MAX_BULK_IDS} userIds per request`,
            });
            return;
        }
        const safe = ids.filter((x): x is string => typeof x === "string");
        const data = await hub.areUsersOnline(safe);
        res.status(200).json({
            statusCode: 200,
            status: true,
            message: "Presence resolved",
            data,
        });
    });

    return router;
}
