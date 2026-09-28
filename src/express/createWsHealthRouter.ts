import { Router, type Request, type Response } from "express";
import type { RealtimeHub } from "../core/RealtimeHub";

/**
 * Express router exposing read-only WebSocket diagnostics, ready to be
 * mounted under any path you prefer (e.g. `/api/v1/ws`):
 *
 * | Method | Path                | Description                              |
 * |--------|---------------------|------------------------------------------|
 * | GET    | `/health`           | Hub status + local connection count      |
 * | GET    | `/online/:userId`   | Single-user presence check               |
 * | POST   | `/online/bulk`      | Bulk presence (`{ userIds: [] }`)        |
 *
 * Mount with the project's standard `asyncContextHandler` if you want
 * the unified response shape — see `examples/integration.example.ts`.
 *
 * The router is intentionally framework-light so your own auth/rate-limit
 * middleware can wrap it.
 */
export function createWsHealthRouter(hub: RealtimeHub): Router {
    const router = Router();

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
