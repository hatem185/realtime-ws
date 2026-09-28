import type { Buffer } from "node:buffer";
import type { IncomingMessage, Server as HttpServer } from "node:http";
import type { Socket } from "node:net";
import { clearTimeout } from "node:timers";
// @ts-types="@types/ws"
import { WebSocketServer } from "ws";
import type { RealtimeHub } from "../core/RealtimeHub.ts";
import type { AuthIdentity } from "../types.ts";
import { LIMIT_DEFAULTS } from "../core/ConnectionManager.ts";
import { errorMessage, settleWithin, startTimer } from "../utils/async.ts";

export interface AttachOptions {
    /**
     * URL path that triggers the upgrade. Defaults to `/api/v1/ws`,
     * matching the custom-api convention of mounting everything under
     * `/api/v1`. Matched exactly (a trailing `/` is tolerated); sub-paths
     * are left to other upgrade listeners.
     */
    path?: string;

    /**
     * Override how the bearer token is extracted from the upgrade
     * request. Defaults to: `?token=…` query-string OR
     * `Authorization: Bearer …` header OR
     * `Sec-WebSocket-Protocol: bearer, <token>` (browser-friendly).
     * A throw is answered with 400.
     */
    extractToken?: (req: IncomingMessage) => string | null;

    /**
     * Hook fired right before the WebSocket is registered with the hub.
     * Throw to abort (answered with 403). Useful for extra validation (e.g.
     * checking blocked users) without modifying the channel.
     */
    onUpgrade?: (identity: AuthIdentity, req: IncomingMessage) => Promise<void> | void;

    /**
     * Longest the handshake (token extraction, `verify()`, `onUpgrade`) may take
     * before the request is answered with 503 and closed. Default: 10000.
     */
    handshakeTimeoutMs?: number;
}

interface AttachedHandle {
    /** The underlying ws server. */
    wss: WebSocketServer;
    /**
     * Detach the upgrade listener, close this server's connections with 1001 and close the ws
     * server. Bounded by the hub's `shutdownTimeoutMs`; stragglers are terminated.
     */
    close: () => Promise<void>;
}

const noop = () => {};

const defaultExtract = (req: IncomingMessage): string | null => {
    if (!req.url) return null;
    const url = new URL(req.url, "http://localhost");
    const fromQuery = url.searchParams.get("token");
    if (fromQuery) return fromQuery;

    const auth = req.headers.authorization;
    if (auth && auth.startsWith("Bearer ")) return auth.slice(7);

    // Browsers cannot set custom headers on a WS handshake, so it is
    // common to smuggle the token through the subprotocol field:
    //   new WebSocket(url, ["bearer", token])
    const proto = req.headers["sec-websocket-protocol"];
    if (typeof proto === "string") {
        const parts = proto.split(",").map((p) => p.trim());
        const idx = parts.findIndex((p) => p.toLowerCase() === "bearer");
        if (idx >= 0 && parts[idx + 1]) return parts[idx + 1];
    }

    return null;
};

/** Answer with an HTTP error and close. No-op on a socket that is already answered or gone. */
const denyUpgrade = (socket: Socket, status: number, reason: string): void => {
    if (socket.destroyed || socket.writableEnded) return;
    socket.once("finish", () => socket.destroy());
    socket.end(`HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
};

const pathOf = (url: string | undefined): string | null => {
    if (!url) return null;
    const q = url.indexOf("?");
    return q === -1 ? url : url.slice(0, q);
};

/**
 * Attach a {@link RealtimeHub} to an existing Node HTTP server. Works
 * out of the box with `app.listen(...)` from Express — pass the returned
 * server to this function.
 *
 * Upgrade requests for other paths (or non-WebSocket upgrades such as `h2c`)
 * are answered with 404 and closed, unless another `upgrade` listener is
 * registered on the server to take care of them.
 *
 * @example
 * ```ts
 * const server = app.listen(PORT);
 * attachWsServer(server, hub, { path: "/api/v1/ws" });
 * ```
 */
export function attachWsServer(
    server: HttpServer,
    hub: RealtimeHub,
    options: AttachOptions = {},
): AttachedHandle {
    const path = options.path ?? "/api/v1/ws";
    const extract = options.extractToken ?? defaultExtract;
    const handshakeTimeoutMs = options.handshakeTimeoutMs ?? 10_000;
    // Oversized frames are refused by ws at the frame header (close 1009), before being buffered.
    const wss = new WebSocketServer({
        noServer: true,
        maxPayload: hub.options.maxMessageSizeBytes,
    });
    const claimed = new WeakSet<Socket>();

    const handshake = async (req: IncomingMessage, socket: Socket, head: Buffer): Promise<void> => {
        const timer = startTimer(
            () => denyUpgrade(socket, 503, "Service Unavailable"),
            handshakeTimeoutMs,
        );
        // Client left, or we already answered (e.g. the handshake timed out).
        const gone = () => socket.destroyed || socket.writableEnded;
        try {
            if (hub.isShuttingDown) return denyUpgrade(socket, 503, "Service Unavailable");

            let token: string | null;
            try {
                token = extract(req);
            } catch {
                return denyUpgrade(socket, 400, "Bad Request");
            }
            if (!token) return denyUpgrade(socket, 401, "Unauthorized");

            let identity: AuthIdentity;
            try {
                identity = await hub.options.authenticator.verify(token);
            } catch {
                return denyUpgrade(socket, 401, "Unauthorized");
            }
            if (gone()) return;
            // Numeric ids (common in SQL-backed hosts) are used as strings, like everywhere else.
            const userId: unknown = identity?.userId;
            if (typeof userId === "number" && Number.isFinite(userId)) {
                identity = { ...identity, userId: String(userId) };
            } else if (typeof userId !== "string" || !userId) {
                hub.logger.warn("WebSocket upgrade refused: the authenticator returned no userId.");
                return denyUpgrade(socket, 401, "Unauthorized");
            }

            if (options.onUpgrade) {
                try {
                    await options.onUpgrade(identity, req);
                } catch {
                    return denyUpgrade(socket, 403, "Forbidden");
                }
                if (gone()) return;
            }
            if (hub.isShuttingDown) return denyUpgrade(socket, 503, "Service Unavailable");

            clearTimeout(timer);
            try {
                wss.handleUpgrade(req, socket, head, (ws) => {
                    socket.off("error", noop); // ws has its own listener from here on
                    hub.acceptSocket(ws, identity).catch((err) => {
                        hub.logger.error("acceptSocket error:", err);
                        try {
                            ws.close(1011, "Internal error");
                        } catch {
                            // ignore
                        }
                    });
                });
            } catch (err) {
                // Only throws when another listener already upgraded this socket: leave it be.
                hub.logger.error("handleUpgrade failed:", errorMessage(err));
            }
        } finally {
            clearTimeout(timer);
        }
    };

    const onUpgrade = (req: IncomingMessage, socket: Socket, head: Buffer): void => {
        // Node removes its own socket error handler before emitting 'upgrade'; without one, a
        // reset while the request is pending would crash the process.
        socket.on("error", noop);

        const reqPath = pathOf(req.url);
        if (reqPath !== path && reqPath !== `${path}/`) {
            if (server.listenerCount("upgrade") <= 1) denyUpgrade(socket, 404, "Not Found");
            return;
        }
        if (claimed.has(socket)) return;
        claimed.add(socket);

        handshake(req, socket, head).catch((err) => {
            hub.logger.error("WebSocket upgrade error:", errorMessage(err));
            denyUpgrade(socket, 500, "Internal Server Error");
        });
    };

    server.on("upgrade", onUpgrade);
    const detach = () => server.off("upgrade", onUpgrade);
    const offShutdown = hub.onShutdown(detach);

    return {
        wss,
        close: async () => {
            detach();
            offShutdown();
            for (const ws of wss.clients) ws.close(1001, "Server shutting down");
            const closed = new Promise<void>((resolve) => wss.close(() => resolve()));
            await settleWithin(
                closed,
                hub.options.shutdownTimeoutMs ?? LIMIT_DEFAULTS.shutdownTimeoutMs,
            );
            for (const ws of wss.clients) ws.terminate();
            await settleWithin(closed, 1000);
        },
    };
}
