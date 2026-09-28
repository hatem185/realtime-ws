import type { IncomingMessage, Server as HttpServer } from "http";
import type { Socket } from "net";
import { WebSocketServer } from "ws";
import type { RealtimeHub } from "../core/RealtimeHub";
import type { AuthIdentity } from "../types";

export interface AttachOptions {
    /**
     * URL path that triggers the upgrade. Defaults to `/api/v1/ws`,
     * matching the custom-api convention of mounting everything under
     * `/api/v1`.
     */
    path?: string;

    /**
     * Override how the bearer token is extracted from the upgrade
     * request. Defaults to: `?token=…` query-string OR
     * `Authorization: Bearer …` header OR
     * `Sec-WebSocket-Protocol: bearer, <token>` (browser-friendly).
     */
    extractToken?: (req: IncomingMessage) => string | null;

    /**
     * Hook fired right before the WebSocket is registered with the hub.
     * Throw to abort. Useful for extra validation (e.g. checking blocked
     * users) without modifying the channel.
     */
    onUpgrade?: (identity: AuthIdentity, req: IncomingMessage) => Promise<void> | void;
}

interface AttachedHandle {
    /** The underlying ws server. */
    wss: WebSocketServer;
    /** Detach upgrade listener and close the ws server. */
    close: () => Promise<void>;
}

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

const denyUpgrade = (socket: Socket, status: number, reason: string): void => {
    try {
        socket.write(
            `HTTP/1.1 ${status} ${reason}\r\n` +
                `Connection: close\r\n` +
                `Content-Length: 0\r\n` +
                `\r\n`,
        );
    } catch {
        // ignore
    }
    try {
        socket.destroy();
    } catch {
        // ignore
    }
};

/**
 * Attach a {@link RealtimeHub} to an existing Node HTTP server. Works
 * out of the box with `app.listen(...)` from Express — pass the returned
 * server to this function.
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
    const wss = new WebSocketServer({ noServer: true });

    const onUpgrade = async (req: IncomingMessage, socket: Socket, head: Buffer) => {
        if (!req.url) {
            denyUpgrade(socket, 400, "Bad Request");
            return;
        }

        // Strict prefix-match (don't use String.includes — see custom-api §17)
        const reqPath = req.url.split("?")[0];
        if (reqPath !== path && !reqPath.startsWith(`${path}/`)) return;

        const token = extract(req);
        if (!token) {
            denyUpgrade(socket, 401, "Unauthorized");
            return;
        }

        let identity: AuthIdentity;
        try {
            identity = await hub.options.authenticator.verify(token);
        } catch {
            denyUpgrade(socket, 401, "Unauthorized");
            return;
        }

        if (!identity?.userId) {
            denyUpgrade(socket, 401, "Unauthorized");
            return;
        }

        if (options.onUpgrade) {
            try {
                await options.onUpgrade(identity, req);
            } catch {
                denyUpgrade(socket, 403, "Forbidden");
                return;
            }
        }

        wss.handleUpgrade(req, socket, head, (ws) => {
            hub.acceptSocket(ws, identity).catch((err) => {
                hub.logger.error("acceptSocket error:", err);
                try {
                    ws.close(1011, "Internal error");
                } catch {
                    // ignore
                }
            });
        });
    };

    server.on("upgrade", onUpgrade);

    return {
        wss,
        close: async () => {
            server.off("upgrade", onUpgrade);
            await new Promise<void>((resolve) => wss.close(() => resolve()));
        },
    };
}
