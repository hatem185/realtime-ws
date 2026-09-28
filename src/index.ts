/**
 * realtime-ws — framework-agnostic WebSocket plugin.
 *
 * See `README.md` for an architecture overview and integration guide.
 */

export * from "./mod.ts";

// Express adapter (optional)
export { createWsHealthRouter } from "./express/createWsHealthRouter.ts";
