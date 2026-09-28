import type { Logger } from "../types.ts";

const PREFIX = "[realtime-ws]";

/** Default logger — prefixes every line so it is easy to grep in production. */
export const defaultLogger: Logger = {
    debug: (msg, ...meta) => console.debug(`${PREFIX} ${msg}`, ...meta),
    info: (msg, ...meta) => console.log(`${PREFIX} ${msg}`, ...meta),
    warn: (msg, ...meta) => console.warn(`${PREFIX} ${msg}`, ...meta),
    error: (msg, ...meta) => console.error(`${PREFIX} ${msg}`, ...meta),
};
