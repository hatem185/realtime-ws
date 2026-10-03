import type { Logger } from "../types.ts";

const noop = (..._args: unknown[]): void => {};

/** Default logger — intentionally silent to avoid console calls inside the package. */
export const defaultLogger: Logger = {
    debug: noop,
    info: noop,
    warn: noop,
    error: noop,
};
