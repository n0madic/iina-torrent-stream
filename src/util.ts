// Shared utilities used by both the main (per-player) and global entries.

/**
 * Extracts a human-readable message from an unknown thrown value.
 *
 * IINA's http module rejects with a plain object (e.g. { reason, statusCode })
 * rather than an Error, so default String(e) yields "[object Object]". This
 * digs out anything human-readable, falling back to JSON, then to String.
 */
export function errorMessage(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (typeof e === "string") return e;
  if (e && typeof e === "object") {
    const obj = e as { message?: unknown; error?: unknown; reason?: unknown };
    if (typeof obj.message === "string") return obj.message;
    if (typeof obj.error === "string") return obj.error;
    if (typeof obj.reason === "string") return obj.reason;
    try {
      return JSON.stringify(e);
    } catch {
      /* fallthrough */
    }
  }
  return String(e);
}

/** Runs a function on the plugin's JS queue, where IINA UI calls are safe. */
export function onPluginQueue(fn: () => void): void {
  setTimeout(fn, 0);
}

/** A simple cooperative cancellation token — IINA's JSC runtime predates
 * AbortController support in plugins, so we use a plain function check. */
export type CancelFn = () => boolean;
