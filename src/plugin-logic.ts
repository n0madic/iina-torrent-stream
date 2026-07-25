/**
 * Shares one in-flight asynchronous attempt between concurrent callers.
 * A settled attempt is cleared so a later call can retry after either success
 * or failure.
 */
export class SingleFlight<T> {
  private active: Promise<T> | null = null;

  run(start: () => Promise<T>): Promise<T> {
    if (this.active !== null) return this.active;

    const attempt = start();
    this.active = attempt;
    void attempt.then(
      () => this.clear(attempt),
      () => this.clear(attempt),
    );
    return attempt;
  }

  private clear(attempt: Promise<T>): void {
    if (this.active === attempt) this.active = null;
  }
}

/** Appends the daemon capability token without discarding existing parameters. */
export function withDaemonToken(path: string, token: string): string {
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}token=${encodeURIComponent(token)}`;
}

/** Prevents the per-run daemon capability from being persisted in diagnostics. */
export function redactDaemonToken(value: string): string {
  return value.replace(/([?&]token=)[^&]*/g, "$1<redacted>");
}
