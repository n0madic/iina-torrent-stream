// log.ts — diagnostic logging. Writes to a file inside the plugin data folder
// so plugin behaviour can be inspected directly, without IINA's in-app viewer.
//
// Each batch is appended to the file via file.handle(..., "write") + seekToEnd
// rather than file.write (which is whole-file replace). The main and global
// entries run in separate JS contexts and each had its own in-memory line
// buffer; a flush in one entry was overwriting whatever the other had just
// written, which is why diag lines from the main entry kept vanishing.

const { file, console } = iina;

export const LOG_PATH = "@data/torrent-stream.log";

const pending: string[] = [];
let flushScheduled = false;

function flush(): void {
  flushScheduled = false;
  if (pending.length === 0) return;
  const batch = pending.splice(0).join("");
  try {
    const h = file.handle(LOG_PATH, "write");
    try {
      // "write" mode here is read/write without truncation (it supports
      // seekTo / seekToEnd / offset), so seekToEnd + write is a true append.
      h.seekToEnd();
      h.write(batch);
    } finally {
      h.close();
    }
  } catch {
    // Put the batch back so the next flush can retry; never lose lines just
    // because the file was momentarily unavailable.
    pending.unshift(batch);
  }
}

function scheduleFlush(): void {
  if (flushScheduled) return;
  flushScheduled = true;
  // Debounced so a burst of diag calls collapses into one write. JSC's
  // single-threaded event loop makes the flag check race-free within this
  // entry. Cross-entry concurrency is handled by the seekToEnd append above.
  setTimeout(flush, 200);
}

/** Appends a timestamped line to the diagnostic log and the IINA console. */
export function diag(message: string): void {
  try {
    console.log(`[TS] ${message}`);
  } catch {
    // ignore
  }
  pending.push(`${new Date().toISOString()} ${message}\n`);
  scheduleFlush();
}

/**
 * Resets the diagnostic log to empty. Must be called only from the global entry
 * (which loads once per IINA launch) — calling it from the main entry would
 * wipe the log every time a player window opens. This also (re)creates the
 * file so subsequent file.handle() calls have something to open.
 */
export function resetDiagLog(): void {
  pending.length = 0;
  try {
    file.write(LOG_PATH, "");
  } catch {
    // Best-effort: a missing or unwritable log is non-fatal.
  }
}
