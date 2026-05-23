// global.ts — Global entry. Provides Plugin-menu entry points for opening a
// torrent. New player windows are created immediately with the daemon's
// all-in-one /play URL — mpv handles "buffering while metadata resolves"
// natively, so the menu never blocks waiting for DHT. Metadata is resolved in
// parallel and the new window's main entry is then told which torrent it is
// playing via a "ts-attach" message so it can attach the sidebar, subtitles,
// and heartbeat tracking.

import {
  ensureRunning,
  addTorrent,
  heartbeat,
  playURL,
  getStatus,
} from "./daemon";
import type { TorrentStatus } from "./daemon";
import { diag, resetDiagLog } from "./log";
import { errorMessage, onPluginQueue } from "./util";

const { menu, global, utils, file, standaloneWindow } = iina;

// Wipe the diagnostic log at IINA launch — same approach as the daemon's
// O_TRUNC on torrentd.log, so neither file accumulates across sessions. Done
// from the global entry (loaded once per IINA run) rather than from log.ts
// (which is re-imported by every player window).
resetDiagLog();

// --- Progress window -------------------------------------------------------
//
// A tiny HUD-style standalone window with a spinner and the source URL,
// shown while the daemon is fetching metadata. IINA doesn't show the player
// window itself until mpv has a decoded frame (an architectural decision —
// the player NSWindow only un-hides on first frame), so for magnets opened
// from IINA's Welcome/Recent screen there is otherwise nothing visible for
// 5-30 seconds. The global entry owns this window so it is shared across
// all in-flight resolves and survives any single player window's lifecycle.

// Progress HUD layout, styling, and stat formatting all live in progress.html
// (loaded via standaloneWindow.loadFile). That's the canonical IINA path —
// the same one sidebar.html follows and the same one IINA's own built-in
// HUDs (Inspector, etc.) use. simpleMode + setStyle was tried first but
// produced bleed-through CSS text and a solid white background instead of
// vibrancy; the loadFile path doesn't have those quirks because the webview
// owns its own document tree.
//
// The plugin script communicates with the webview via postMessage("update",
// { source, status }) on every render, and the webview's iina.onMessage
// handler in progress.html updates the DOM. Per IINA's webview docs the
// body is transparent by default, so hudWindow vibrancy shows through.

// standaloneWindow is a per-plugin singleton, so the magnet-input dialog and
// the progress HUD have to share it. windowMode tracks what's currently
// loaded so we only call setProperty/setFrame/loadFile when switching modes
// — reloading the same page on every showProgress would cause a visible
// flash.
//
// CRITICAL: IINA's standaloneWindow.loadFile wipes the entire message
// listener table (JavascriptMessageHub.clearListeners is called inside it).
// So every onMessage handler MUST be (re-)registered AFTER each loadFile,
// not before — otherwise webview → plugin messages silently vanish because
// no listener exists when they arrive.
type WindowMode = "none" | "progress" | "magnet-input";
let windowMode: WindowMode = "none";
let progressOpen = false;
// Push timer: while the HUD is open, re-send the current state to the
// webview every 400ms. This costs nothing (a JSON.stringify of a tiny
// object) and is the only reliable way to defeat early-message loss — the
// webview may not have registered iina.onMessage by the time we post the
// first update, and there's no reliable "ready" signal that always lands.
let progressPushTimer: string | null = null;
// Track in-flight resolves: source -> latest status snapshot (null until the
// first poll succeeds) + the polling interval id (null when we don't yet
// know the infohash). The window closes only when the last entry is gone —
// otherwise opening a second magnet would dismiss the indicator that's
// still active for the first.
interface ProgressEntry {
  status: TorrentStatus | null;
  pollTimer: string | null;
}
const activeSources = new Map<string, ProgressEntry>();
// Which source is the HUD currently rendering. Updates re-render only when
// they target the visible entry (we show "latest" so a second open replaces
// the visible label and stats with the new torrent — the older one keeps
// the HUD alive but doesn't compete for the visible content).
let visibleSource: string | null = null;

/** Registers the standaloneWindow message handlers for the current mode.
 * MUST be called AFTER loadFile — see the comment on WindowMode above.
 * Idempotent for a given mode: re-registering the same handlers replaces
 * them in JavascriptMessageHub (a forEach-style registry keyed by name). */
function registerHandlersFor(mode: "progress" | "magnet-input"): void {
  if (mode === "progress") {
    standaloneWindow.onMessage("requestUpdate", () => {
      renderVisible();
    });
  } else {
    standaloneWindow.onMessage("magnet-submit", (value: unknown) => {
      diag(`magnet-submit received (len=${typeof value === "string" ? value.length : "non-string"})`);
      onMagnetSubmit(typeof value === "string" ? value : "");
    });
    standaloneWindow.onMessage("magnet-cancel", () => {
      diag("magnet-cancel received");
      onMagnetCancel();
    });
    // Magnet-input dialog tells us when its webview has registered onMessage,
    // so prefill from clipboard doesn't race against early-message loss.
    standaloneWindow.onMessage("magnet-ready", () => {
      diag("magnet-ready received");
      void onMagnetReady();
    });
  }
}

function switchWindowMode(mode: "progress" | "magnet-input"): void {
  if (windowMode === mode) return;
  diag(`switchWindowMode: ${windowMode} -> ${mode}`);
  windowMode = mode;
  if (mode === "progress") {
    // hudWindow=true gives the AppKit panel its translucent, vibrancy
    // background. Explicit title prevents IINA's default "Window — <plugin>"
    // label. We do NOT use fullSizeContentView / hideTitleBar — those
    // caused the macOS window controls to draw on top of content.
    standaloneWindow.setProperty({
      resizable: false,
      hudWindow: true,
      title: "Torrent Stream",
    });
    standaloneWindow.setFrame(380, 150, null, null);
    standaloneWindow.loadFile("progress.html");
  } else {
    // Magnet-input dialog: a plain panel (not HUD) so the text-field
    // background reads correctly, wide enough that long magnet URIs are
    // visible without horizontal scrolling.
    standaloneWindow.setProperty({
      resizable: false,
      hudWindow: false,
      title: "Open Magnet Link",
    });
    standaloneWindow.setFrame(640, 170, null, null);
    standaloneWindow.loadFile("magnet-input.html");
  }
  // loadFile clears all previously registered listeners; register fresh
  // ones AFTER it. This is the only correct ordering — registering before
  // loadFile silently drops every webview → plugin message.
  registerHandlersFor(mode);
}

function renderVisible(): void {
  if (!visibleSource) return;
  const entry = activeSources.get(visibleSource);
  if (!entry) return;
  standaloneWindow.postMessage("update", {
    source: visibleSource,
    status: entry.status,
  });
}

function startProgressPush(): void {
  if (progressPushTimer !== null) return;
  progressPushTimer = setInterval(() => renderVisible(), 400);
}

function stopProgressPush(): void {
  if (progressPushTimer !== null) {
    clearInterval(progressPushTimer);
    progressPushTimer = null;
  }
}

function showProgress(source: string): void {
  switchWindowMode("progress");
  if (!activeSources.has(source)) {
    activeSources.set(source, { status: null, pollTimer: null });
  }
  visibleSource = source;
  if (!progressOpen) {
    standaloneWindow.open();
    progressOpen = true;
  }
  renderVisible();
  startProgressPush();
}

function hideProgress(source: string): void {
  const entry = activeSources.get(source);
  if (entry) {
    if (entry.pollTimer !== null) {
      clearInterval(entry.pollTimer);
    }
    activeSources.delete(source);
  }
  if (visibleSource === source) {
    // Pick any remaining source to keep showing; or close if none.
    visibleSource = activeSources.keys().next().value ?? null;
    if (visibleSource) renderVisible();
  }
  if (activeSources.size === 0 && progressOpen) {
    standaloneWindow.close();
    progressOpen = false;
    windowMode = "none";
    stopProgressPush();
  }
}

/** Starts polling /torrents/{ih} for live stats and feeds the HUD. Idempotent
 * — a repeat start for the same source replaces the previous timer. */
function startProgressPolling(source: string, port: number, infohash: string): void {
  const entry = activeSources.get(source);
  if (!entry) return; // showProgress wasn't called (or hideProgress already ran)
  if (entry.pollTimer !== null) clearInterval(entry.pollTimer);
  const tick = async (): Promise<void> => {
    const status = await getStatus(port, infohash);
    const e = activeSources.get(source);
    if (!e) return; // hideProgress ran while the request was in flight
    if (status) {
      e.status = status;
      if (visibleSource === source) renderVisible();
    }
  };
  entry.pollTimer = setInterval(() => void tick(), 1000);
  void tick();
}

/**
 * Defence in depth against leftover bytes in the daemon's cache directory.
 * The daemon already purges its cache on exit, but in case that ever fails
 * (a crash, a kill, a race with anacrolix's shared file pool…) this catches
 * it: when no daemon is alive, any cache content is stale and safe to remove
 * with a recursive shell delete.
 *
 * Interlock against a LIVE daemon — including one mid-startup that has
 * already created the cache directory but not yet published its port — by
 * checking BOTH the lock file (created right after the lock is acquired,
 * before any cache work) AND the state file (written once the HTTP server
 * is listening). Looking at the state file alone leaves a multi-hundred-ms
 * window in NewEngine/net.Listen where the sweep would rm -rf the freshly
 * created cache of a perfectly healthy daemon.
 */
async function sweepOrphanedCache(): Promise<void> {
  try {
    if (file.exists("@data/torrentd.json") || file.exists("@data/torrentd.lock")) return;
    const cacheDir = utils.resolvePath("@tmp/torrent-cache");
    // Defence in depth: if utils.resolvePath ever returns something other
    // than the expected sandbox tmp path we DO NOT pass it to rm -rf.
    // An empty / very short / root-adjacent path here would be catastrophic.
    // Real IINA sandbox paths look like
    // "/Users/<u>/Library/Containers/.../tmp/.../torrent-cache" — well over
    // 30 chars and always ending in /torrent-cache.
    if (
      !cacheDir ||
      cacheDir.length < 30 ||
      cacheDir === "/" ||
      !cacheDir.endsWith("/torrent-cache")
    ) {
      diag(`sweepOrphanedCache: refusing suspicious path: ${cacheDir}`);
      return;
    }
    const res = await utils.exec("/bin/rm", ["-rf", cacheDir]);
    if (res.status !== 0) {
      diag(`sweepOrphanedCache: rm exit=${res.status} stderr=${res.stderr.trim()}`);
    }
  } catch (e) {
    diag(`sweepOrphanedCache failed: ${errorMessage(e)}`);
  }
}

/**
 * Holds a running daemon alive for the lifetime of the IINA process — even
 * when no player windows are open — so closing and re-opening a player window
 * resumes the same torrent without re-downloading. The global entry is loaded
 * once per IINA launch and torn down with IINA itself, so heartbeats stop
 * exactly when IINA quits; the daemon then idles out (idleTimeout, 30s) and
 * purges its cache as it always did. Reads the port from the state file
 * rather than caching it, so a daemon that died and came back on a different
 * port is picked up automatically.
 */
async function keepDaemonAlive(): Promise<void> {
  try {
    if (!file.exists("@data/torrentd.json")) return;
    const text = file.read("@data/torrentd.json");
    if (!text) return;
    const state = JSON.parse(text) as { port?: number };
    if (typeof state.port === "number" && state.port > 0) {
      await heartbeat(state.port);
    }
  } catch {
    // Non-fatal: a missing or unreadable state file means there is no daemon
    // to keep alive right now; the next tick will retry.
  }
}

/**
 * Opens a torrent source in a new player window without first waiting for
 * metadata. The window is created immediately with the daemon's all-in-one
 * /play URL — mpv connects, the daemon adds the magnet and blocks the stream
 * until metadata arrives, mpv shows its buffering UI in the meantime. In
 * parallel we resolve metadata via POST /torrents so the main entry can be
 * told the infohash and attach its sidebar/subtitles/heartbeat tracking.
 */
async function resolveAndOpen(source: string): Promise<void> {
  let port: number | null = null;
  let failure: string | null = null;

  // Show the progress HUD up-front so the user always sees an acknowledgement
  // even when ensureRunning has to spawn the daemon for the first time.
  onPluginQueue(() => showProgress(source));

  try {
    port = await ensureRunning();
  } catch (e) {
    failure = errorMessage(e);
    try {
      diag(`global: raw error: ${JSON.stringify(e)}`);
    } catch {
      /* circular or unserialisable */
    }
  }

  // Open the window first — it's the user-visible feedback, and any metadata
  // delay should look like buffering inside a real player window, not a
  // frozen menu.
  const playerId = await new Promise<number | null>((resolve) => {
    onPluginQueue(() => {
      if (port === null) {
        diag(`global: ERROR ${failure}`);
        hideProgress(source);
        utils.ask(`Torrent Stream error: ${failure}`);
        resolve(null);
        return;
      }
      const id = global.createPlayerInstance({
        url: playURL(port, source),
        enablePlugins: true,
      });
      diag(`global: opened player ${id} with /play URL`);
      resolve(id);
    });
  });

  if (port === null || playerId === null) return;

  // Resolve metadata in the background and notify the new window so it can
  // attach the sidebar, subtitles, and heartbeat. addTorrent is idempotent
  // for an already-added torrent (anacrolix returns the existing one) so
  // this races safely with the /play handler's own Add call.
  //
  // The cancel callback flips true once hideProgress removes the source from
  // activeSources — which happens on window-will-close (via ts-progress-close)
  // and on any other dismissal. That stops addTorrent's metadata polling so
  // we are not heartbeating the daemon on behalf of a window the user has
  // already abandoned.
  try {
    const info = await addTorrent(port, source, () => !activeSources.has(source));
    // We have the infohash now — start feeding live stats into the HUD.
    onPluginQueue(() => startProgressPolling(source, port!, info.infohash));
    // `source` tells the new window's main entry which progress HUD entry
    // it should close once playback actually starts. For the on_load
    // intercept path the main entry already owns the source (it's the
    // string it intercepted), but for this menu path it had no way to
    // know the original magnet/.torrent URL.
    global.postMessage(playerId, "ts-attach", { port, infohash: info.infohash, source });
    diag(`global: sent ts-attach to player ${playerId}`);
  } catch (e) {
    diag(`global: addTorrent failed: ${errorMessage(e)}`);
    onPluginQueue(() => hideProgress(source));
  }
  // Note: we do NOT hideProgress in the success case here. The HUD stays
  // open until the new player window's main entry reports playback started
  // via ts-progress-close, which gives the user the live stats up to that
  // moment instead of closing the moment metadata arrives.
}

// --- Magnet input dialog ----------------------------------------------------
//
// Replaces IINA's built-in utils.prompt for the "Open Magnet Link…" menu.
// utils.prompt offers no way to widen the input field, so long magnet URIs
// scroll off the edge invisibly. This custom standaloneWindow dialog is
// 640px wide and prefills from the clipboard if it holds a torrent-looking
// URL — which is the common case (user just copied a magnet from a browser).
//
// Only one dialog can be in flight at a time (standaloneWindow is a
// singleton). pendingMagnetResolve is non-null exactly while one is open.

let pendingMagnetResolve: ((value: string | null) => void) | null = null;

function finishMagnetDialog(value: string | null): void {
  if (!pendingMagnetResolve) {
    diag("finishMagnetDialog: no pending resolver, ignoring");
    return;
  }
  const resolve = pendingMagnetResolve;
  pendingMagnetResolve = null;
  diag(`finishMagnetDialog: resolving with ${value === null ? "<null>" : `value len=${value.length}`}`);
  onPluginQueue(() => {
    standaloneWindow.close();
    windowMode = "none";
  });
  resolve(value);
}

function onMagnetSubmit(value: string): void {
  finishMagnetDialog(value.trim() || null);
}

function onMagnetCancel(): void {
  finishMagnetDialog(null);
}

/** Reads the system clipboard and returns its contents if they look like a
 * torrent source. Returns null otherwise (or on any error). WKWebView in
 * IINA's standalone windows denies navigator.clipboard, so we read via
 * pbpaste from the plugin side and push the result over postMessage. */
async function readClipboardTorrent(): Promise<string | null> {
  try {
    const { status, stdout } = await utils.exec("/usr/bin/pbpaste", []);
    if (status !== 0) return null;
    const text = stdout.trim();
    if (!text) return null;
    if (text.toLowerCase().startsWith("magnet:")) return text;
    if (/^https?:\/\/\S+\.torrent(\?\S*)?$/i.test(text)) return text;
    return null;
  } catch {
    return null;
  }
}

async function onMagnetReady(): Promise<void> {
  // Only push prefill if a dialog is actually waiting — a stray "ready"
  // from a previously closed window would otherwise stomp on no one.
  if (!pendingMagnetResolve) return;
  const prefill = await readClipboardTorrent();
  if (!prefill) return;
  // Re-check after the await: finishMagnetDialog may have run while
  // pbpaste was in flight.
  if (pendingMagnetResolve === null) return;
  onPluginQueue(() => standaloneWindow.postMessage("prefill", prefill));
}

/** Opens the magnet-input dialog and resolves with the entered value, or
 * null if the user cancelled. Rejects no error path — the menu handler
 * just no-ops on null. */
function showMagnetInputDialog(): Promise<string | null> {
  // If a previous dialog is somehow still pending, cancel it so this one
  // takes over cleanly. In practice this can't happen because the menu
  // item is the only entry point and clicks are serialised.
  if (pendingMagnetResolve) {
    const old = pendingMagnetResolve;
    pendingMagnetResolve = null;
    old(null);
  }
  return new Promise((resolve) => {
    pendingMagnetResolve = resolve;
    onPluginQueue(() => {
      switchWindowMode("magnet-input");
      standaloneWindow.open();
    });
  });
}

// utils.chooseFile presents a native dialog and resolves asynchronously at
// runtime, even though the IINA type definitions declare a synchronous
// return — so its result must be awaited.
async function openMagnetLink(): Promise<void> {
  diag("menu: Open Magnet Link clicked");
  const source = await showMagnetInputDialog();
  diag(`openMagnetLink: dialog resolved with ${source ? `source len=${source.length}` : "<null/empty>"}`);
  if (source) await resolveAndOpen(source);
}

async function openTorrentFile(): Promise<void> {
  diag("menu: Open Torrent File clicked");
  const path = await Promise.resolve(
    utils.chooseFile("Select a .torrent file", { allowedFileTypes: ["torrent"] }),
  );
  if (path) await resolveAndOpen(path);
}

menu.addItem(menu.item("Open Magnet Link…", () => void openMagnetLink()));
menu.addItem(menu.item("Open Torrent File…", () => void openTorrentFile()));

// Messages from a main entry that detected a magnet/.torrent in its on_load
// hook (Welcome screen, Recent Files, drag-drop) and bounced it here so the
// torrent can be resolved without blocking that window's mpv pipeline.
global.onMessage("ts-open-source", (data) => {
  diag(`ts-open-source received: ${JSON.stringify(data)}`);
  if (data && typeof data.source === "string") {
    void resolveAndOpen(data.source);
  }
});

// A main entry asks us to show the progress HUD while it resolves the
// torrent in-place (the recent-files path, where on_load substituted a
// /play URL into the existing window and IINA's player NSWindow won't
// un-hide until the first decoded frame). The HUD bridges the gap.
global.onMessage("ts-progress-open", (data) => {
  if (data && typeof data.source === "string") {
    onPluginQueue(() => showProgress(data.source));
  }
});
// Main entry tells us the infohash once addTorrent comes back, so we can
// start feeding live download stats into the HUD.
global.onMessage("ts-progress-track", (data) => {
  if (
    data &&
    typeof data.source === "string" &&
    typeof data.port === "number" &&
    typeof data.infohash === "string"
  ) {
    onPluginQueue(() => startProgressPolling(data.source, data.port, data.infohash));
  }
});
global.onMessage("ts-progress-close", (data) => {
  if (data && typeof data.source === "string") {
    onPluginQueue(() => hideProgress(data.source));
  }
});

// Sweep any orphaned cache from a daemon that exited without cleaning up.
// Runs at startup and once a minute thereafter so the cache never lingers
// for more than ~60s after the daemon dies.
void sweepOrphanedCache();
setInterval(() => void sweepOrphanedCache(), 60_000);
// Start the daemon eagerly so the main entry's on_load can use the /play URL
// from the very first torrent — without this, the first magnet opened from
// IINA's Welcome / Recent / drag-drop falls back to opening a fresh window
// via the global entry. The await is fire-and-forget; failures surface via
// the normal /play / addTorrent error paths once a torrent is opened.
void ensureRunning().catch((e) => diag(`eager ensureRunning failed: ${errorMessage(e)}`));
// Keep the daemon alive for the lifetime of the IINA process. Daemon's
// idleTimeout is 30s, so anything well under that works; 10s gives plenty
// of slack against a momentarily-blocked event loop.
setInterval(() => void keepDaemonAlive(), 10_000);
diag("global.ts entry loaded");
