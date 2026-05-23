// index.ts — Main (per-player) entry. Two responsibilities:
//   1. Intercept torrent sources (magnet / .torrent) opened directly into an
//      existing window and redirect mpv to a daemon stream URL (on_load hook).
//   2. Attach the Torrent sidebar with live progress once the torrent for this
//      window is known (assigned by the global entry via a "ts-attach" message).
//
// Ordering matters: sidebar APIs must not be touched before the window is
// loaded (IINA raises an error otherwise, which would abort this whole entry).
// So mpv/message listeners are registered at load time, and everything sidebar
// is deferred to the iina.window-loaded event.

import {
  getStatus,
  heartbeat,
  streamURL,
  pauseTorrent,
  resumeTorrent,
  prewarmFile,
  playURL,
  readDaemonState,
  addTorrent,
} from "./daemon";
import type { TorrentStatus } from "./daemon";
import {
  isTorrentSource,
  subtitleFiles,
  videoFiles,
  fileBaseName,
} from "./torrent";
import { opt } from "./options";
import { diag } from "./log";
import { errorMessage, onPluginQueue } from "./util";

const { core, console, mpv, event, sidebar, global, playlist } = iina;

diag("index.ts main entry loaded");

const HEARTBEAT_INTERVAL = 10_000;
const STATUS_INTERVAL = 1_500;
// attachTracking initial-status retry schedule. Exponential so a transient
// hiccup recovers fast, but a longer outage still gets ~5s of total budget.
// Total ≈ 500 + 1000 + 2000 + 4000 + 8000 = 15.5s spread over 5 attempts.
const ATTACH_RETRY_BASE_MS = 500;

/** State for the torrent currently tracked in this player window. */
interface ActiveTorrent {
  port: number;
  infohash: string;
  info: TorrentStatus;
  multiVideo: boolean;
}

let active: ActiveTorrent | null = null;
let attaching = false;
let heartbeatTimer: string | null = null;
let statusTimer: string | null = null;
// Latched by iina.window-will-close. Used by attachTracking's deferred
// onPluginQueue callback to refuse to start timers on a window that has
// already closed — otherwise the timers would never be cleared and would
// keep heart-beating the daemon for the lifetime of IINA.
let windowClosed = false;

let sidebarReady = false;
let pendingShow = false;
// True once the user has seen the sidebar (auto-show or manual reveal). Until
// then the status poller short-circuits — there is no UI consuming the data,
// so polling every 1.5s is wasted bandwidth and daemon CPU. We can't observe
// hide events from IINA, so once revealed we keep polling for the rest of
// the window's lifetime.
let sidebarRevealed = false;

// Streaming mpv buffer options that have to be in place BEFORE mpv starts
// pulling bytes from the daemon — otherwise the first seconds of playback
// run on mpv's default tiny buffer and stall the moment the swarm hiccups.
// Applied per stream from on_load when we recognise a torrent stream URL,
// rather than at module load: that way a user's mpv.conf is only overridden
// for windows we actually own. force-window=immediate is deliberately NOT
// set — IINA's player window is an NSWindow wrapper around MPVView that
// ignores that mpv option.
const DAEMON_URL_RE = /^http:\/\/127\.0\.0\.1:\d+\/(play|stream)(\/|\?|$)/;
let mpvBuffersApplied = false;
function applyTorrentMpvBuffers(): void {
  if (mpvBuffersApplied) return;
  mpvBuffersApplied = true;
  mpv.set("cache", "yes");
  mpv.set("demuxer-max-bytes", `${opt.demuxerMaxBytesMiB}MiB`);
  mpv.set("cache-secs", "30");
}

/** Reveals the sidebar, deferring if it is not loaded yet. */
function showSidebar(): void {
  sidebarRevealed = true;
  if (sidebarReady) {
    sidebar.show();
  } else {
    pendingShow = true;
  }
}

function clearTimers(): void {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  if (statusTimer) {
    clearInterval(statusTimer);
    statusTimer = null;
  }
}

/** Starts the heartbeat and status-polling timers for an active torrent. */
function startTracking(port: number, info: TorrentStatus, multiVideo: boolean): void {
  clearTimers();
  active = { port, infohash: info.infohash, info, multiVideo };
  diag(`tracking started for ${info.name}`);

  heartbeatTimer = setInterval(() => {
    heartbeat(port);
  }, HEARTBEAT_INTERVAL);

  // Status polling is gated on whether the user has actually revealed the
  // sidebar — until then the response would be discarded anyway, and on a
  // multi-window setup the per-window pollers would all hit the daemon
  // every 1.5s for nothing. The heartbeat above (10s) keeps the daemon
  // from idling out regardless.
  statusTimer = setInterval(async () => {
    if (!active || !sidebarRevealed) return;
    const status = await getStatus(active.port, active.infohash);
    if (status && sidebarReady) {
      onPluginQueue(() => sidebar.postMessage("status", status));
    }
  }, STATUS_INTERVAL);
}

/** Stops all tracking and clears the active-torrent state. */
function stopTracking(): void {
  clearTimers();
  active = null;
}

/**
 * Attaches the sidebar and live tracking for a torrent. Reached either from a
 * "ts-attach" message (menu windows) or from the on_load hook (magnet opened
 * directly into a window).
 */
async function attachTracking(port: number, infohash: string): Promise<void> {
  if (active && active.infohash === infohash) return;
  if (attaching) {
    diag(`attachTracking: already attaching, ignoring ih=${infohash}`);
    return;
  }
  attaching = true;
  diag(`attachTracking port=${port} ih=${infohash}`);
  try {
    // Retry transient null responses with exponential backoff. getStatus
    // swallows every HTTP/network error into null (see daemon.ts), so a
    // single hiccup at this critical moment used to silently leave the
    // window with no sidebar, no heartbeats, and no pause-mirroring for
    // the rest of playback — and the daemon then idle-shut-down 30s
    // later, mid-stream. The schedule (500ms × 2^attempt) recovers fast
    // from a brief stall but still burns ~15s of total budget on a real
    // outage before giving up.
    let status: TorrentStatus | null = null;
    for (let attempt = 0; attempt < 5; attempt++) {
      if (windowClosed) {
        diag("attachTracking: window closed during retry loop, aborting");
        return;
      }
      status = await getStatus(port, infohash);
      if (status) break;
      const delay = ATTACH_RETRY_BASE_MS * Math.pow(2, attempt);
      await new Promise<void>((r) => setTimeout(r, delay));
    }
    if (!status) {
      diag("attachTracking: status unavailable after retries");
      return;
    }
    if (windowClosed) {
      diag("attachTracking: window closed after status fetched, aborting");
      return;
    }
    const finalStatus = status;
    const videos = videoFiles(finalStatus);
    const multiVideo = videos.length > 1;

    // Pre-warm subtitle files in the daemon so their pieces are downloaded
    // and anacrolix renames "<file>.part" → "<file>" on disk. IINA's
    // core.subtitle.loadTrack rejects HTTP URLs ("Unsupported external
    // subtitles"); pointing it at the daemon's already-on-disk torrent cache
    // file is what works. A multi-video torrent is opened as a .m3u playlist
    // so IINA owns subtitle attachment there — only single-file torrents
    // attach sibling subtitle files here.
    let localSubtitlePaths: string[] = [];
    if (!multiVideo) {
      const subs = subtitleFiles(finalStatus);
      if (subs.length > 0) {
        diag(`prewarming ${subs.length} subtitle file(s)`);
        const results = await Promise.all(
          subs.map(async (sub) => {
            const path = await prewarmFile(port, infohash, sub.index);
            if (!path) console.warn(`could not prewarm subtitle ${sub.path}`);
            return path;
          }),
        );
        localSubtitlePaths = results.filter((p): p is string => p !== null);
      }
    }

    onPluginQueue(() => {
      if (windowClosed) {
        // window-will-close fired while we were awaiting getStatus. Don't
        // schedule timers — there's no window-will-close left to clear them.
        diag("attachTracking: window already closed, skipping startTracking");
        return;
      }
      startTracking(port, finalStatus, multiVideo);
      if (opt.autoShowSidebar) showSidebar();
      if (sidebarReady) sidebar.postMessage("status", finalStatus);
      for (const subPath of localSubtitlePaths) {
        try {
          core.subtitle.loadTrack(subPath);
        } catch (e) {
          console.warn(`could not attach subtitle ${subPath}: ${errorMessage(e)}`);
        }
      }
    });
  } finally {
    attaching = false;
  }
}

// --- Load-time registrations (must all run; no window-dependent calls here) ---

// Intercept magnet/.torrent opened directly into this window — typically from
// IINA's Welcome screen, Recent Files, or a magnet:/.torrent drop. Swap the
// unplayable URL for the daemon's all-in-one /play URL: mpv connects to it
// immediately, sees buffering while the daemon waits for metadata, and starts
// playing once the primary file is selected. No need to block on_load and no
// "cannot open URL" flash. If the daemon is not running yet (cold IINA
// start — rare since the global entry starts it eagerly) we fall back to
// forwarding to the global entry, which will open a fresh window.
// Source currently being resolved in-place for this window. The global entry
// owns the progress HUD; we tell it which source we are working on so it can
// close the HUD once we observe real playback in this window.
let pendingSource: string | null = null;

function closePendingProgress(): void {
  if (pendingSource !== null) {
    global.postMessage("ts-progress-close", { source: pendingSource });
    pendingSource = null;
  }
  if (firstFramePoller !== null) {
    clearInterval(firstFramePoller);
    firstFramePoller = null;
  }
}

// Poller that watches for the moment the player window actually appears.
// IINA un-hides the player NSWindow only once mpv has rendered a first
// decoded frame, so core.window.visible is the most reliable signal — none
// of the mpv events tried first (iina.file-started, mpv.playback-time,
// mpv.video-out-params) coincided with the visible un-hide: they all fire
// earlier, on input bytes / property bookkeeping / decode setup.
let firstFramePoller: string | null = null;

/** Start a poller that closes the global entry's progress HUD as soon as
 * this player window becomes visible (i.e. mpv has decoded a first frame).
 * Called from two places: the on_load hook that intercepted a magnet/
 * .torrent source, and the ts-attach handler that runs in menu-opened
 * windows (where on_load only sees the daemon's /play URL and so couldn't
 * own the HUD-close itself). */
function startFirstFramePoller(source: string): void {
  pendingSource = source;
  if (firstFramePoller !== null) clearInterval(firstFramePoller);
  firstFramePoller = setInterval(() => {
    if (core.window.visible) {
      diag("progress: player window visible, closing HUD");
      closePendingProgress();
    }
  }, 150);
}

mpv.addHook("on_load", 10, (next) => {
  const source = mpv.getString("stream-open-filename");
  diag(`on_load: ${source}`);

  // Apply our streaming buffer settings only when this window is actually
  // playing torrent content — either a magnet/.torrent we are about to
  // rewrite, or a /play|/stream URL the global entry already pointed mpv
  // at. Module-level mpv.set was overwriting user mpv.conf for every IINA
  // window even when no torrent was involved.
  if (isTorrentSource(source) || DAEMON_URL_RE.test(source)) {
    applyTorrentMpvBuffers();
  }

  if (isTorrentSource(source)) {
    const state = readDaemonState();
    if (state) {
      mpv.set("stream-open-filename", playURL(state.port, source));
      global.postMessage("ts-progress-open", { source });
      startFirstFramePoller(source);
      void attachAfterResolve(state.port, source);
    } else {
      diag("on_load: daemon not running yet, forwarding to global");
      global.postMessage("ts-open-source", { source });
    }
  }
  next?.();
});

/** Background resolution for the main entry's on_load path: addTorrent gets
 * us the infohash (anacrolix returns the existing torrent for an already-
 * added one, so this races safely with the /play handler's own Add), then
 * attachTracking wires up the sidebar, subtitles, and heartbeat. We also
 * tell the global entry the infohash so its progress HUD can start showing
 * live download stats.
 *
 * Cancellation: addTorrent polls the daemon for metadata for up to ~90s.
 * If the user closes the window while that's in flight, we tell addTorrent
 * to abort so the daemon stops being polled (and so the polling does not
 * keep the daemon's idle timer touched, holding it alive for nothing). */
async function attachAfterResolve(port: number, source: string): Promise<void> {
  try {
    const info = await addTorrent(port, source, () => windowClosed);
    if (windowClosed) {
      diag("attachAfterResolve: window closed during addTorrent, skipping attach");
      return;
    }
    global.postMessage("ts-progress-track", { source, port, infohash: info.infohash });
    void attachTracking(port, info.infohash);
  } catch (e) {
    diag(`attachAfterResolve ERROR: ${errorMessage(e)}`);
  }
}

// Receive the torrent assignment from the global entry (menu-opened windows).
// The global entry owns the progress HUD for this source; we are the only
// one who can tell when playback actually started in this window, so we
// start the first-frame poller too. (The on_load path above already did
// this for the magnet/.torrent-intercept case.)
global.onMessage("ts-attach", (data) => {
  diag(`ts-attach received: ${JSON.stringify(data)}`);
  if (data && typeof data.port === "number" && typeof data.infohash === "string") {
    if (typeof data.source === "string" && data.source && pendingSource === null) {
      startFirstFramePoller(data.source);
    }
    void attachTracking(data.port, data.infohash);
  }
});

event.on("iina.window-will-close", () => {
  // Stop heart-beating and polling the daemon. Once every window has closed the
  // daemon receives no more activity, idles out, and purges its disk cache —
  // see the daemon's lifecycle.monitor. No explicit "release" call is needed.
  windowClosed = true;
  stopTracking();
  // If the window is closed before playback ever started (user gave up while
  // waiting), make sure we don't leave the progress HUD hanging — and detach
  // the playback-time listener so it can't fire after window teardown.
  closePendingProgress();
});

// Mirror mpv's pause state into the daemon so it stops prefetching pieces
// while the viewer is paused (and starts again on resume). Avoids burning
// bandwidth on a movie that may never be unpaused.
event.on("mpv.pause.changed", () => {
  if (!active) return;
  const paused = mpv.getFlag("pause");
  diag(`pause-changed: ${paused}`);
  if (paused) {
    void pauseTorrent(active.port, active.infohash);
  } else {
    void resumeTorrent(active.port, active.infohash);
  }
});

// --- Sidebar setup, deferred until the window is loaded -----------------------

function initSidebar(): void {
  if (sidebarReady) return;
  sidebarReady = true;
  diag("initSidebar: loading sidebar.html");
  sidebar.loadFile("sidebar.html");

  sidebar.onMessage("switchFile", (index: number) => {
    if (!active) return;
    const target = active.info.files[index];
    if (!target) return;
    const url = streamURL(active.port, active.infohash, index, fileBaseName(target.path));
    // For a multi-video torrent (an mpv playlist) navigate within the playlist
    // so next/previous keep working; otherwise just open the file.
    if (active.multiVideo) {
      const at = playlist.list().findIndex((it) => it.filename === url);
      if (at >= 0) {
        playlist.play(at);
      } else {
        // Do NOT fall through to core.open(url) — that would replace the
        // m3u-driven playlist with a single stream URL, destroying the
        // next/previous navigation for the rest of the episodes.
        diag(`switchFile: URL not in playlist, refusing to break playlist: ${url}`);
      }
      return;
    }
    core.open(url);
  });

  sidebar.onMessage("requestStatus", async () => {
    // The sidebar webview calls requestStatus on load — so the first time
    // we see this message, the user has the sidebar visible (either via
    // showSidebar() or by clicking its tab themselves). Flip the gate so
    // the status poller starts producing updates.
    sidebarRevealed = true;
    if (!active) return;
    const status = await getStatus(active.port, active.infohash);
    if (status) onPluginQueue(() => sidebar.postMessage("status", status));
  });

  if (pendingShow) {
    pendingShow = false;
    sidebar.show();
  }
}

if (core.window.loaded) {
  initSidebar();
} else {
  event.on("iina.window-loaded", initSidebar);
}
