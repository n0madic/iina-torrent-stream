// daemon.ts — manages the native `torrentd` companion process: downloading the
// binary on first use, spawning it, discovering its port, and talking to its
// localhost HTTP API.
//
// IINA plugins run in a sandboxed JavaScriptCore engine with no sockets, so the
// actual torrent engine must live in this separate native process.
//
// This module avoids the main-entry-only `core` API so it can be used from both
// the main and the global plugin entries.

import { opt } from "./options";
import { diag } from "./log";
import type { CancelFn } from "./util";

const { http, utils, file, console } = iina;

// --- Configuration ----------------------------------------------------------

// GitHub repository hosting the torrentd release binaries. Release assets must
// be named `torrentd-darwin-arm64` and `torrentd-darwin-x86_64`.
const DAEMON_REPO = "n0madic/iina-torrent-stream";
const DAEMON_VERSION = "v0.1.1";

const BIN_PATH = "@data/torrentd";
const STATE_PATH = "@data/torrentd.json";
// How long the daemon waits, with no heartbeat or status poll from any player
// window, before it shuts down and purges its disk cache. Kept short so the
// cache is reclaimed promptly once the last torrent window closes.
const IDLE_TIMEOUT = "30s";

// SHA-256 checksums of the release binaries, injected at release time by the
// GitHub Actions workflow (.github/workflows/release.yml). Empty values skip
// verification, which is the case for local and source builds.
const EXPECTED_SHA256: Record<string, string> = {
  arm64: "",
  x86_64: "",
};

// --- Types ------------------------------------------------------------------

/** A single file inside a torrent, as reported by the daemon. */
export interface FileStatus {
  index: number;
  path: string;
  length: number;
  bytesCompleted: number;
  mime: string;
  isVideo: boolean;
  isSubtitle: boolean;
}

/** A torrent status snapshot, as reported by the daemon. */
export interface TorrentStatus {
  infohash: string;
  name: string;
  length: number;
  bytesCompleted: number;
  progress: number;
  downloadRate: number;
  uploadRate: number;
  peers: number;
  activePeers: number;
  seeders: number;
  pieceCount: number;
  primaryIndex: number;
  files: FileStatus[];
}

interface DaemonState {
  port: number;
  pid: number;
  version: string;
}

// --- Helpers ----------------------------------------------------------------

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function errorText(res: IINA.HTTPResponse): string {
  // Prefer the daemon's JSON error envelope when present.
  try {
    const parsed = JSON.parse(res.text);
    if (parsed && parsed.error) return String(parsed.error);
  } catch {
    // not JSON; surface the raw body (trimmed) instead of an opaque code so
    // plain-text errors from upstream proxies / mpv don't show as "HTTP 502"
    // with no detail.
  }
  const body = (res.text ?? "").trim();
  if (body) {
    const snippet = body.length > 200 ? `${body.slice(0, 200)}…` : body;
    return `HTTP ${res.statusCode}: ${snippet}`;
  }
  return `HTTP ${res.statusCode}`;
}

// --- Binary management ------------------------------------------------------

/** Detects the host CPU architecture as used in the release asset names.
 * Throws on anything other than arm64 / x86_64 — silently mapping unknowns
 * to x86_64 produced "torrent engine did not start in time" errors with no
 * useful diagnosis when the user was on an unsupported platform. */
async function detectArch(): Promise<string> {
  const { status, stdout } = await utils.exec("/usr/bin/uname", ["-m"]);
  if (status !== 0) {
    throw new Error("could not detect CPU architecture");
  }
  const arch = stdout.trim();
  if (arch === "arm64") return "arm64";
  if (arch === "x86_64") return "x86_64";
  throw new Error(`unsupported CPU architecture: ${arch || "<empty>"}`);
}

/** Verifies the downloaded binary against a known checksum, if one is set. */
async function verifyChecksum(resolvedPath: string, arch: string): Promise<void> {
  const expected = EXPECTED_SHA256[arch];
  if (!expected) {
    diag(`no expected SHA-256 for ${arch}, skipping verification`);
    return;
  }
  const { status, stdout } = await utils.exec("/usr/bin/shasum", [
    "-a",
    "256",
    resolvedPath,
  ]);
  if (status !== 0) {
    // Delete the unverified binary too — otherwise the next ensureBinary
    // call sees the file already on disk and short-circuits past the whole
    // verification step, silently shipping an unchecked executable.
    file.delete(BIN_PATH);
    throw new Error("could not compute the binary checksum");
  }
  const actual = stdout.trim().split(/\s+/)[0].toLowerCase();
  if (actual !== expected.toLowerCase()) {
    file.delete(BIN_PATH);
    throw new Error("torrentd checksum mismatch — the download may be corrupt");
  }
}

/** Probes the on-disk binary's reported version. Returns null if the binary
 * cannot be invoked (e.g. permission, wrong arch). */
async function probeBinaryVersion(resolved: string): Promise<string | null> {
  try {
    const { status, stdout } = await utils.exec(resolved, ["--version"]);
    if (status !== 0) return null;
    return stdout.trim();
  } catch {
    return null;
  }
}

/** Downloads, verifies, and prepares the daemon binary at BIN_PATH. The
 * partial-file cleanup is critical: a half-written binary left from a failed
 * download would otherwise be picked up by the next ensureBinary() call as
 * "already present" and shipped to the user unchecked. */
async function downloadFreshBinary(arch: string): Promise<string> {
  const asset = `torrentd-darwin-${arch}`;
  const url = `https://github.com/${DAEMON_REPO}/releases/download/${DAEMON_VERSION}/${asset}`;
  diag(`downloading ${url}`);

  try {
    await http.download(url, BIN_PATH);
  } catch (e) {
    // A partial file MUST be removed — otherwise the next ensureBinary
    // call sees BIN_PATH "present" and short-circuits past the download
    // and checksum verification, silently running a truncated binary.
    try {
      file.delete(BIN_PATH);
    } catch {
      /* best-effort */
    }
    throw e;
  }

  const resolved = utils.resolvePath(BIN_PATH);

  try {
    await verifyChecksum(resolved, arch);
    await utils.exec("/bin/chmod", ["+x", resolved]);
    await utils.exec("/usr/bin/xattr", ["-dr", "com.apple.quarantine", resolved]);
  } catch (e) {
    // Same logic: if anything in the post-download setup fails we cannot
    // leave a binary on disk that the next call would treat as "ready".
    try {
      file.delete(BIN_PATH);
    } catch {
      /* best-effort */
    }
    throw e;
  }

  diag(`daemon binary ready: ${resolved}`);
  return resolved;
}

/**
 * Ensures the daemon binary exists locally, downloading it from GitHub releases
 * on first use. Returns the resolved absolute path to the executable.
 *
 * Also handles the "plugin updated, binary stale" case: a `torrentd --version`
 * probe compares the on-disk binary's version against DAEMON_VERSION (which
 * the release workflow injects). On mismatch the binary is removed and a
 * fresh one is downloaded. Dev builds (binaries reporting "dev") are accepted
 * unconditionally so `make dev-daemon` keeps working without round-tripping
 * through a GitHub release.
 */
async function ensureBinary(): Promise<string> {
  const arch = await detectArch();

  if (utils.fileInPath(BIN_PATH)) {
    const resolved = utils.resolvePath(BIN_PATH);
    const installedVersion = await probeBinaryVersion(resolved);
    const expectedVersion = DAEMON_VERSION.replace(/^v/, "");

    if (installedVersion === null) {
      // Could not run the binary at all — treat it as broken and re-download.
      diag("daemon binary present but --version failed; re-downloading");
      try {
        file.delete(BIN_PATH);
      } catch {
        /* best-effort */
      }
    } else if (installedVersion === "dev") {
      diag(`daemon binary is a dev build, skipping version check (${resolved})`);
      return resolved;
    } else if (installedVersion !== expectedVersion) {
      diag(
        `daemon binary version ${installedVersion} != expected ${expectedVersion} — re-downloading`,
      );
      try {
        file.delete(BIN_PATH);
      } catch {
        /* best-effort */
      }
    } else {
      diag(`daemon binary present (v${installedVersion}): ${resolved}`);
      return resolved;
    }
  }

  diag("daemon binary missing or stale — downloading");
  return await downloadFreshBinary(arch);
}

// --- Lifecycle --------------------------------------------------------------

let cachedPort: number | null = null;

/** Reads the daemon's state file, or returns null if it is absent/invalid.
 * Exported so the main entry can find the running daemon's port without
 * having to spawn one itself (only the global entry should do that). */
export function readDaemonState(): DaemonState | null {
  return readState();
}

function readState(): DaemonState | null {
  try {
    if (!file.exists(STATE_PATH)) return null;
    const text = file.read(STATE_PATH);
    if (!text) return null;
    const state = JSON.parse(text) as DaemonState;
    return typeof state.port === "number" && state.port > 0 ? state : null;
  } catch {
    return null;
  }
}

/** Returns true if a daemon answers /health on the given port. */
async function isHealthy(port: number): Promise<boolean> {
  try {
    const res = await http.get(`http://127.0.0.1:${port}/health`, {} as any);
    return res.statusCode === 200;
  } catch {
    return false;
  }
}

/**
 * Spawns the daemon and waits for it to become reachable. The daemon enforces
 * a single instance per data directory via a lock file, so spawning a second
 * time simply attaches to the already-running one.
 */
async function spawnDaemon(): Promise<number> {
  const bin = await ensureBinary();
  // IINA's parsePath only recognises the prefix form with a trailing slash
  // ("@data/"), not a bare "@data".
  const dataDir = utils.resolvePath("@data/");
  const cacheDir = utils.resolvePath("@tmp/torrent-cache");
  const stateFile = utils.resolvePath(STATE_PATH);

  const args = [
    `--data-dir=${dataDir}`,
    `--cache-dir=${cacheDir}`,
    `--state-file=${stateFile}`,
    `--readahead-mib=${opt.readaheadMiB}`,
    `--seed=${opt.seeding}`,
    `--idle-timeout=${IDLE_TIMEOUT}`,
  ];
  // Diagnostic endpoints are opt-in (preference checkbox in pref.html).
  // Only pass the flag when enabled — older daemon binaries that lack the
  // flag would otherwise refuse to start with an "unknown flag" error.
  if (opt.debugEndpoints) {
    args.push("--debug-endpoints=true");
  }
  diag(`spawning daemon: ${bin} ${args.join(" ")}`);

  // `utils.exec` resolves only when the process exits. The daemon is
  // long-lived, so we deliberately do not await it; we poll for readiness
  // instead and use the resolved promise to detect an unexpected exit —
  // which short-circuits the polling loop so we report the actual reason
  // immediately instead of after the full 15-second timeout.
  let daemonExited = false;
  let exitInfo = "";

  utils
    // Daemon stderr goes to the IINA console only — not the diagnostic file —
    // so verbose engine logging cannot flood and truncate our own diag lines.
    .exec(bin, args, null, null, (errData) => console.log(`[torrentd] ${errData.trim()}`))
    .then((res) => {
      daemonExited = true;
      if (res.status !== 0) {
        exitInfo = `status ${res.status}: ${res.stderr.trim()}`;
        diag(`daemon exited with ${exitInfo}`);
      } else {
        exitInfo = "exit 0";
        diag("daemon process exited");
      }
    })
    .catch((e) => {
      daemonExited = true;
      exitInfo = String(e);
      diag(`daemon exec error: ${e}`);
    });

  // Poll the state file and /health until the daemon is ready (~15s budget).
  for (let i = 0; i < 30; i++) {
    await sleep(500);
    if (daemonExited) {
      throw new Error(`torrent engine exited before becoming ready (${exitInfo})`);
    }
    const state = readState();
    if (state && (await isHealthy(state.port))) {
      diag(`daemon ready on port ${state.port}`);
      return state.port;
    }
  }
  throw new Error("the torrent engine did not start in time");
}

/** Returns the port of a running daemon, starting one if necessary. */
export async function ensureRunning(): Promise<number> {
  if (cachedPort !== null && (await isHealthy(cachedPort))) {
    return cachedPort;
  }
  const existing = readState();
  if (existing && (await isHealthy(existing.port))) {
    diag(`attached to running daemon on port ${existing.port}`);
    cachedPort = existing.port;
    return cachedPort;
  }
  cachedPort = await spawnDaemon();
  return cachedPort;
}

// --- HTTP API ---------------------------------------------------------------

/** How long the plugin will keep polling /torrents/{ih} for metadata after a
 * successful POST /torrents before giving up. Generous: a sparsely-seeded
 * magnet can need a minute or more for the .torrent info to arrive via DHT. */
const METADATA_POLL_TIMEOUT_MS = 90_000;
const METADATA_POLL_INTERVAL_MS = 750;

/**
 * Adds a torrent (magnet, remote .torrent URL, or local path) and resolves
 * once its metadata (file list) is available.
 *
 * POST /torrents on the daemon returns immediately — for a magnet the only
 * useful field is the infohash. Metadata (file list, length, piece count)
 * is then polled via GET /torrents/{ih} until it arrives or the
 * METADATA_POLL_TIMEOUT_MS budget runs out. The source travels in the
 * query string to avoid HTTP body encoding ambiguity.
 *
 * The optional `isCancelled` callback short-circuits the polling loop —
 * callers should pass a function that returns true once their owning window
 * has closed, so the daemon stops being polled (and heartbeated indirectly
 * by Status) for a torrent the user no longer cares about.
 */
export async function addTorrent(
  port: number,
  source: string,
  isCancelled?: CancelFn,
): Promise<TorrentStatus> {
  if (isCancelled?.()) throw new Error("addTorrent cancelled before request");

  const url = `http://127.0.0.1:${port}/torrents?source=${encodeURIComponent(source)}`;
  diag(`POST /torrents source=${source}`);
  const res = await http.post(url, {} as any);
  if (res.statusCode !== 200) {
    throw new Error(errorText(res));
  }
  const stub = JSON.parse(res.text) as TorrentStatus;
  if (stub.files && stub.files.length > 0) {
    diag(`torrent added: ${stub.name} (${stub.files.length} files, metadata immediate)`);
    return stub;
  }

  diag(`torrent ${stub.infohash} added; polling for metadata`);
  const deadline = Date.now() + METADATA_POLL_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (isCancelled?.()) {
      throw new Error("addTorrent cancelled while polling for metadata");
    }
    await sleep(METADATA_POLL_INTERVAL_MS);
    if (isCancelled?.()) {
      throw new Error("addTorrent cancelled while polling for metadata");
    }
    const status = await getStatus(port, stub.infohash);
    if (status && status.files && status.files.length > 0) {
      diag(`metadata ready: ${status.name} (${status.files.length} files)`);
      return status;
    }
  }
  throw new Error("timed out waiting for torrent metadata");
}

/** Fetches a torrent's status snapshot, or null on any failure. */
export async function getStatus(port: number, infohash: string): Promise<TorrentStatus | null> {
  try {
    const res = await http.get(`http://127.0.0.1:${port}/torrents/${infohash}`, {} as any);
    return res.statusCode === 200 ? (JSON.parse(res.text) as TorrentStatus) : null;
  } catch {
    return null;
  }
}

/** Sends a keepalive so the daemon does not idle-shut-down during playback. */
export async function heartbeat(port: number): Promise<void> {
  try {
    await http.post(`http://127.0.0.1:${port}/heartbeat`, {} as any);
  } catch {
    // A missed heartbeat is non-fatal; the next tick will retry.
  }
}

/** Suspends look-ahead downloading on a torrent. In-flight chunk requests still
 * complete, but no new pieces are prioritised — so a paused viewer is not
 * silently consuming bandwidth for content that may never be watched. */
export async function pauseTorrent(port: number, infohash: string): Promise<void> {
  try {
    await http.post(`http://127.0.0.1:${port}/torrents/${infohash}/pause`, {} as any);
  } catch {
    // Non-fatal: at worst the daemon keeps prefetching until the next pause.
  }
}

/** Restores look-ahead downloading on a previously paused torrent. */
export async function resumeTorrent(port: number, infohash: string): Promise<void> {
  try {
    await http.post(`http://127.0.0.1:${port}/torrents/${infohash}/resume`, {} as any);
  } catch {
    // Non-fatal: the next status poll keeps the daemon aware the viewer is back.
  }
}

/** Response shape from POST /warm-next. `deferred=true` means the head warm
 * was skipped because the active stream's buffer is too thin to safely spend
 * bandwidth on a background fetch — the caller should retry on its next tick
 * rather than mark this index as done. */
export interface WarmNextResult {
  nextIndex: number;
  started: boolean;
  deferred: boolean;
}

/** Asks the daemon to prewarm the head + tail of the next video file in a
 * torrent (after the given index). When `currentOffset` is supplied (mpv's
 * stream-pos in the file at `afterIdx`), the daemon gates the 128 MiB head
 * warm on the active stream's buffer health so it does not steal bandwidth
 * from the file the user is watching. Errors return a deferred:false result
 * so the caller does not retry indefinitely on a misconfigured daemon. */
export async function warmNext(
  port: number,
  infohash: string,
  afterIdx: number,
  currentOffset?: number,
): Promise<WarmNextResult> {
  let url = `http://127.0.0.1:${port}/torrents/${infohash}/warm-next?after=${afterIdx}`;
  if (typeof currentOffset === "number" && currentOffset > 0) {
    url += `&current_offset=${Math.floor(currentOffset)}`;
  }
  try {
    const res = await http.post(url, {} as any);
    if (res.statusCode !== 200) {
      return { nextIndex: -1, started: false, deferred: false };
    }
    const body = JSON.parse(res.text) as Partial<WarmNextResult>;
    return {
      nextIndex: typeof body.nextIndex === "number" ? body.nextIndex : -1,
      started: body.started === true,
      deferred: body.deferred === true,
    };
  } catch {
    return { nextIndex: -1, started: false, deferred: false };
  }
}

/**
 * Builds an all-in-one playback URL. mpv hits this single URL and the daemon
 * resolves the source, waits for metadata, and streams the primary video file
 * — so the player window can be created instantly (buffering until metadata
 * arrives) without the plugin first having to POST /torrents and then
 * redirect mpv with `core.open()`.
 */
export function playURL(port: number, source: string): string {
  return `http://127.0.0.1:${port}/play?source=${encodeURIComponent(source)}`;
}

/**
 * Builds the HTTP URL that streams a torrent file into the player. An optional
 * file name is appended as a trailing path segment so the player can show a
 * readable title; the daemon ignores it.
 */
export function streamURL(
  port: number,
  infohash: string,
  fileIndex: number,
  name?: string,
): string {
  const base = `http://127.0.0.1:${port}/stream/${infohash}/${fileIndex}`;
  return name ? `${base}/${encodeURIComponent(name)}` : base;
}

/**
 * Asks the daemon to download a single file inside a torrent to completion
 * and return its absolute on-disk path.
 *
 * IINA's core.subtitle.loadTrack rejects HTTP URLs with "Unsupported external
 * subtitles" — it requires a local file path with a recognised extension. The
 * daemon already writes torrent data to disk via anacrolix's storage.NewFile,
 * so once all pieces of a subtitle file are downloaded and verified the
 * resulting file is a real .srt/.ass/etc. on disk that IINA can load
 * directly. Returns null if the daemon fails to materialise the file in time.
 */
export async function prewarmFile(
  port: number,
  infohash: string,
  fileIndex: number,
): Promise<string | null> {
  try {
    const res = await http.post(
      `http://127.0.0.1:${port}/torrents/${infohash}/files/${fileIndex}/prewarm`,
      {} as any,
    );
    if (res.statusCode !== 200) return null;
    const body = JSON.parse(res.text) as { path?: string };
    return body && typeof body.path === "string" ? body.path : null;
  } catch {
    return null;
  }
}
