// Typed accessors for the plugin's user preferences. The defaults mirror
// `preferenceDefaults` in Info.json and act as a fallback if a value is missing
// or malformed.

const { preferences } = iina;

function readInt(key: string, fallback: number): number {
  const value = parseInt(preferences.get(key), 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function readBool(key: string, fallback: boolean): boolean {
  // IINA's preference round-trip may surface a boolean as the literal true/false,
  // as 0/1 (Cocoa NSNumber-backed bool), or as the string "true"/"false". Accept
  // all of them — falling through to the fallback would silently override the
  // user's checkbox setting.
  const v = preferences.get(key);
  if (v === true || v === 1 || v === "true" || v === "1") return true;
  if (v === false || v === 0 || v === "false" || v === "0") return false;
  return fallback;
}

export const opt = {
  /** Size of the daemon's streaming read-ahead window, in MiB. */
  get readaheadMiB(): number {
    return readInt("readaheadMiB", 512);
  },
  /** Whether the daemon keeps seeding while it runs. */
  get seeding(): boolean {
    return readBool("seeding", true);
  },
  /** Whether the Torrent sidebar is revealed automatically on playback start. */
  get autoShowSidebar(): boolean {
    return readBool("autoShowSidebar", false);
  },
  /** mpv `demuxer-max-bytes` value, in MiB — the in-player buffer size.
   * Used as the starting value before duration is known, and as the final
   * value when auto-tune is off. */
  get demuxerMaxBytesMiB(): number {
    return readInt("demuxerMaxBytesMiB", 256);
  },
  /** When true, resize demuxer-max-bytes to match the stream's actual
   * bitrate × cache-secs once mpv reports a duration — so the byte cap
   * does not silently truncate the cache-secs buffer on high-bitrate
   * content (e.g. 4K), and low-bitrate content does not over-allocate. */
  get demuxerMaxBytesAuto(): boolean {
    return readBool("demuxerMaxBytesAuto", true);
  },
  /** Whether to expose /debug/memstats and /debug/pprof/* on the daemon's
   * HTTP server. Off by default. Requires restarting IINA after toggling
   * for the change to take effect (the daemon is spawned once per session
   * and reads its flags at startup). */
  get debugEndpoints(): boolean {
    return readBool("debugEndpoints", false);
  },
};
