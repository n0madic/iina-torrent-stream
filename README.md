# IINA Torrent Stream

Stream torrents directly into the [IINA](https://iina.io) media player on macOS —
with full **seeking** and **buffering** support. Paste a magnet link and the
video starts playing in seconds; the rest downloads in the background while you
watch.

---

## How it works

IINA plugins run inside a sandboxed JavaScript engine with no access to raw
network sockets, so a torrent client cannot live inside the plugin itself.
Instead this project has two parts:

```
 IINA plugin  (JavaScript sandbox)              torrentd  (native Go process)
 ─────────────────────────────────             ──────────────────────────────
 global.ts   Plugin-menu entry points,
             progress HUD, daemon lifecycle
 index.ts    on_load hook intercepts
             magnet: / *.torrent
             │  spawn via utils.exec ─────────► HTTP API on 127.0.0.1:<port>
             │  GET /play?source=... ─────────► ├─ anacrolix/torrent engine
             │  (one URL: metadata + stream)    ├─ ephemeral scratch cache
             │                                  ├─ tracker / DHT / UPnP
             ▼                                  └─ GET /stream — Range 206
            mpv  ◄── byte-range HTTP stream ──────┘
 sidebar     live progress, peers, file list
             (polls /torrents/{ih})
```

- **The plugin** (TypeScript) intercepts torrent sources as a file is opened
  and points mpv at the daemon's all-in-one `/play` URL. mpv starts buffering
  immediately while the daemon resolves metadata in the background — no
  "cannot open URL" flash from mpv trying to play a `magnet:` itself.
- **`torrentd`** (Go, using [`anacrolix/torrent`](https://github.com/anacrolix/torrent))
  downloads pieces on demand and serves the chosen file over HTTP with byte-range
  support. Range requests are what make **seeking** work: when you drag the seek
  bar, mpv requests a new byte range and the engine instantly re-prioritises the
  pieces around that position.

Seeking and buffering specifics:

- **Seeking** — `http.ServeContent` over a seekable torrent reader answers
  `206 Partial Content`; the reader re-prioritises pieces at the new offset.
- **Buffering** — the reader keeps a configurable read-ahead window (default
  512 MiB, clamped to 256–2048 MiB) so the whole peer swarm stays busy, the
  file tail is fetched eagerly (so MP4 `moov` atoms at end-of-file do not stall
  startup), and mpv's own demuxer cache is enlarged.
- **Ephemeral cache** — downloaded pieces are written to a temporary scratch
  directory. The daemon shuts down and purges that directory shortly after the
  last player window closes, so downloaded data does not linger on disk.

## Requirements

- macOS (Apple Silicon or Intel)
- IINA 1.3 or newer with the plugin system enabled
- Internet access on first use (to download the `torrentd` binary once)

## Installation

### From a release

1. Download `iina-torrent-stream.iinaplgz` from the Releases page.
2. Open it with IINA, or drag it onto the IINA window.
3. Enable the plugin under **Settings → Plugins** and grant the requested
   permissions.

On first use the plugin downloads the small native `torrentd` engine into its
private data folder. Everything works offline afterwards.

### From source

```sh
git clone https://github.com/n0madic/iina-torrent-stream.git
cd iina-torrent-stream
make dev          # build the plugin and symlink it into IINA
make dev-daemon   # build torrentd for this Mac and install it locally
```

Then restart IINA and enable the plugin. `make dev-daemon` installs the engine
locally so development needs no network and no GitHub release.

## Usage

- **Magnet link** — open it with IINA (e.g. from your browser), or use
  **Plugins → Torrent Stream → Open Magnet Link…** and paste it.
- **.torrent file** — open it with IINA, or use **Open Torrent File…**.
- **Remote .torrent URL** — opening an `http(s)://…/file.torrent` URL works too.

For multi-video torrents (season packs) every episode is added to the playlist.
The **Torrent** sidebar shows live download speed, peers, and the file list;
click any video file there to switch to it.

## Preferences

Found under **Settings → Plugins → Torrent Stream**:

| Setting | Default | Description |
|---|---|---|
| Read-ahead window | 512 MiB | How far ahead the engine downloads (clamped 256–2048 MiB). Not a disk cap. Raise it if playback stalls on a slow swarm. |
| In-player buffer | 256 MiB | mpv `demuxer-max-bytes` — larger smooths slow swarms. |
| Keep seeding | on | Upload back to the swarm while the engine runs. |
| Show sidebar automatically | off | Reveal the Torrent sidebar on playback start. |
| Enable diagnostic endpoints | off | Expose `/debug/memstats` and `/debug/pprof/*` on the daemon for memory/CPU diagnosis. Restart IINA after toggling. |

## Building

```sh
make plugin          # install npm deps, build dist/index.js + dist/global.js
make daemon          # build a universal2 torrentd into build/ (local testing)
make dev             # build the plugin and symlink it into IINA for live dev
make dev-daemon      # build torrentd for this Mac and install it into the data dir
make test            # run the daemon's Go test suite
make package         # produce iina-torrent-stream.iinaplgz
make daemon-release  # build per-arch release binaries + print SHA-256 sums
make clean           # remove all build artifacts
```

The daemon's reported version is set by the `VERSION` environment variable
(falls back to `dev` if unset). For local dev builds `dev` is recognised by
the plugin as a "skip version check" sentinel, so `make dev-daemon` keeps
working without needing a GitHub release.

## Releasing

Releases are fully automated. Pushing a version tag builds and publishes
everything:

```sh
git tag v0.2.0
git push origin v0.2.0
```

On a macOS runner the [release workflow](.github/workflows/release.yml) runs the
test suite, builds and ad-hoc-signs `torrentd-darwin-arm64` and
`torrentd-darwin-x86_64`, injects the tag version and the binaries' SHA-256
checksums into the plugin sources, packages `iina-torrent-stream.iinaplgz`, and
creates a GitHub Release with all three assets attached. No manual steps and no
source edits are needed — just push the tag.

> **Code signing.** Release binaries are ad-hoc signed and the plugin clears the
> macOS quarantine flag after download. For wider distribution, sign the
> binaries with a Developer ID certificate and notarize them.

## Project layout

```
Info.json                plugin manifest (id, permissions, preference defaults)
package.json             npm + parcel build config for the plugin
tsconfig.json            TypeScript compiler config
Makefile                 build / dev / package / release targets
src/
  index.ts               per-player main entry: on_load hook, sidebar, tracking
  global.ts              once-per-IINA entry: menus, progress HUD, daemon lifecycle
  daemon.ts              torrentd HTTP client + binary download/verify/spawn
  torrent.ts             URL classification and file filtering helpers
  options.ts             typed accessors for user preferences
  log.ts                 append-only diagnostic log
  util.ts                shared utilities (errorMessage, onPluginQueue, CancelFn)
sidebar.html             Torrent sidebar webview (live progress, peers, files)
pref.html                preferences page
progress.html            HUD shown while resolving torrent metadata
magnet-input.html        wide input dialog for the "Open Magnet Link…" menu
torrentd/                the native Go daemon
  main.go                CLI flags, lifecycle, lock, log, cache purge
  server.go              HTTP API + diagnostic endpoints
  engine.go              anacrolix wrapper, per-torrent GC, stream/prewarm
  storage.go             anacrolix file storage backend
  lifecycle.go           idle-timeout monitor
  engine_test.go         unit tests
LICENSE / NOTICE         MIT (plugin) + MPL-2.0 attribution for daemon deps
.github/workflows/       release.yml — tag-push → GitHub Release
```

## Troubleshooting

- **"The torrent engine did not start in time"** — check IINA's Log Window
  (Plugins → Log Viewer) for `[torrentd]` lines. During development, confirm
  `make dev-daemon` installed the binary.
- **Playback never starts** — the torrent may be poorly seeded; the sidebar
  shows the peer count. Metadata resolution times out after ~90 s on the
  plugin side; the daemon will keep trying for up to 5 min after that.
- **Engine keeps running after closing IINA** — it should not: `torrentd`
  self-terminates after 30 s of inactivity. Verify with `pgrep torrentd`.
- **High memory use during long sessions** — note that macOS `ps -o rss`
  and Activity Monitor's "Real Memory" column include shared mapped files
  and dyld cache, which routinely inflate the displayed number into the
  gigabyte range without any actual leak. The honest figure is **Physical
  footprint** from `vmmap -summary $(pgrep torrentd)` — for one active
  torrent it should sit around 100–150 MB. Idle torrents are dropped
  automatically after 2 min of inactivity.

  For deeper inspection, enable **Settings → Plugins → Torrent Stream →
  Enable diagnostic endpoints** and restart IINA. The daemon then exposes
  `http://127.0.0.1:<port>/debug/memstats` (cheap JSON snapshot) and the
  standard Go `/debug/pprof/heap`, `/debug/pprof/goroutine`, etc. The port
  is in
  `~/Library/Application Support/com.colliderli.iina/plugins/.data/com.github.n0madic.iina-torrent-stream/torrentd.json`.

## Legal

This plugin is a streaming tool. You are responsible for ensuring you have the
right to download and stream any content you use it with. The automated tests
use only public-domain torrents (Blender Foundation films).

## License

This project's own source code is licensed under the [MIT License](LICENSE).
The `torrentd` daemon links Mozilla Public License 2.0 libraries; see
[NOTICE](NOTICE) for details.
