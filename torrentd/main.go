// Command torrentd is the native companion process for the IINA Torrent Stream
// plugin. It runs a small localhost HTTP server that streams torrent content
// (with byte-range support for seeking) into the player. The IINA plugin spawns
// and supervises this process; see the project README for the architecture.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

// version is overridden at build time via:
//
//	-ldflags "-X main.version=<value>"
//
// Both `make daemon` and `make daemon-release` set this from the VERSION
// environment variable; the release workflow exports VERSION from the git
// tag. The plugin's ensureBinary() runs `torrentd --version` after install
// to confirm the bundled binary matches DAEMON_VERSION, so this value is
// load-bearing for the "stale binary after plugin update" defence.
var version = "dev"

// multiSink is io.MultiWriter without the short-circuit-on-error behaviour. A
// failing write on one sink (typically a stderr pipe that IINA closed when it
// quit) must NOT stop the other sinks — otherwise the persistent log file
// would silently stop receiving lines just when we need it most.
type multiSink struct{ ws []io.Writer }

func (m *multiSink) Write(p []byte) (int, error) {
	for _, w := range m.ws {
		_, _ = w.Write(p)
	}
	return len(p), nil
}

// config holds the daemon's runtime options, all supplied by the plugin.
type config struct {
	dataDir        string
	cacheDir       string
	stateFile      string
	readaheadBytes int64
	idleTimeout    time.Duration
	seed           bool
	showVersion    bool
	debugEndpoints bool
}

func parseFlags() config {
	var (
		c            config
		readaheadMiB int64
	)
	flag.StringVar(&c.dataDir, "data-dir", "", "directory for the lock and state files")
	flag.StringVar(&c.cacheDir, "cache-dir", "", "directory for the streaming scratch cache (default: <data-dir>/cache)")
	flag.StringVar(&c.stateFile, "state-file", "", "path to the JSON state file (default: <data-dir>/torrentd.json)")
	flag.Int64Var(&readaheadMiB, "readahead-mib", 1024, "size of the streaming read-ahead window in MiB (clamped to 256..2048)")
	// Default matches the plugin's IDLE_TIMEOUT (src/daemon.ts). The plugin
	// always passes --idle-timeout explicitly, so this default only matters
	// when torrentd is run by hand for debugging.
	flag.DurationVar(&c.idleTimeout, "idle-timeout", 30*time.Second, "exit after this long with no activity")
	flag.BoolVar(&c.seed, "seed", true, "keep seeding while the daemon runs")
	flag.BoolVar(&c.showVersion, "version", false, "print version and exit")
	// Diagnostic endpoints (/debug/memstats, /debug/pprof/*) are off by
	// default — they let any localhost process inspect the daemon's memory
	// layout and capture profiles, which is a small but unnecessary attack
	// surface for normal use. Enable from the plugin's preferences when
	// debugging memory or goroutine issues.
	flag.BoolVar(&c.debugEndpoints, "debug-endpoints", false, "expose /debug/memstats and /debug/pprof/* for diagnosis")
	flag.Parse()

	c.readaheadBytes = readaheadMiB << 20
	if c.dataDir == "" {
		c.dataDir = filepath.Join(os.TempDir(), "iina-torrent-stream")
	}
	if c.cacheDir == "" {
		c.cacheDir = filepath.Join(c.dataDir, "cache")
	}
	if c.stateFile == "" {
		c.stateFile = filepath.Join(c.dataDir, "torrentd.json")
	}
	return c
}

func main() {
	log.SetPrefix("[torrentd] ")
	log.SetFlags(log.Ltime)

	// Surviving SIGPIPE is essential. When IINA quits, the stderr pipe back to
	// IINA closes, and Go's default handler turns the next write to stderr (or
	// stdout) into a FATAL signal — terminating us before any deferred cleanup
	// (cache purge, lock release, …) can run. Ignoring SIGPIPE turns those
	// writes into plain EPIPE errors instead, which multiSink swallows.
	signal.Ignore(syscall.SIGPIPE)

	cfg := parseFlags()
	if cfg.showVersion {
		fmt.Println(version)
		return
	}

	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	// Singleton: only one daemon may own a given data directory. If the lock is
	// already held, another daemon is running and the plugin will reuse it.
	lockPath := filepath.Join(cfg.dataDir, "torrentd.lock")
	lock, acquired, err := acquireLock(lockPath)
	if err != nil {
		log.Fatalf("acquire lock: %v", err)
	}
	if !acquired {
		fmt.Println("ALREADY-RUNNING")
		return
	}
	defer func() {
		_ = lock.Close()
		_ = os.Remove(lockPath)
	}()

	// Tee log to a file in the data dir as well, so a record survives even when
	// the parent IINA closes our stderr pipe (orphan daemon, IINA quit). The
	// file is what you read to find out why the cache was not purged. We
	// O_TRUNC on every startup so it never accumulates across daemon runs —
	// only the current session is kept, and the file is listed first in the
	// sink so a broken stderr never starves it.
	logFilePath := filepath.Join(cfg.dataDir, "torrentd.log")
	// 0o600: the log may contain the daemon's listening port and infohashes
	// of opened torrents — readable only by the owning user.
	logFile, ferr := os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if ferr != nil {
		// Surface the reason — silently falling back to stderr-only means
		// every later log line vanishes once IINA closes the stderr pipe,
		// taking with it the diagnostics that prove cleanup actually ran.
		log.Printf("warning: open log file %s: %v (logging to stderr only)", logFilePath, ferr)
	} else {
		log.SetOutput(&multiSink{ws: []io.Writer{logFile, os.Stderr}})
		defer func() { _ = logFile.Close() }()
	}

	// A previous daemon may have exited without cleaning up (hard kill, crash,
	// or the host app quitting). We now hold the singleton lock, so any leftover
	// cache is stale and safe to purge.
	if err := os.RemoveAll(cfg.cacheDir); err != nil {
		log.Printf("warning: could not purge stale cache: %v", err)
	}
	if err := os.MkdirAll(cfg.cacheDir, 0o755); err != nil {
		log.Fatalf("create cache dir: %v", err)
	}
	// Always purge the cache on the way out — including on a panic inside
	// engine.Close — so the user never finds stale downloads on disk after
	// the daemon exits. Retries because anacrolix's shared file handle pool
	// can briefly recreate files after Client.Close returns.
	defer purgeCacheDir(cfg.cacheDir)
	// And the state file, so the plugin's "is a daemon running?" check is
	// authoritative (no state file = no daemon = the next torrent-open will
	// spawn a fresh, clean daemon).
	defer func() { _ = os.Remove(cfg.stateFile) }()

	engine, err := NewEngine(cfg.dataDir, cfg.cacheDir, cfg.readaheadBytes, cfg.seed)
	if err != nil {
		log.Fatalf("start torrent engine: %v", err)
	}

	lc := newLifecycle(cfg.idleTimeout)
	srv := newServer(engine, lc, cfg.debugEndpoints)
	if cfg.debugEndpoints {
		log.Printf("debug endpoints enabled at /debug/memstats and /debug/pprof/*")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	httpServer := &http.Server{Handler: srv.mux}
	go func() {
		if serveErr := httpServer.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			log.Printf("http server: %v", serveErr)
			lc.trigger()
		}
	}()

	if err := writeState(cfg.stateFile, port); err != nil {
		log.Printf("warning: write state file: %v", err)
	}

	log.Printf("listening on 127.0.0.1:%d (readahead %d MiB, cache at %s)", port, cfg.readaheadBytes>>20, cfg.cacheDir)

	go lc.monitor()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sigCh:
		log.Printf("received signal %s, shutting down", s)
	case <-lc.shutdown:
		log.Printf("shutdown requested (idle timeout or /shutdown), shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	log.Printf("shutdown: stopping HTTP server")
	_ = httpServer.Shutdown(shutdownCtx)
	log.Printf("shutdown: closing engine")
	engine.Close()
	log.Printf("shutdown: returning from main, deferred cleanup runs next")
	// State-file removal and cache purge run from the deferred cleanup above.
}

// purgeCacheDir removes the cache dir, retrying briefly because the anacrolix
// file storage maintains a shared file handle pool that, in some races, can
// recreate files just after Client.Close returns. On final failure it lists
// what is still on disk so the cause can be diagnosed.
func purgeCacheDir(dir string) {
	for attempt := 1; attempt <= purgeMaxAttempts; attempt++ {
		err := os.RemoveAll(dir)
		if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
			log.Printf("purged cache dir %s (attempt %d)", dir, attempt)
			return
		}
		log.Printf("purge attempt %d: RemoveAll err=%v, dir still present", attempt, err)
		time.Sleep(purgeRetryInterval)
	}
	entries, _ := os.ReadDir(dir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	log.Printf("FAILED to purge cache dir %s — leftover entries: %v", dir, names)
}

// writeState records the listening port and PID so the plugin can find the daemon.
func writeState(path string, port int) error {
	data, err := json.Marshal(struct {
		Port    int    `json:"port"`
		PID     int    `json:"pid"`
		Version string `json:"version"`
	}{Port: port, PID: os.Getpid(), Version: version})
	if err != nil {
		return err
	}
	// 0o600: state file holds the local listening port; on a multi-user Mac
	// other users have no reason to read this.
	return os.WriteFile(path, data, 0o600)
}

// acquireLock takes an exclusive non-blocking flock on path. The returned bool
// is false (with a nil error) when another process already holds the lock.
func acquireLock(path string) (*os.File, bool, error) {
	// 0o600: lock file is purely a coordination handle for the owning user.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, false, nil
	}
	return f, true, nil
}
