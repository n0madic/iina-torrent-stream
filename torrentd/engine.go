package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"golang.org/x/time/rate"
)

const (
	// readaheadMin/Max bound the look-ahead window — how far ahead of the play
	// cursor the streaming reader keeps pieces prioritised. A large window is
	// essential: it is what keeps many "wanted" pieces in flight so the whole
	// peer swarm can download in parallel. A window of only a few pieces leaves
	// most peers idle and starves playback.
	readaheadMin = 256 << 20 // 256 MiB
	readaheadMax = 2 << 30   // 2 GiB
	// tailWarmBytes is the size of the file tail fetched eagerly so containers
	// that store their index at end-of-file start instantly.
	tailWarmBytes = 4 << 20 // 4 MiB
	// headWarmBytes is the size of the file head fetched eagerly during a
	// next-episode prewarm. Sized to outlast the moment mpv switches playlist
	// items so the swarm has time to reprioritise on the new file before mpv's
	// cache-secs window empties. 128 MiB buys ~1.5-3.5 minutes of pre-buffered
	// HD video — at the 90 %-played trigger that fits well inside the ~4
	// minutes of an average remaining episode. Started at 32 MiB; that was
	// too small and caused visible buffering shortly after the cut.
	headWarmBytes = 128 << 20 // 128 MiB
	// metadataTimeout bounds how long the background goroutine waits for a
	// magnet's metadata before giving up and dropping the torrent. POST
	// /torrents returns immediately (with just the infohash), so this timeout
	// no longer affects the HTTP response — the plugin polls /torrents/{ih}
	// for the file list with its own (shorter) deadline.
	metadataTimeout = 5 * time.Minute
	// The next four constants make the engine more aggressive about peer
	// discovery so a stream starts pulling data quickly. They are deliberately
	// well above anacrolix's defaults (50 / 25 / 100 / 64 MiB).
	establishedConnsPerTorrent = 200
	halfOpenConnsPerTorrent    = 60
	totalHalfOpenConns         = 300
	// maxUnverifiedBytes raises the cap on downloaded-but-unverified data so
	// the engine keeps more pieces in flight while hashes catch up.
	maxUnverifiedBytes = 256 << 20

	// remoteMetainfoTimeout is the per-request budget for downloading a remote
	// .torrent file. It bounds wall-clock time at the HTTP-client level so a
	// hung server cannot block Add indefinitely (the caller-supplied context
	// also applies on top of this).
	remoteMetainfoTimeout = 30 * time.Second
	// remoteMetainfoMaxBytes caps the response body of a remote .torrent fetch.
	// Real-world .torrent files are well under 10 MiB; this cap prevents a
	// malicious or misbehaving server from streaming gigabytes into metainfo.Load.
	remoteMetainfoMaxBytes = 32 << 20 // 32 MiB
	// remoteMetainfoMaxRedirects caps follow-the-Location chains. We accept a
	// couple of hops (CDN edge → origin) but refuse arbitrarily long chains
	// that could attempt to hide the final target from the user.
	remoteMetainfoMaxRedirects = 3

	// purgeRetryInterval and purgeMaxAttempts are used by main.purgeCacheDir
	// when retrying RemoveAll after the anacrolix file pool races with cleanup.
	purgeRetryInterval = 200 * time.Millisecond
	purgeMaxAttempts   = 5
	// prewarmPollInterval / prewarmPollTimeout govern the busy-wait after the
	// final piece passes hash verification, while anacrolix renames the
	// "<file>.part" scratch file to its final name.
	prewarmPollInterval = 50 * time.Millisecond
	prewarmPollTimeout  = 3 * time.Second
	// shutdownTimeout bounds the http.Server graceful shutdown wait.
	shutdownTimeout = 5 * time.Second

	// torrentIdleTimeout is the per-torrent inactivity window. A torrent is
	// dropped (t.Drop) once it has had zero active streaming readers AND no
	// status/pause/resume request for at least this long. Without this, a
	// long IINA session accumulates every torrent the user has ever opened:
	// each kept-alive torrent maintains peer connections, UTP buffers, DHT
	// activity, and (when seed=true) seeds in perpetuity — easily 50-100 MB
	// of RSS per dormant torrent. With per-torrent GC, RSS returns to baseline
	// once watching stops.
	torrentIdleTimeout = 2 * time.Minute
	// torrentGCInterval is how often the GC loop scans for evictable torrents.
	torrentGCInterval = 30 * time.Second

	// recoveryCheckInterval, recoveryWindowBuckets and recoveryErrorThreshold
	// configure the network-stall watchdog. Every recoveryCheckInterval the
	// loop drains the resendErrorMonitor counter into a rolling window of
	// recoveryWindowBuckets entries; when the sum across the window crosses
	// recoveryErrorThreshold the torrent client is rebuilt. The default of
	// 30 errors over 30 s (3 × 10 s buckets) catches the post-VPN-flap
	// "every uTP send is ENETUNREACH" storm — observed at ~2 errors/s in
	// the wild — without firing on a one-second blip of a handful of packets.
	recoveryCheckInterval  = 10 * time.Second
	recoveryWindowBuckets  = 3
	recoveryErrorThreshold = 30
	// recoveryBackoff is the minimum gap between two successive client
	// rebuilds. A rebuild that does not actually clear the routing problem
	// (e.g. the underlying network is still broken) would otherwise loop
	// continuously, churning peer connections and adding nothing.
	recoveryBackoff = 60 * time.Second
)

// publicTrackers is a small curated set of stable public BitTorrent trackers,
// appended to every (non-private) torrent we add. Each entry is in its own
// tracker tier so anacrolix announces to them in parallel rather than
// fall-through-order. Their job is to enlarge the peer pool at startup beyond
// whatever the original magnet/.torrent listed.
var publicTrackers = [][]string{
	{"udp://tracker.opentrackr.org:1337/announce"},
	{"udp://tracker.openbittorrent.com:6969/announce"},
	{"udp://exodus.desync.com:6969/announce"},
	{"udp://open.demonii.com:1337/announce"},
	{"udp://tracker.torrent.eu.org:451/announce"},
	{"udp://tracker.dler.org:6969/announce"},
}

// Engine wraps an anacrolix torrent client and exposes the operations the HTTP
// server needs: adding sources, streaming files, and reporting status.
type Engine struct {
	// client is held via atomic.Pointer because the network-stall watchdog
	// swaps generations in recreateClient. Operations that bind a new torrent
	// also hold clientMu, while read-only callers load the pointer once and pass
	// it down rather than mixing generations mid-operation.
	client atomic.Pointer[torrent.Client]
	// clientMu serialises operations that bind torrents to a client generation
	// with recovery rebuilds. A torrent added after a rebuild snapshot must never
	// be left attached to the client that is about to be closed.
	clientMu sync.RWMutex
	// storage is held so Close can shut down the piece-completion DB and so
	// recreateClient can hand the same storage to the rebuilt client (which
	// keeps every already-downloaded piece). The torrent client does not
	// close its default storage on its own.
	storage storage.ClientImplCloser
	// seed is captured here so recreateClient can rebuild the client config
	// without the caller having to plumb the flag through again.
	seed bool

	// cacheDir is the on-disk root anacrolix writes torrent files to. We use it
	// to construct absolute paths for callers that want to read a torrent file
	// directly from disk (subtitle pre-warm — IINA's subtitle loader requires a
	// local file path with a recognised extension).
	cacheDir string

	// readahead is the look-ahead window size applied to every streaming
	// reader — how much data is kept prioritised ahead of the play cursor.
	readahead int64

	// monitor counts uTP resend-failure log lines so recoveryLoop can detect a
	// dead-socket condition. Optional: nil disables the watchdog (used by
	// tests that never wire the std logger through resendErrorMonitor).
	monitor *resendErrorMonitor

	// done/ctx are cancelled by Close. ctx is also used by region warmers so a
	// blocked torrent read stops before the client and storage are closed.
	done chan struct{}
	ctx  context.Context
	stop context.CancelFunc
	// wg tracks all goroutines spawned by the engine (sampleLoop, gcLoop,
	// recoveryLoop, completeMetadata and warm workers) so Close can wait for them to
	// exit before it closes the torrent client. Without this wait, sampleLoop
	// could call t.Stats() and completeMetadata could call t.AddTrackers
	// concurrently with client.Close — anacrolix does not document those
	// as safe.
	wg sync.WaitGroup
	// workerMu prevents WaitGroup.Add from racing with Close/Wait.
	workerMu  sync.Mutex
	closed    bool
	closeOnce sync.Once

	mu       sync.Mutex
	torrents map[string]*managedTorrent
}

// managedTorrent holds per-torrent bookkeeping, including download-rate samples.
type managedTorrent struct {
	// t is the current *torrent.Torrent handle. Held atomically because the
	// network-stall watchdog can swap it (recreateClient): the rest of the
	// engine reads via t.Load() so a swap mid-call simply means later
	// operations land on the new torrent. Any in-flight reader bound to the
	// previous handle will see EOF/error on its next Read once the old
	// client is closed and unwind normally.
	t          atomic.Pointer[torrent.Torrent]
	addedAt    time.Time
	warmed     map[int]bool // file indexes whose tail has already been warmed
	warmedHead map[int]bool // file indexes whose head has already been warmed

	// readers tracks the live streaming readers. gcLoop uses len(readers) > 0
	// to decide whether a torrent is still in use; /debug/memstats sums the
	// counts across torrents.
	readers []managedReader
	paused  bool
	// warmCtx is cancelled independently on Pause/Remove so background head/tail
	// reads stop even when they are blocked inside ReadContext. Resume installs
	// a fresh child of the engine context for later warm attempts.
	warmCtx    context.Context
	cancelWarm context.CancelFunc

	// lastActiveAt is updated on every per-torrent client interaction
	// (status poll, pause/resume, prewarm, the start of a stream request).
	// gcLoop evicts torrents that have no live readers AND whose lastActiveAt
	// has not been touched for torrentIdleTimeout. While at least one stream
	// reader is registered the torrent is considered live regardless of this
	// timestamp — in-flight playback must never be torn down by the GC.
	lastActiveAt time.Time

	lastSampleAt time.Time
	lastRead     int64
	lastWritten  int64
	downloadRate int64
	uploadRate   int64
}

type managedReader struct {
	reader    torrent.Reader
	readahead int64
}

// NewEngine creates the torrent client and sets the streaming read-ahead
// window. readaheadBytes is the requested look-ahead size; values outside
// [readaheadMin, readaheadMax] are clamped. monitor may be nil (in which
// case the network-stall watchdog is disabled — useful for tests).
func NewEngine(dataDir, cacheDir string, readaheadBytes int64, seed bool, monitor *resendErrorMonitor) (*Engine, error) {
	store := newStreamStorage(cacheDir)

	client, err := buildTorrentClient(cacheDir, store, seed)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	// Clamp the configured read-ahead window into a sane range. The window is
	// what keeps the whole peer swarm busy — too small and most peers go idle.
	readahead := max(readaheadBytes, readaheadMin)
	if readahead > readaheadMax {
		readahead = readaheadMax
	}

	engineCtx, stop := context.WithCancel(context.Background())
	e := &Engine{
		storage:   store,
		seed:      seed,
		cacheDir:  cacheDir,
		readahead: readahead,
		monitor:   monitor,
		done:      make(chan struct{}),
		ctx:       engineCtx,
		stop:      stop,
		torrents:  make(map[string]*managedTorrent),
	}
	e.client.Store(client)
	e.startWorker(e.sampleLoop)
	e.startWorker(e.gcLoop)
	if monitor != nil {
		e.startWorker(e.recoveryLoop)
	}
	return e, nil
}

func (e *Engine) startWorker(fn func()) bool {
	e.workerMu.Lock()
	defer e.workerMu.Unlock()
	if e.closed {
		return false
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		fn()
	}()
	return true
}

// buildTorrentClient is the single source of truth for the torrent client
// configuration — used both for the engine's initial client and for the
// recoveryLoop's rebuild. cacheDir is the DataDir; store is the storage
// implementation that owns the on-disk piece layout and BoltDB completion
// records (shared across rebuilds so already-downloaded pieces survive).
func buildTorrentClient(cacheDir string, store storage.ClientImplCloser, seed bool) (*torrent.Client, error) {
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = cacheDir
	cfg.DefaultStorage = store
	cfg.Seed = seed

	// Tune for maximum reachability and a fast streaming start:
	//   - ListenPort = 0 lets the OS pick a free peer port (anacrolix's
	//     default 42069 fails NewClient if anything else holds it). UPnP and
	//     incoming-peer routing both honour the actually-bound port.
	//   - The connection-limit knobs are well above defaults so the daemon
	//     populates the peer pool aggressively at start.
	//   - DialRateLimiter is raised 10× over the default (10/s) — fast
	//     enough to drain a fresh peer pool in seconds rather than minutes,
	//     but not "unlimited" (a SYN-scan-shaped burst can trip IDS/IPS in
	//     some networks, and we are already bounded by TotalHalfOpenConns
	//     anyway).
	cfg.ListenPort = 0
	cfg.EstablishedConnsPerTorrent = establishedConnsPerTorrent
	cfg.HalfOpenConnsPerTorrent = halfOpenConnsPerTorrent
	cfg.TotalHalfOpenConns = totalHalfOpenConns
	cfg.MaxUnverifiedBytes = maxUnverifiedBytes
	cfg.DialRateLimiter = rate.NewLimiter(100, 100)

	client, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create torrent client: %w", err)
	}
	return client, nil
}

// Close shuts the torrent client and the underlying piece-completion DB down.
// Engine goroutines (sampleLoop, gcLoop, recoveryLoop, completeMetadata
// workers) are signalled via e.done and then waited on before the client is
// closed — calling client.Close while sampleLoop is still inside t.Stats(),
// or while a completeMetadata worker is inside t.AddTrackers, races on
// anacrolix internals. Closing the storage explicitly is what releases the
// BoltDB writer on the completion DB — torrent.Client.Close does not close
// the default storage.
func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		// Wait for any Add/rebuild currently binding handles to finish, then
		// prevent future workers from being registered before Wait begins.
		e.clientMu.Lock()
		e.workerMu.Lock()
		e.closed = true
		close(e.done)
		e.stop()
		e.workerMu.Unlock()
		e.clientMu.Unlock()

		e.wg.Wait()
		if c := e.client.Load(); c != nil {
			c.Close()
		}
		if err := e.storage.Close(); err != nil {
			log.Printf("warning: close storage: %v", err)
		}
	})
}

// Add resolves a magnet link, a remote .torrent URL, or a local .torrent file
// and registers it with the engine. It does NOT block waiting for metadata —
// for a magnet that can take 30s+, which exceeds IINA's hidden HTTP-client
// timeout and causes the plugin's POST /torrents to reject opaquely. Instead
// the torrent is registered immediately with whatever info anacrolix has
// (just an infohash for a magnet); a background goroutine fills in metadata,
// attaches public trackers, and on timeout drops the torrent. Callers learn
// when metadata is ready by polling /torrents/{ih} for a non-empty file list.
func (e *Engine) Add(ctx context.Context, source string) (*managedTorrent, error) {
	source = strings.TrimSpace(source)
	lowerSource := strings.ToLower(source)
	var (
		mi  *metainfo.MetaInfo
		err error
	)
	switch {
	case strings.HasPrefix(lowerSource, "magnet:"):
		// Parsing/binding happens below while the client generation is pinned.
	case strings.HasPrefix(lowerSource, "http://"), strings.HasPrefix(lowerSource, "https://"):
		mi, err = loadRemoteMetainfo(ctx, source)
	default:
		mi, err = metainfo.LoadFromFile(source)
	}
	if err != nil {
		return nil, fmt.Errorf("add torrent: %w", err)
	}

	// Pin the current client generation until the torrent handle is registered
	// in e.torrents. recreateClient takes the write side of this lock.
	e.clientMu.RLock()
	defer e.clientMu.RUnlock()
	e.workerMu.Lock()
	closed := e.closed
	e.workerMu.Unlock()
	if closed {
		return nil, fmt.Errorf("torrent engine is shutting down")
	}
	client := e.client.Load()
	if client == nil {
		return nil, fmt.Errorf("torrent client unavailable")
	}

	var t *torrent.Torrent
	if strings.HasPrefix(lowerSource, "magnet:") {
		t, err = client.AddMagnet(source)
	} else {
		t, err = client.AddTorrent(mi)
	}
	if err != nil {
		return nil, fmt.Errorf("add torrent: %w", err)
	}

	ih := t.InfoHash().HexString()
	e.mu.Lock()
	if mt := e.torrents[ih]; mt != nil {
		// Already tracked. AddMagnet/AddTorrent above are idempotent per
		// infohash, so t is the live handle on the CURRENT client. A previous
		// watchdog rebuild may have left mt.t pointing at a torrent from a
		// since-closed client (re-add fell back, or the client was swapped
		// out from under an in-flight reader) — that stale handle serves
		// frozen status and EOFs every read. Adopt the live handle so playback
		// self-heals when the plugin re-opens the source, no daemon restart
		// needed. The compare keeps the common healthy re-open a no-op
		// (anacrolix returns the same *Torrent for an existing infohash).
		rebound := mt.t.Load() != t
		if rebound {
			mt.t.Store(t)
		}
		// A re-open starts a new playback session, so drop any leftover pause.
		e.resumeLocked(mt)
		e.mu.Unlock()
		if rebound {
			e.startWorker(func() { e.completeMetadata(mt) })
			log.Printf("re-bound torrent %s to the current client", ih)
		}
		return mt, nil
	}

	now := time.Now()
	warmCtx, cancelWarm := context.WithCancel(e.ctx)
	mt := &managedTorrent{
		addedAt:      now,
		warmed:       make(map[int]bool),
		warmedHead:   make(map[int]bool),
		lastSampleAt: now,
		lastActiveAt: now,
		warmCtx:      warmCtx,
		cancelWarm:   cancelWarm,
	}
	mt.t.Store(t)
	e.torrents[ih] = mt
	e.mu.Unlock()
	log.Printf("added torrent %s (info ready=%v)", ih, t.Info() != nil)

	// Async: wait for metadata, attach public trackers once we know the
	// private bit, or drop the torrent on timeout so it does not linger in
	// anacrolix holding connections / DHT traffic forever.
	e.startWorker(func() { e.completeMetadata(mt) })
	return mt, nil
}

// completeMetadata waits (with a long timeout) for the torrent's info to
// arrive, then attaches public trackers if appropriate. If metadata never
// arrives, the torrent is dropped from the engine.
func (e *Engine) completeMetadata(mt *managedTorrent) {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTimeout)
	defer cancel()
	t := mt.t.Load()
	ih := t.InfoHash().HexString()
	select {
	case <-t.GotInfo():
		e.clientMu.RLock()
		e.mu.Lock()
		current := e.torrents[ih] == mt && mt.t.Load() == t
		e.mu.Unlock()
		if !current {
			e.clientMu.RUnlock()
			return
		}
		// Boost peer discovery by announcing to extra well-known trackers —
		// but only when the torrent is NOT marked private (BEP 27). Announcing
		// a private torrent to public trackers can get the user banned from
		// their private tracker, so respect the flag.
		if info := t.Info(); info == nil || info.Private == nil || !*info.Private {
			t.AddTrackers(publicTrackers)
		}
		log.Printf("torrent %s metadata ready (%q, %d files)", ih, t.Name(), len(t.Files()))
		e.clientMu.RUnlock()
	case <-ctx.Done():
		if e.dropIfCurrent(ih, mt, t) {
			log.Printf("torrent %s: metadata wait timed out, dropping", ih)
		} else {
			log.Printf("torrent %s: stale metadata waiter timed out; replacement kept", ih)
		}
	case <-e.done:
		return
	}
}

func (e *Engine) dropIfCurrent(ih string, mt *managedTorrent, t *torrent.Torrent) bool {
	e.clientMu.RLock()
	defer e.clientMu.RUnlock()
	e.mu.Lock()
	current, ok := e.torrents[ih]
	if !ok || current != mt || mt.t.Load() != t {
		e.mu.Unlock()
		return false
	}
	delete(e.torrents, ih)
	e.mu.Unlock()
	if mt.cancelWarm != nil {
		mt.cancelWarm()
	}
	t.Drop()
	return true
}

func (e *Engine) get(ih string) *managedTorrent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.torrents[ih]
}

// touch updates a torrent's lastActiveAt timestamp. Called from every
// per-torrent endpoint so a torrent that is actively being queried (e.g.
// status polling) is never evicted out from under the viewer, even when
// no streaming reader is currently registered. Caller need not hold e.mu.
func (e *Engine) touch(ih string) {
	e.mu.Lock()
	if mt, ok := e.torrents[ih]; ok {
		mt.lastActiveAt = time.Now()
	}
	e.mu.Unlock()
}

// gcLoop periodically evicts torrents that have been dormant for longer than
// torrentIdleTimeout (no streaming readers AND no per-torrent API activity).
// Without this, every torrent ever opened during an IINA session accumulates
// peer connections, UTP buffers, seeding state, and DHT activity — a steady
// memory leak on long sessions.
//
// Dropped torrents stop downloading, close all peer connections, and stop
// announcing to trackers. Files on disk and BoltDB piece-completion records
// are NOT touched — those are owned by the engine-wide cache purge on
// daemon shutdown. So evicting and re-adding the same torrent later in the
// session is safe and fast (anacrolix re-uses the already-downloaded pieces).
func (e *Engine) gcLoop() {
	ticker := time.NewTicker(torrentGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.collectGarbage()
		}
	}
}

// countTorrentsAndReaders returns the current live torrent count and the
// sum of registered streaming readers across all torrents. Used by the
// /debug/memstats endpoint.
func (e *Engine) countTorrentsAndReaders() (torrents, readers int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	torrents = len(e.torrents)
	for _, mt := range e.torrents {
		readers += len(mt.readers)
	}
	return torrents, readers
}

// collectGarbage scans for evictable torrents and drops them. Exposed as a
// separate method so tests can invoke it without waiting on a ticker.
func (e *Engine) collectGarbage() {
	now := time.Now()
	var toDrop []struct {
		ih         string
		t          *torrent.Torrent
		cancelWarm context.CancelFunc
	}

	e.mu.Lock()
	for ih, mt := range e.torrents {
		if len(mt.readers) > 0 {
			continue // active stream — never evict
		}
		if now.Sub(mt.lastActiveAt) < torrentIdleTimeout {
			continue // recent client interaction
		}
		toDrop = append(toDrop, struct {
			ih         string
			t          *torrent.Torrent
			cancelWarm context.CancelFunc
		}{ih, mt.t.Load(), mt.cancelWarm})
		delete(e.torrents, ih)
	}
	e.mu.Unlock()

	// Drop outside the lock — t.Drop closes peer connections and may take a
	// moment; we do not want to hold the central engine mutex through it.
	for _, d := range toDrop {
		log.Printf("torrent gc: evicting idle torrent %s", d.ih)
		if d.cancelWarm != nil {
			d.cancelWarm()
		}
		d.t.Drop()
	}
	if len(toDrop) > 0 {
		// Hand the freed memory back to the OS now. Go's runtime is otherwise
		// in no hurry to return arenas to the kernel — RSS would stay near
		// the peak watermark instead of reflecting the live torrent count.
		runtime.GC()
		debug.FreeOSMemory()
	}
}

// Pause sets every live reader's look-ahead window to zero. With mpv no longer
// issuing foreground reads, this stops the torrent from silently fetching
// future content while preserving the current connections and reader offsets.
func (e *Engine) Pause(ih string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	mt, ok := e.torrents[ih]
	if !ok {
		return false
	}
	mt.paused = true
	if mt.cancelWarm != nil {
		mt.cancelWarm()
	}
	for _, mr := range mt.readers {
		mr.reader.SetReadahead(0)
	}
	mt.lastActiveAt = time.Now()
	return true
}

// Resume restores each reader's intended look-ahead window.
func (e *Engine) Resume(ih string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	mt, ok := e.torrents[ih]
	if !ok {
		return false
	}
	e.resumeLocked(mt)
	return true
}

// resumeLocked undoes Pause: it clears the flag, hands the torrent a fresh warm
// context (Pause cancelled the previous one) and restores every reader's
// look-ahead. It is also called when a paused torrent is re-opened, because
// nothing else clears the flag — the plugin stops tracking a player before the
// window closes, so its "unpaused" event never arrives and the torrent would
// otherwise stay stuck at zero look-ahead for the rest of its life.
// Caller must hold e.mu.
func (e *Engine) resumeLocked(mt *managedTorrent) {
	if mt.paused {
		mt.paused = false
		parent := e.ctx
		if parent == nil {
			parent = context.Background()
		}
		mt.warmCtx, mt.cancelWarm = context.WithCancel(parent)
		for _, mr := range mt.readers {
			mr.reader.SetReadahead(mr.readahead)
		}
	}
	mt.lastActiveAt = time.Now()
}

// Remove drops a torrent and cancels its downloads.
func (e *Engine) Remove(ih string) bool {
	e.mu.Lock()
	mt, ok := e.torrents[ih]
	delete(e.torrents, ih)
	e.mu.Unlock()
	if !ok {
		return false
	}
	if mt.cancelWarm != nil {
		mt.cancelWarm()
	}
	mt.t.Load().Drop()
	log.Printf("removed torrent %s", ih)
	return true
}

// Stream serves a file from a torrent over HTTP with byte-range support, which
// is what enables seeking inside the IINA/mpv player.
func (e *Engine) Stream(w http.ResponseWriter, r *http.Request, ih string, idx int) {
	mt := e.get(ih)
	if mt == nil {
		http.Error(w, "torrent not found", http.StatusNotFound)
		return
	}
	files := mt.t.Load().Files()
	if idx < 0 || idx >= len(files) {
		http.Error(w, "file index out of range", http.StatusNotFound)
		return
	}
	f := files[idx]

	reader := f.NewReader()
	defer reader.Close()
	reader.SetResponsive()

	// Register the reader under e.mu so the append is consistent with
	// gcLoop's len(mt.readers) read and /debug/memstats' sum.
	e.mu.Lock()
	mt.readers = append(mt.readers, managedReader{reader: reader, readahead: e.readahead})
	if mt.paused {
		reader.SetReadahead(0)
	} else {
		reader.SetReadahead(e.readahead)
	}
	// Touch on stream start. While the reader is registered the gcLoop will
	// not evict because len(readers) > 0; this stamp covers the window
	// between unregistration and any next interaction.
	mt.lastActiveAt = time.Now()
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		for i, mr := range mt.readers {
			if mr.reader == reader {
				mt.readers = append(mt.readers[:i], mt.readers[i+1:]...)
				break
			}
		}
		// Refresh on stream end so the GC's idle window starts NOW rather
		// than at the moment the stream began (which for a long playback
		// could already be past the timeout).
		mt.lastActiveAt = time.Now()
		e.mu.Unlock()
	}()

	e.warmTail(mt, idx, f)

	w.Header().Set("Content-Type", mimeForPath(f.DisplayPath()))

	// http.ServeContent's internal copy loop does not observe the request
	// context. If mpv fills its read-ahead buffer, stops reading, and the player
	// window is then closed, the handler's blocked Write would hang indefinitely
	// — and a hung /stream handler keeps the daemon from ever idling out and
	// purging its disk cache. Forcing a past write deadline once the context is
	// cancelled makes the blocked Write fail at once so the handler returns.
	ctx := r.Context()
	rc := http.NewResponseController(w)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = rc.SetWriteDeadline(time.Now())
		case <-stop:
		}
	}()

	cr := &contextReader{ctx: ctx, r: reader}
	http.ServeContent(w, r, filepath.Base(f.DisplayPath()), time.Time{}, cr)
}

// Play resolves a source (magnet/.torrent URL/local .torrent), waits for the
// torrent's metadata, picks its primary video file, and returns the managed
// torrent plus that file index. Used by the /play HTTP endpoint, which then
// hands the result off to Stream — mpv sees a single URL that "just plays".
// The caller's context bounds the metadata wait.
func (e *Engine) Play(ctx context.Context, source string) (*managedTorrent, int, error) {
	mt, err := e.Add(ctx, source)
	if err != nil {
		return nil, -1, err
	}
	t := mt.t.Load()
	if t.Info() == nil {
		select {
		case <-t.GotInfo():
		case <-ctx.Done():
			return nil, -1, fmt.Errorf("timed out waiting for torrent metadata")
		}
	}
	files := t.Files()
	if len(files) == 0 {
		return nil, -1, fmt.Errorf("torrent has no files")
	}
	metas := make([]fileMeta, len(files))
	for i, f := range files {
		metas[i] = fileMeta{Path: f.DisplayPath(), Length: f.Length()}
	}
	idx := selectPrimaryIndex(metas)
	if idx < 0 {
		return nil, -1, fmt.Errorf("torrent has no playable file")
	}
	return mt, idx, nil
}

// Prewarm reads a torrent file to completion so anacrolix promotes its
// ".part" scratch file to its final on-disk name, then returns that absolute
// path. Used for small ancillary files (subtitles) that the player needs as
// real local files rather than HTTP streams — IINA's subtitle loader rejects
// HTTP URLs with "Unsupported external subtitles".
//
// Reading is intentionally done WITHOUT registering the reader in
// mt.readers — Prewarm reads the file to completion via io.Copy on its own
// schedule, so it should not be visible to anything that iterates
// mt.readers (currently only gcLoop and /debug/memstats).
func (e *Engine) Prewarm(ctx context.Context, ih string, idx int) (string, error) {
	mt := e.get(ih)
	if mt == nil {
		return "", fmt.Errorf("torrent not found")
	}
	e.touch(ih)
	files := mt.t.Load().Files()
	if idx < 0 || idx >= len(files) {
		return "", fmt.Errorf("file index out of range")
	}
	f := files[idx]

	r := f.NewReader()
	defer r.Close()
	r.SetResponsive()
	r.SetReadahead(e.readahead)

	cr := &contextReader{ctx: ctx, r: r}
	if _, err := io.Copy(io.Discard, cr); err != nil {
		return "", fmt.Errorf("prewarm read: %w", err)
	}

	// f.Path() already includes the torrent's root directory name (for
	// multi-file torrents it returns "<TorrentName>/<filepath>"); joining
	// with cacheDir gives the absolute on-disk path anacrolix writes to.
	finalPath := filepath.Join(e.cacheDir, f.Path())

	// After the final piece passes hash verification, anacrolix renames
	// "<final>.part" → "<final>" asynchronously. Poll briefly for that.
	deadline := time.Now().Add(prewarmPollTimeout)
	for {
		if _, err := os.Stat(finalPath); err == nil {
			return finalPath, nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("file did not appear at %s within timeout", finalPath)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(prewarmPollInterval):
		}
	}
}

// warmRegion describes a contiguous byte range of a file to prewarm in the
// background, plus the idempotency map that guards re-entry for that region
// kind. The caller holds e.mu when reading/writing the flag map.
type warmRegion struct {
	name   string // "head" or "tail" — used only in failure logs
	offset int64
	length int64
	flag   map[int]bool // mt.warmed (tail) or mt.warmedHead (head)
}

// warmRegionAsync schedules a one-shot background read of a file region so
// the swarm prioritises those pieces. The reader is registered in mt.readers
// for its lifetime so gcLoop will not evict the torrent mid-warm. Returns
// true if a warm-up was actually scheduled, false if this region was already
// warmed or in flight (de-duplicated via region.flag[idx]).
func (e *Engine) warmRegionAsync(mt *managedTorrent, idx int, f *torrent.File, region warmRegion) bool {
	e.mu.Lock()
	if mt.paused || region.flag[idx] {
		e.mu.Unlock()
		return false
	}
	warmCtx := mt.warmCtx
	if warmCtx == nil {
		warmCtx = e.ctx
	}
	if warmCtx == nil {
		warmCtx = context.Background()
	}
	region.flag[idx] = true
	e.mu.Unlock()

	if !e.startWorker(func() { e.runWarmReader(mt, f.NewReader(), region, idx, warmCtx) }) {
		e.mu.Lock()
		delete(region.flag, idx)
		e.mu.Unlock()
		return false
	}
	return true
}

// runWarmReader executes the actual warm-up read against an already-built
// torrent.Reader: registers it, applies a region-sized readahead, seeks, and
// reads region.length bytes. Split out from warmRegionAsync so tests can
// supply a fake reader.
func (e *Engine) runWarmReader(
	mt *managedTorrent,
	tr torrent.Reader,
	region warmRegion,
	idx int,
	ctx context.Context,
) {
	defer tr.Close()
	tr.SetResponsive()
	e.mu.Lock()
	mt.readers = append(mt.readers, managedReader{reader: tr, readahead: region.length})
	// Cap readahead at the region length. The full engine-wide readahead
	// (often 512 MiB) here would mark hundreds of MB around the seek
	// position as "wanted", stealing peer bandwidth from the active
	// stream for data nobody reads.
	if mt.paused {
		tr.SetReadahead(0)
	} else {
		tr.SetReadahead(region.length)
	}
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		for i, mr := range mt.readers {
			if mr.reader == tr {
				mt.readers = append(mt.readers[:i], mt.readers[i+1:]...)
				break
			}
		}
		mt.lastActiveAt = time.Now()
		e.mu.Unlock()
	}()
	if region.offset > 0 {
		if _, err := tr.Seek(region.offset, io.SeekStart); err != nil {
			// Allow a retry on the next call — otherwise a transient
			// seek failure leaves the region pinned as "warmed" forever.
			e.mu.Lock()
			delete(region.flag, idx)
			e.mu.Unlock()
			log.Printf("warm-%s: seek of file idx=%d failed: %v", region.name, idx, err)
			return
		}
	}
	cr := &contextReader{ctx: ctx, r: tr}
	if _, err := io.CopyN(io.Discard, cr, region.length); err != nil {
		e.mu.Lock()
		delete(region.flag, idx)
		e.mu.Unlock()
		log.Printf("warm-%s: copy of file idx=%d failed: %v", region.name, idx, err)
	}
}

// warmTail eagerly fetches the tail of a file once, so containers that store
// their index at end-of-file can begin playback without first downloading the
// whole file sequentially.
func (e *Engine) warmTail(mt *managedTorrent, idx int, f *torrent.File) {
	e.warmRegionAsync(mt, idx, f, warmRegion{
		name:   "tail",
		offset: max(f.Length()-tailWarmBytes, 0),
		length: tailWarmBytes,
		flag:   mt.warmed,
	})
}

// warmNextHealthThreshold is the minimum contiguous bytes ahead of the
// active reader required to permit a 128 MiB next-episode head warm. Below
// this, the head warm is deferred to avoid stealing bandwidth from the
// currently-playing file when the network is unable to keep up. 32 MiB ≈
// 25 s of HD video at 10 Mbps — long enough to ride out a brief request
// burst without bleeding the active stream's lookahead dry.
const warmNextHealthThreshold = 32 << 20

// WarmNext schedules a background prewarm of the next video file in the
// torrent after afterIdx — its head (headWarmBytes) and tail (tailWarmBytes).
// Returns the chosen file index, whether the head warm-up was actually
// started, and whether it was deferred for bandwidth reasons (callers should
// retry on a deferred result). Triggered by the plugin once playback of the
// current episode crosses ~90 %, so the next episode's first frames +
// container index are ready when mpv switches playlist items.
//
// currentOffset is the byte position of the active reader inside the file at
// afterIdx, as reported by the plugin from mpv's stream-pos. When > 0 the
// head warm is gated on contiguousBytesAhead being healthy enough that the
// extra ~128 MiB of background traffic won't starve the active stream. Pass
// 0 to bypass the gate (legacy behavior).
func (e *Engine) WarmNext(ih string, afterIdx int, currentOffset int64) (int, bool, bool, error) {
	mt := e.get(ih)
	if mt == nil {
		return -1, false, false, fmt.Errorf("torrent not found")
	}
	t := mt.t.Load()
	if t.Info() == nil {
		// No metadata yet — nothing to warm. Caller can retry once status
		// reports a non-empty file list.
		return -1, false, false, nil
	}
	files := t.Files()
	paths := make([]string, len(files))
	for i, f := range files {
		paths[i] = f.DisplayPath()
	}
	nextIdx := nextVideoIndex(paths, afterIdx)
	if nextIdx < 0 {
		return -1, false, false, nil
	}
	e.touch(ih)
	f := files[nextIdx]
	e.mu.Lock()
	paused := mt.paused
	e.mu.Unlock()
	if paused {
		return nextIdx, false, true, nil
	}

	// Always warm the tail (4 MiB) first — it's cheap and carries the
	// container index for mkv/mp4, without which mpv stalls on its first
	// read when it switches to the next file. warmRegionAsync is idempotent
	// on mt.warmed, so this is a no-op if Stream already warmed it.
	e.warmRegionAsync(mt, nextIdx, f, warmRegion{
		name:   "tail",
		offset: max(f.Length()-tailWarmBytes, 0),
		length: tailWarmBytes,
		flag:   mt.warmed,
	})

	// Gate the 128 MiB head warm on the active stream's buffer health. When
	// the current file at afterIdx has less than warmNextHealthThreshold
	// bytes of contiguous data ahead of the play cursor, starting another
	// large background fetch would steal bandwidth from the file the user is
	// actually watching. Defer; the plugin polls every 5 s and will retry.
	if currentOffset > 0 && afterIdx >= 0 && afterIdx < len(files) {
		ahead := contiguousBytesAhead(t, afterIdx, currentOffset)
		if ahead < warmNextHealthThreshold {
			log.Printf("warm-next: torrent %s deferred (current idx=%d offset=%d contiguous_ahead=%d < %d)",
				ih, afterIdx, currentOffset, ahead, warmNextHealthThreshold)
			return nextIdx, false, true, nil
		}
	}

	started := e.warmRegionAsync(mt, nextIdx, f, warmRegion{
		name:   "head",
		offset: 0,
		length: min(f.Length(), headWarmBytes),
		flag:   mt.warmedHead,
	})
	if started {
		log.Printf("warm-next: torrent %s file idx=%d (%s) scheduled", ih, nextIdx, f.DisplayPath())
	}
	return nextIdx, started, false, nil
}

// contiguousBytesAhead reports how many bytes are immediately readable from a
// file starting at fromOffset — that is, the size of the contiguous prefix of
// completed pieces beginning at the piece that contains fromOffset. Returns 0
// when the piece containing fromOffset is not yet complete, when metadata is
// missing, or when fromOffset is out of range. Used by WarmNext to gate the
// next-episode head-warm, and by future buffer-ahead diagnostics.
func contiguousBytesAhead(t *torrent.Torrent, fileIdx int, fromOffset int64) int64 {
	info := t.Info()
	if info == nil {
		return 0
	}
	files := t.Files()
	if fileIdx < 0 || fileIdx >= len(files) {
		return 0
	}
	f := files[fileIdx]
	return contiguousBytesAheadCore(
		info.PieceLength,
		f.Offset(),
		f.Length(),
		fromOffset,
		t.NumPieces(),
		func(pi int) bool { return t.PieceState(pi).Complete },
	)
}

// contiguousBytesAheadCore is the pure-logic implementation of
// contiguousBytesAhead, kept separate so it can be unit-tested without
// standing up an anacrolix client. isComplete reports whether a piece by
// index is fully downloaded and verified.
func contiguousBytesAheadCore(
	pieceLen, fileOffset, fileLength, fromOffset int64,
	numPieces int,
	isComplete func(pieceIdx int) bool,
) int64 {
	if pieceLen <= 0 || fileLength <= 0 || fromOffset < 0 || fromOffset >= fileLength {
		return 0
	}
	absStart := fileOffset + fromOffset
	fileEnd := fileOffset + fileLength
	startPiece := int(absStart / pieceLen)

	lastCompleteEnd := absStart
	for pi := startPiece; pi < numPieces; pi++ {
		if !isComplete(pi) {
			break
		}
		pieceEnd := int64(pi+1) * pieceLen
		lastCompleteEnd = pieceEnd
		if pieceEnd >= fileEnd {
			break
		}
	}
	if lastCompleteEnd > fileEnd {
		lastCompleteEnd = fileEnd
	}
	if lastCompleteEnd <= absStart {
		return 0
	}
	return lastCompleteEnd - absStart
}

// contextReader adapts a torrent.Reader to honour an HTTP request's context, so
// a read aborts promptly when the player drops the connection on a seek.
type contextReader struct {
	ctx context.Context
	r   torrent.Reader
}

func (cr *contextReader) Read(p []byte) (int, error) {
	return cr.r.ReadContext(cr.ctx, p)
}

func (cr *contextReader) Seek(offset int64, whence int) (int64, error) {
	return cr.r.Seek(offset, whence)
}

// sampleLoop refreshes per-torrent download/upload rate estimates every second.
// It returns when Close signals via e.done — without this the goroutine would
// outlive the torrent client and keep poking at freed state.
//
// We snapshot the torrent pointers under e.mu, then call mt.t.Stats()
// (a potentially slow anacrolix call) WITHOUT holding the central engine
// lock, so streaming/add/pause operations are not blocked behind us. The
// per-torrent rate fields are then updated under a short lock per torrent.
func (e *Engine) sampleLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			e.mu.Lock()
			snapshot := make([]*managedTorrent, 0, len(e.torrents))
			for _, mt := range e.torrents {
				snapshot = append(snapshot, mt)
			}
			e.mu.Unlock()

			now := time.Now()
			for _, mt := range snapshot {
				stats := mt.t.Load().Stats()
				read := stats.BytesReadData.Int64()
				written := stats.BytesWrittenData.Int64()
				e.mu.Lock()
				if dt := now.Sub(mt.lastSampleAt).Seconds(); dt > 0 {
					mt.downloadRate = int64(float64(read-mt.lastRead) / dt)
					mt.uploadRate = int64(float64(written-mt.lastWritten) / dt)
				}
				mt.lastRead, mt.lastWritten, mt.lastSampleAt = read, written, now
				e.mu.Unlock()
			}
		}
	}
}

// FileStatus describes a single file within a torrent.
type FileStatus struct {
	Index          int    `json:"index"`
	Path           string `json:"path"`
	Length         int64  `json:"length"`
	BytesCompleted int64  `json:"bytesCompleted"`
	Mime           string `json:"mime"`
	IsVideo        bool   `json:"isVideo"`
	IsSubtitle     bool   `json:"isSubtitle"`
}

// TorrentStatus is the JSON payload returned by /torrents and /torrents/{ih}.
type TorrentStatus struct {
	InfoHash       string       `json:"infohash"`
	Name           string       `json:"name"`
	Length         int64        `json:"length"`
	BytesCompleted int64        `json:"bytesCompleted"`
	Progress       float64      `json:"progress"`
	DownloadRate   int64        `json:"downloadRate"`
	UploadRate     int64        `json:"uploadRate"`
	Peers          int          `json:"peers"`
	ActivePeers    int          `json:"activePeers"`
	Seeders        int          `json:"seeders"`
	PieceCount     int          `json:"pieceCount"`
	PrimaryIndex   int          `json:"primaryIndex"`
	Files          []FileStatus `json:"files"`
}

// Status builds the current status snapshot for a torrent. When the torrent's
// metadata has not arrived yet (a magnet still resolving), the returned
// snapshot contains just the infohash and Files is nil — the plugin uses that
// as the signal to keep polling.
func (e *Engine) Status(ih string) (*TorrentStatus, bool) {
	mt := e.get(ih)
	if mt == nil {
		return nil, false
	}
	t := mt.t.Load()
	stats := t.Stats()

	st := &TorrentStatus{
		InfoHash:    ih,
		Name:        t.Name(),
		Peers:       stats.TotalPeers,
		ActivePeers: stats.ActivePeers,
		Seeders:     stats.ConnectedSeeders,
	}
	e.mu.Lock()
	st.DownloadRate = mt.downloadRate
	st.UploadRate = mt.uploadRate
	// Status polling is the primary "this torrent is still wanted" signal
	// the plugin emits while the sidebar is visible. Update last-active
	// here so gcLoop never evicts a torrent the user is actively watching.
	mt.lastActiveAt = time.Now()
	e.mu.Unlock()

	// Anything below requires the torrent's info dictionary — accessing
	// t.Files()/t.Length()/t.NumPieces() before GotInfo fires nil-derefs.
	if t.Info() == nil {
		return st, true
	}

	length := t.Length()
	completed := t.BytesCompleted()
	progress := 0.0
	if length > 0 {
		progress = float64(completed) / float64(length)
	}
	st.Length = length
	st.BytesCompleted = completed
	st.Progress = progress
	st.PieceCount = t.NumPieces()

	files := t.Files()
	metas := make([]fileMeta, len(files))
	for i, f := range files {
		st.Files = append(st.Files, FileStatus{
			Index:          i,
			Path:           f.DisplayPath(),
			Length:         f.Length(),
			BytesCompleted: f.BytesCompleted(),
			Mime:           mimeForPath(f.DisplayPath()),
			IsVideo:        isVideoFile(f.DisplayPath()),
			IsSubtitle:     isSubtitleFile(f.DisplayPath()),
		})
		metas[i] = fileMeta{Path: f.DisplayPath(), Length: f.Length()}
	}
	st.PrimaryIndex = selectPrimaryIndex(metas)
	return st, true
}

// loadRemoteMetainfo downloads and parses a .torrent file from an HTTP(S) URL.
//
// Defensive: a dedicated http.Client (not DefaultClient) caps the wall-clock
// budget, limits redirect chains, and the response body is wrapped in an
// io.LimitReader so a malicious server cannot stream gigabytes of data into
// the bencode parser. Combined with the caller's context this gives us:
//   - bounded time         (Client.Timeout + ctx)
//   - bounded redirects    (CheckRedirect)
//   - bounded payload size (io.LimitReader)
func loadRemoteMetainfo(ctx context.Context, url string) (*metainfo.MetaInfo, error) {
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, fmt.Errorf("split remote address: %w", err)
			}
			ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
			if err != nil {
				return nil, fmt.Errorf("resolve remote host: %w", err)
			}
			dialer := &net.Dialer{}
			// Try every public address before giving up, the way the stdlib
			// dialer would: a host whose first record is dead (or an AAAA
			// answer on an IPv4-only network) must not fail the whole dial.
			var (
				tried   int
				lastErr error
			)
			for _, ip := range ips {
				if !isPublicRemoteIP(ip) {
					continue
				}
				tried++
				conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			if tried == 0 {
				return nil, fmt.Errorf("remote torrent URL resolves only to private or local addresses")
			}
			return nil, fmt.Errorf("dial remote host: %w", lastErr)
		},
	}
	return loadRemoteMetainfoWithClient(ctx, url, newMetainfoHTTPClient(transport))
}

func newMetainfoHTTPClient(transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: transport,
		Timeout:   remoteMetainfoTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= remoteMetainfoMaxRedirects {
				return fmt.Errorf("too many redirects (max %d)", remoteMetainfoMaxRedirects)
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("unsupported redirect scheme %q", req.URL.Scheme)
			}
			return nil
		},
	}
}

func loadRemoteMetainfoWithClient(
	ctx context.Context,
	source string,
	client *http.Client,
) (*metainfo.MetaInfo, error) {
	parsed, err := url.Parse(source)
	if err != nil {
		return nil, err
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return nil, fmt.Errorf("remote torrent URL must use http or https")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download torrent file: HTTP %d", resp.StatusCode)
	}
	// Read at most remoteMetainfoMaxBytes+1 — if the limit reader returns
	// strictly more than the cap, the body was too large and we refuse it
	// rather than silently truncating (a truncated bencode would still
	// parse cleanly into a partial metainfo otherwise).
	body := io.LimitReader(resp.Body, remoteMetainfoMaxBytes+1)
	mi, err := metainfo.Load(body)
	if err != nil {
		return nil, fmt.Errorf("parse torrent file: %w", err)
	}
	// Probe for trailing bytes: if Load consumed exactly remoteMetainfoMaxBytes
	// or more, the source likely exceeded the cap. Reading one more byte is
	// a cheap way to detect that.
	var probe [1]byte
	if n, _ := resp.Body.Read(probe[:]); n > 0 {
		return nil, fmt.Errorf("torrent file exceeds %d-byte cap", remoteMetainfoMaxBytes)
	}
	return mi, nil
}

func isPublicRemoteIP(ip net.IP) bool {
	if ip == nil ||
		!ip.IsGlobalUnicast() ||
		ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() {
		return false
	}
	// Carrier-grade NAT is not classified as private by net.IP.IsPrivate, but
	// it is still an internal address range and must not be reachable via SSRF.
	if v4 := ip.To4(); v4 != nil {
		return !(v4[0] == 100 && v4[1]&0xc0 == 0x40)
	}
	return true
}

// fileMeta is the minimal file description used for primary-file selection.
type fileMeta struct {
	Path   string
	Length int64
}

// selectPrimaryIndex picks the file that should play by default: the largest
// video file, or — if the torrent contains no video — the largest file overall.
// It returns -1 for an empty file list.
func selectPrimaryIndex(files []fileMeta) int {
	best := -1
	var bestLen int64 = -1
	for i, f := range files {
		if isVideoFile(f.Path) && f.Length > bestLen {
			best, bestLen = i, f.Length
		}
	}
	if best >= 0 {
		return best
	}
	for i, f := range files {
		if f.Length > bestLen {
			best, bestLen = i, f.Length
		}
	}
	return best
}

// nextVideoIndex returns the index of the next video file strictly after
// afterIdx, or -1 if none. Non-video files (subtitles, .txt, etc.) between
// afterIdx and the next video are skipped. afterIdx may be -1 to search from
// the beginning, and out-of-range values yield -1.
func nextVideoIndex(paths []string, afterIdx int) int {
	start := afterIdx + 1
	if start < 0 {
		start = 0
	}
	for i := start; i < len(paths); i++ {
		if isVideoFile(paths[i]) {
			return i
		}
	}
	return -1
}

// videoMimes maps known video file extensions to their MIME types.
var videoMimes = map[string]string{
	".mp4":  "video/mp4",
	".m4v":  "video/x-m4v",
	".mkv":  "video/x-matroska",
	".webm": "video/webm",
	".avi":  "video/x-msvideo",
	".mov":  "video/quicktime",
	".flv":  "video/x-flv",
	".wmv":  "video/x-ms-wmv",
	".mpg":  "video/mpeg",
	".mpeg": "video/mpeg",
	".ts":   "video/mp2t",
	".m2ts": "video/mp2t",
	".ogv":  "video/ogg",
	".3gp":  "video/3gpp",
}

// isVideoFile reports whether a path looks like a playable video file.
func isVideoFile(name string) bool {
	_, ok := videoMimes[strings.ToLower(filepath.Ext(name))]
	return ok
}

// subtitleExts lists the file extensions recognised as external subtitle
// tracks. Kept in lock-step with the IINA subtitle loader's accepted formats
// (.srt/.ass/.ssa/.vtt/.sub). The plugin filters files via FileStatus.IsSubtitle
// so this is the single source of truth for the classification.
var subtitleExts = map[string]bool{
	".srt": true,
	".ass": true,
	".ssa": true,
	".vtt": true,
	".sub": true,
}

// isSubtitleFile reports whether a path looks like an external subtitle file.
func isSubtitleFile(name string) bool {
	return subtitleExts[strings.ToLower(filepath.Ext(name))]
}

// mimeForPath returns the best-guess MIME type for a file path.
func mimeForPath(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	if m, ok := videoMimes[ext]; ok {
		return m
	}
	if m := mime.TypeByExtension(ext); m != "" {
		return m
	}
	return "application/octet-stream"
}

// recoveryAction is the verdict from a single watchdog tick.
type recoveryAction int

const (
	recoveryNone     recoveryAction = iota // sum == 0, stay quiet
	recoveryReport                         // 0 < sum < threshold, log only
	recoveryCoolDown                       // sum >= threshold but inside backoff
	recoveryRebuild                        // sum >= threshold and outside backoff
)

// recoveryDecision is the pure-logic policy of one watchdog tick: given the
// rolling-window sum and the time elapsed since the last rebuild, decide
// what (if anything) to do. Extracted so it can be unit-tested without
// standing up a torrent client.
func recoveryDecision(sum int64, sinceLastRebuild time.Duration, threshold int64, backoff time.Duration) recoveryAction {
	switch {
	case sum >= threshold && sinceLastRebuild >= backoff:
		return recoveryRebuild
	case sum >= threshold:
		return recoveryCoolDown
	case sum > 0:
		return recoveryReport
	default:
		return recoveryNone
	}
}

// recoveryLoop is the network-stall watchdog. Every recoveryCheckInterval it
// drains the resendErrorMonitor counter into a rolling window and triggers a
// torrent.Client rebuild when the window sum crosses recoveryErrorThreshold —
// the post-VPN-flap / post-network-change / post-sleep state where every uTP
// send returns ENETUNREACH and the daemon would otherwise never recover.
// recoveryBackoff bounds how often a rebuild can fire so a persistent network
// outage does not become an infinite rebuild loop.
func (e *Engine) recoveryLoop() {
	ticker := time.NewTicker(recoveryCheckInterval)
	defer ticker.Stop()
	var (
		window      [recoveryWindowBuckets]int64
		idx         int
		lastRebuild time.Time
	)
	windowSeconds := int(recoveryCheckInterval.Seconds()) * recoveryWindowBuckets
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			n := e.monitor.LoadAndReset()
			window[idx] = n
			idx = (idx + 1) % recoveryWindowBuckets
			var sum int64
			for _, v := range window {
				sum += v
			}
			// On the very first tick lastRebuild is the zero value, so
			// time.Since returns a huge duration that always passes the
			// backoff check — exactly what we want.
			switch recoveryDecision(sum, time.Since(lastRebuild), recoveryErrorThreshold, recoveryBackoff) {
			case recoveryRebuild:
				log.Printf("network watchdog: %d uTP resend errors in %ds — rebuilding torrent client", sum, windowSeconds)
				e.recreateClient()
				lastRebuild = time.Now()
				// Clear the window so the next decision is based on fresh
				// data; otherwise we would immediately re-trigger.
				for i := range window {
					window[i] = 0
				}
			case recoveryCoolDown:
				log.Printf("network watchdog: %d uTP resend errors in %ds (cool-down active, no rebuild)", sum, windowSeconds)
			case recoveryReport:
				log.Printf("network watchdog: %d uTP resend errors suppressed in %ds (below threshold %d)",
					sum, windowSeconds, recoveryErrorThreshold)
			case recoveryNone:
				// nothing to do
			}
		}
	}
}

// recreateClient builds a replacement torrent.Client against the same storage
// before tearing down the current generation (so a build failure leaves the
// working client intact, while already-downloaded pieces and BoltDB
// piece-completion survive). Every still-tracked managedTorrent is re-added
// to the new client by infohash; its metainfo (when info bytes are present)
// is re-supplied so the new torrent has the file list immediately.
// In-flight streaming readers anchored on the previous torrent.Torrent will
// return EOF/error on their next Read once the old client is closed — the
// HTTP handler unwinds, mpv reconnects through /play and lands on the new
// torrent. Engine state (mt.warmed, mt.lastActiveAt, etc.) is preserved
// across the swap because we keep the managedTorrent value and only swap
// its embedded atomic.Pointer.
func (e *Engine) recreateClient() {
	e.clientMu.Lock()
	defer e.clientMu.Unlock()
	e.workerMu.Lock()
	closed := e.closed
	e.workerMu.Unlock()
	if closed {
		return
	}

	e.mu.Lock()
	snaps := make([]reAddSnapshot, 0, len(e.torrents))
	for ih, mt := range e.torrents {
		t := mt.t.Load()
		if t == nil {
			continue
		}
		snaps = append(snaps, reAddSnapshot{
			ih:      ih,
			mt:      mt,
			mi:      t.Metainfo(),
			hasInfo: t.Info() != nil,
		})
	}
	e.mu.Unlock()

	// Build first so a transient bind/configuration failure leaves the current
	// client usable. ListenPort=0 gives the replacement independent sockets.
	newClient, err := buildTorrentClient(e.cacheDir, e.storage, e.seed)
	if err != nil {
		log.Printf("network watchdog: building replacement torrent client failed: %v; keeping current client", err)
		return
	}
	oldClient := e.client.Load()
	if oldClient != nil {
		oldClient.Close()
	}
	e.client.Store(newClient)

	var readded, failed int
	for _, s := range snaps {
		newT, ok := e.readdToClient(newClient, s)
		if !ok {
			// No live handle could be obtained on the new client. Stop
			// serving a stale handle from the now-closed client: drop the
			// torrent from tracking so its endpoints honestly 404 and the
			// next /play recreates it fresh, rather than freezing forever on
			// dead status / EOF reads.
			e.mu.Lock()
			var cancelWarm context.CancelFunc
			if e.torrents[s.ih] == s.mt {
				delete(e.torrents, s.ih)
				cancelWarm = s.mt.cancelWarm
			}
			e.mu.Unlock()
			// Same teardown as every other drop path: warm workers would
			// otherwise keep their context and stay in e.wg until their reads
			// against the closed client error out.
			if cancelWarm != nil {
				cancelWarm()
			}
			failed++
			continue
		}
		e.mu.Lock()
		if e.torrents[s.ih] != s.mt {
			e.mu.Unlock()
			newT.Drop()
			continue
		}
		s.mt.t.Store(newT)
		e.mu.Unlock()
		// Re-arm the metadata watcher so public trackers attach again once
		// info arrives on the new client (same as Add does for fresh torrents).
		mt := s.mt
		e.startWorker(func() { e.completeMetadata(mt) })
		readded++
	}
	log.Printf("network watchdog: client rebuilt, %d torrent(s) re-added, %d failed", readded, failed)
}

// reAddSnapshot captures the minimum a tracked torrent needs to be
// re-registered on a freshly rebuilt client (see recreateClient /
// readdToClient). mi is a copy of the torrent's metainfo taken before the old
// client is closed.
type reAddSnapshot struct {
	ih      string
	mt      *managedTorrent
	mi      metainfo.MetaInfo
	hasInfo bool
}

// resolveInfohash returns the torrent's infohash from its snapshot, preferring
// the info bytes (set once metadata is known) and falling back to the hex map
// key. The info-bytes branch is gated on len(InfoBytes) > 0 because
// HashInfoBytes() is SHA1(InfoBytes): for an info-less magnet it would return
// SHA1("") (a non-zero hash) rather than the real infohash, so .IsZero() is not
// a usable guard. ok is false only when neither source yields a parseable hash
// — which cannot happen for a tracked torrent whose key is its hex infohash,
// but is handled so callers never use a bogus hash.
func resolveInfohash(ih string, mi *metainfo.MetaInfo) (h metainfo.Hash, ok bool) {
	if len(mi.InfoBytes) > 0 {
		return mi.HashInfoBytes(), true
	}
	if err := h.FromHexString(ih); err != nil {
		return metainfo.Hash{}, false
	}
	return h, true
}

// readdToClient re-registers a snapshotted torrent on the rebuilt client and
// returns a live handle. It first tries the full-metainfo path, which preserves
// genuine v2/hybrid piece layers; on ANY error there it falls back to an
// infohash-only add and re-supplies the info bytes. The fallback cannot hit the
// piece-layer validation that the full path can, so it yields a live torrent
// that re-verifies the surviving on-disk pieces and resumes downloading instead
// of leaving the torrent orphaned until a daemon restart. ok is false only when
// the infohash itself cannot be resolved (so the caller drops the torrent
// rather than keep a dead handle).
func (e *Engine) readdToClient(cl *torrent.Client, s reAddSnapshot) (*torrent.Torrent, bool) {
	if s.hasInfo {
		// t.Metainfo() reconstructs PieceLayers via pieceLayers(), which always
		// allocates a non-nil map even for v1 torrents that have none.
		// anacrolix's addPieceLayersLocked only short-circuits on a nil map, so
		// an empty-but-non-nil one errors "no piece root set for file" on every
		// v1 file with >1 piece (and AddTorrentSpec then Drops the torrent).
		// Nil the empty map so v1 re-adds cleanly; real v2/hybrid layers are
		// non-empty and preserved.
		mi := s.mi
		if len(mi.PieceLayers) == 0 {
			mi.PieceLayers = nil
		}
		t, err := cl.AddTorrent(&mi)
		if err == nil {
			return t, true
		}
		log.Printf("network watchdog: AddTorrent for %s failed (%v); falling back to infohash add", s.ih, err)
	}

	h, ok := resolveInfohash(s.ih, &s.mi)
	if !ok {
		log.Printf("network watchdog: cannot resolve infohash for %q; dropping torrent", s.ih)
		return nil, false
	}
	t, _ := cl.AddTorrentInfoHash(h)
	// Re-supply known metadata so the torrent has its file list immediately and
	// matches the surviving on-disk pieces; harmless no-op when info is absent.
	if len(s.mi.InfoBytes) > 0 {
		if err := t.SetInfoBytes(s.mi.InfoBytes); err != nil {
			log.Printf("network watchdog: re-supplying info bytes for %s failed: %v", s.ih, err)
		}
	}
	return t, true
}
