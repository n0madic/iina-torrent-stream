package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
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
	client *torrent.Client
	// storage is held so Close can shut down the piece-completion DB. The
	// torrent client does not close its default storage on its own.
	storage storage.ClientImplCloser

	// cacheDir is the on-disk root anacrolix writes torrent files to. We use it
	// to construct absolute paths for callers that want to read a torrent file
	// directly from disk (subtitle pre-warm — IINA's subtitle loader requires a
	// local file path with a recognised extension).
	cacheDir string

	// readahead is the look-ahead window size applied to every streaming
	// reader — how much data is kept prioritised ahead of the play cursor.
	readahead int64

	// done is closed by Close; sampleLoop watches it so the rate-sampler goroutine
	// exits cleanly instead of running after the underlying client is gone.
	done chan struct{}
	// wg tracks all goroutines spawned by the engine (sampleLoop +
	// completeMetadata workers) so Close can wait for them to exit before it
	// closes the torrent client. Without this wait, sampleLoop could call
	// mt.t.Stats() and completeMetadata could call t.AddTrackers concurrently
	// with client.Close — anacrolix does not document those as safe.
	wg sync.WaitGroup

	mu       sync.Mutex
	torrents map[string]*managedTorrent
}

// managedTorrent holds per-torrent bookkeeping, including download-rate samples.
type managedTorrent struct {
	t          *torrent.Torrent
	addedAt    time.Time
	warmed     map[int]bool // file indexes whose tail has already been warmed
	warmedHead map[int]bool // file indexes whose head has already been warmed

	// readers tracks the live streaming readers. gcLoop uses len(readers) > 0
	// to decide whether a torrent is still in use; /debug/memstats sums the
	// counts across torrents.
	readers []torrent.Reader

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

// NewEngine creates the torrent client and sets the streaming read-ahead
// window. readaheadBytes is the requested look-ahead size; values outside
// [readaheadMin, readaheadMax] are clamped.
func NewEngine(dataDir, cacheDir string, readaheadBytes int64, seed bool) (*Engine, error) {
	store := newStreamStorage(cacheDir)

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
		_ = store.Close()
		return nil, fmt.Errorf("create torrent client: %w", err)
	}

	// Clamp the configured read-ahead window into a sane range. The window is
	// what keeps the whole peer swarm busy — too small and most peers go idle.
	readahead := max(readaheadBytes, readaheadMin)
	if readahead > readaheadMax {
		readahead = readaheadMax
	}

	e := &Engine{
		client:    client,
		storage:   store,
		cacheDir:  cacheDir,
		readahead: readahead,
		done:      make(chan struct{}),
		torrents:  make(map[string]*managedTorrent),
	}
	e.wg.Add(2)
	go func() { defer e.wg.Done(); e.sampleLoop() }()
	go func() { defer e.wg.Done(); e.gcLoop() }()
	return e, nil
}

// Close shuts the torrent client and the underlying piece-completion DB down.
// Engine goroutines (sampleLoop, completeMetadata workers) are signalled via
// e.done and then waited on before the client is closed — calling
// client.Close while sampleLoop is still inside mt.t.Stats(), or while a
// completeMetadata worker is inside t.AddTrackers, races on anacrolix internals.
// Closing the storage explicitly is what releases the BoltDB writer on the
// completion DB — torrent.Client.Close does not close the default storage.
func (e *Engine) Close() {
	close(e.done)
	e.wg.Wait()
	e.client.Close()
	if err := e.storage.Close(); err != nil {
		log.Printf("warning: close storage: %v", err)
	}
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
	var (
		t   *torrent.Torrent
		err error
	)
	switch {
	case strings.HasPrefix(source, "magnet:"):
		t, err = e.client.AddMagnet(source)
	case strings.HasPrefix(source, "http://"), strings.HasPrefix(source, "https://"):
		var mi *metainfo.MetaInfo
		mi, err = loadRemoteMetainfo(ctx, source)
		if err == nil {
			t, err = e.client.AddTorrent(mi)
		}
	default:
		var mi *metainfo.MetaInfo
		mi, err = metainfo.LoadFromFile(source)
		if err == nil {
			t, err = e.client.AddTorrent(mi)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("add torrent: %w", err)
	}

	ih := t.InfoHash().HexString()
	if mt := e.get(ih); mt != nil {
		return mt, nil
	}

	now := time.Now()
	mt := &managedTorrent{
		t:            t,
		addedAt:      now,
		warmed:       make(map[int]bool),
		warmedHead:   make(map[int]bool),
		lastSampleAt: now,
		lastActiveAt: now,
	}
	e.mu.Lock()
	e.torrents[ih] = mt
	e.mu.Unlock()
	log.Printf("added torrent %s (info ready=%v)", ih, t.Info() != nil)

	// Async: wait for metadata, attach public trackers once we know the
	// private bit, or drop the torrent on timeout so it does not linger in
	// anacrolix holding connections / DHT traffic forever.
	e.wg.Go(func() { ; e.completeMetadata(mt) })
	return mt, nil
}

// completeMetadata waits (with a long timeout) for the torrent's info to
// arrive, then attaches public trackers if appropriate. If metadata never
// arrives, the torrent is dropped from the engine.
func (e *Engine) completeMetadata(mt *managedTorrent) {
	ctx, cancel := context.WithTimeout(context.Background(), metadataTimeout)
	defer cancel()
	t := mt.t
	ih := t.InfoHash().HexString()
	select {
	case <-t.GotInfo():
		// Boost peer discovery by announcing to extra well-known trackers —
		// but only when the torrent is NOT marked private (BEP 27). Announcing
		// a private torrent to public trackers can get the user banned from
		// their private tracker, so respect the flag.
		if info := t.Info(); info == nil || info.Private == nil || !*info.Private {
			t.AddTrackers(publicTrackers)
		}
		log.Printf("torrent %s metadata ready (%q, %d files)", ih, t.Name(), len(t.Files()))
	case <-ctx.Done():
		log.Printf("torrent %s: metadata wait timed out, dropping", ih)
		e.mu.Lock()
		delete(e.torrents, ih)
		e.mu.Unlock()
		t.Drop()
	case <-e.done:
		return
	}
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
		ih string
		t  *torrent.Torrent
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
			ih string
			t  *torrent.Torrent
		}{ih, mt.t})
		delete(e.torrents, ih)
	}
	e.mu.Unlock()

	// Drop outside the lock — t.Drop closes peer connections and may take a
	// moment; we do not want to hold the central engine mutex through it.
	for _, d := range toDrop {
		log.Printf("torrent gc: evicting idle torrent %s", d.ih)
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

// Pause marks the torrent as touched by the plugin (the player went on
// pause) and returns false only when the torrent is not loaded. It does NOT
// shrink the readahead window — on slow links the user pauses precisely to
// let the buffer fill, so look-ahead downloading must keep going. Anacrolix
// stops requesting new pieces on its own once the window is satisfied.
// Refreshing lastActiveAt extends the gcLoop idle window so a brief gap
// between reader teardown and the next interaction cannot evict the torrent
// during a pause.
func (e *Engine) Pause(ih string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	mt, ok := e.torrents[ih]
	if !ok {
		return false
	}
	mt.lastActiveAt = time.Now()
	return true
}

// Resume is the symmetric counterpart to Pause — the plugin calls it on
// mpv's resume event. It only refreshes lastActiveAt; the readahead window
// was never lowered, so there is nothing to restore.
func (e *Engine) Resume(ih string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	mt, ok := e.torrents[ih]
	if !ok {
		return false
	}
	mt.lastActiveAt = time.Now()
	return true
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
	mt.t.Drop()
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
	files := mt.t.Files()
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
	mt.readers = append(mt.readers, reader)
	reader.SetReadahead(e.readahead)
	// Touch on stream start. While the reader is registered the gcLoop will
	// not evict because len(readers) > 0; this stamp covers the window
	// between unregistration and any next interaction.
	mt.lastActiveAt = time.Now()
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		for i, r := range mt.readers {
			if r == reader {
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
	t := mt.t
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
	files := mt.t.Files()
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
	if region.flag[idx] {
		e.mu.Unlock()
		return false
	}
	region.flag[idx] = true
	e.mu.Unlock()

	go e.runWarmReader(mt, f.NewReader(), region, idx)
	return true
}

// runWarmReader executes the actual warm-up read against an already-built
// torrent.Reader: registers it, applies a region-sized readahead, seeks, and
// reads region.length bytes. Split out from warmRegionAsync so tests can
// supply a fake reader.
func (e *Engine) runWarmReader(mt *managedTorrent, tr torrent.Reader, region warmRegion, idx int) {
	defer tr.Close()
	tr.SetResponsive()
	e.mu.Lock()
	mt.readers = append(mt.readers, tr)
	// Cap readahead at the region length. The full engine-wide readahead
	// (often 512 MiB) here would mark hundreds of MB around the seek
	// position as "wanted", stealing peer bandwidth from the active
	// stream for data nobody reads.
	tr.SetReadahead(region.length)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		for i, r := range mt.readers {
			if r == tr {
				mt.readers = append(mt.readers[:i], mt.readers[i+1:]...)
				break
			}
		}
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
	if _, err := io.CopyN(io.Discard, tr, region.length); err != nil {
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
	if mt.t.Info() == nil {
		// No metadata yet — nothing to warm. Caller can retry once status
		// reports a non-empty file list.
		return -1, false, false, nil
	}
	files := mt.t.Files()
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
		ahead := contiguousBytesAhead(mt.t, afterIdx, currentOffset)
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
				stats := mt.t.Stats()
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
	t := mt.t
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
	client := &http.Client{
		Timeout: remoteMetainfoTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= remoteMetainfoMaxRedirects {
				return fmt.Errorf("too many redirects (max %d)", remoteMetainfoMaxRedirects)
			}
			return nil
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
