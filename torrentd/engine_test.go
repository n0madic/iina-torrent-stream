package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

func TestIsVideoFile(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		{"Movie.mp4", true},
		{"Movie.MKV", true},
		{"clip.webm", true},
		{"show.S01E01.1080p.x265.mkv", true},
		{"archive.zip", false},
		{"subtitle.srt", false},
		{"noext", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isVideoFile(c.name); got != c.want {
			t.Errorf("isVideoFile(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestMimeForPath(t *testing.T) {
	t.Parallel()
	cases := []struct{ name, want string }{
		{"a.mp4", "video/mp4"},
		{"a.mkv", "video/x-matroska"},
		{"a.MKV", "video/x-matroska"},
		{"a.unknownext", "application/octet-stream"},
		{"noext", "application/octet-stream"},
	}
	for _, c := range cases {
		if got := mimeForPath(c.name); got != c.want {
			t.Errorf("mimeForPath(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestIsSubtitleFile(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		{"track.srt", true},
		{"track.SRT", true},
		{"track.ass", true},
		{"track.ssa", true},
		{"track.vtt", true},
		{"track.sub", true},
		{"track.mkv", false},
		{"track.txt", false},
		{"noext", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isSubtitleFile(c.name); got != c.want {
			t.Errorf("isSubtitleFile(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestLifecycle_TriggersAfterIdleTimeout(t *testing.T) {
	t.Parallel()
	lc := newLifecycle(20 * time.Millisecond)
	lc.monitorInterval = 5 * time.Millisecond
	go lc.monitor()
	select {
	case <-lc.shutdown:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("monitor did not trigger shutdown within deadline")
	}
}

func TestLifecycle_TouchKeepsAlive(t *testing.T) {
	t.Parallel()
	lc := newLifecycle(50 * time.Millisecond)
	lc.monitorInterval = 5 * time.Millisecond
	go lc.monitor()

	// Touch repeatedly for longer than idleTimeout — must NOT shut down.
	keepAlive := time.NewTicker(10 * time.Millisecond)
	defer keepAlive.Stop()
	deadline := time.After(150 * time.Millisecond)
loop:
	for {
		select {
		case <-keepAlive.C:
			lc.touch()
		case <-lc.shutdown:
			t.Fatal("monitor shut down despite ongoing activity")
		case <-deadline:
			break loop
		}
	}

	// Stop touching — shutdown must now trigger.
	select {
	case <-lc.shutdown:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("monitor did not eventually shut down after activity stopped")
	}
}

func TestLifecycle_TriggerIsIdempotent(t *testing.T) {
	t.Parallel()
	lc := newLifecycle(time.Hour)
	lc.trigger()
	lc.trigger() // must not panic on close(closed-chan)
	select {
	case <-lc.shutdown:
		// expected
	default:
		t.Fatal("trigger did not close shutdown channel")
	}
}

func TestLoadRemoteMetainfo_RejectsOversizedBody(t *testing.T) {
	t.Parallel()
	// Server streams more bytes than remoteMetainfoMaxBytes — load must
	// refuse instead of consuming the whole stream.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// 64 MiB of zero bytes — well over the 32 MiB cap.
		buf := make([]byte, 1<<20) // 1 MiB at a time
		for range 64 {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	_, err := loadRemoteMetainfo(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error for oversized body, got nil")
	}
}

func TestLoadRemoteMetainfo_RejectsTooManyRedirects(t *testing.T) {
	t.Parallel()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always redirect to itself — produces an unbounded chain.
		http.Redirect(w, r, srv.URL+"/next", http.StatusFound)
	}))
	defer srv.Close()

	_, err := loadRemoteMetainfo(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error for redirect loop, got nil")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Errorf("expected redirect error, got: %v", err)
	}
}

func TestLoadRemoteMetainfo_RejectsNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := loadRemoteMetainfo(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected HTTP 404 in error, got: %v", err)
	}
}

func TestNextVideoIndex(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		paths    []string
		afterIdx int
		want     int
	}{
		{
			name:     "next video right after afterIdx",
			paths:    []string{"a.mkv", "b.mkv", "c.mkv"},
			afterIdx: 0,
			want:     1,
		},
		{
			name:     "skips non-video files between videos",
			paths:    []string{"s01e01.mkv", "s01e01.srt", "readme.txt", "s01e02.mkv"},
			afterIdx: 0,
			want:     3,
		},
		{
			name:     "no video after afterIdx",
			paths:    []string{"s01e01.mkv", "s01e02.mkv"},
			afterIdx: 1,
			want:     -1,
		},
		{
			name:     "afterIdx beyond list",
			paths:    []string{"a.mkv"},
			afterIdx: 5,
			want:     -1,
		},
		{
			name:     "afterIdx=-1 searches from start",
			paths:    []string{"readme.txt", "s01e01.mkv"},
			afterIdx: -1,
			want:     1,
		},
		{
			name:     "empty list",
			paths:    nil,
			afterIdx: 0,
			want:     -1,
		},
	}
	for _, c := range cases {
		if got := nextVideoIndex(c.paths, c.afterIdx); got != c.want {
			t.Errorf("%s: nextVideoIndex = %d, want %d", c.name, got, c.want)
		}
	}
}

// fakeReader implements torrent.Reader for tests of runWarmReader. It
// records readahead/responsive/seek calls and serves `length` bytes worth
// of dummy data starting from the most recent Seek position — so the
// caller's io.CopyN(_, _, region.length) drains cleanly without EOF.
type fakeReader struct {
	mu              sync.Mutex
	length          int64 // total bytes available to Read after Seek
	pos             int64 // bytes already returned via Read
	readaheadCalls  []int64
	responsiveCalls int
	seekCalls       []int64
	closed          bool
}

func (f *fakeReader) SetContext(context.Context) {}
func (f *fakeReader) SetReadahead(n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readaheadCalls = append(f.readaheadCalls, n)
}
func (f *fakeReader) SetReadaheadFunc(torrent.ReadaheadFunc) {}
func (f *fakeReader) SetResponsive() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responsiveCalls++
}
func (f *fakeReader) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pos >= f.length {
		return 0, io.EOF
	}
	remaining := f.length - f.pos
	n := int64(len(p))
	if n > remaining {
		n = remaining
	}
	f.pos += n
	return int(n), nil
}
func (f *fakeReader) ReadContext(_ context.Context, p []byte) (int, error) {
	return f.Read(p)
}
func (f *fakeReader) Seek(offset int64, _ int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seekCalls = append(f.seekCalls, offset)
	f.pos = 0 // reset read counter; we model "length bytes starting here"
	return offset, nil
}
func (f *fakeReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func TestRunWarmReader_AppliesRegionSizedReadahead(t *testing.T) {
	t.Parallel()

	const (
		fileLen    = 1 << 30   // 1 GiB
		tailWarm   = 4 << 20   // 4 MiB
		engineWide = 512 << 20 // 512 MiB — what the bug applied
	)

	// Engine with a deliberately large engine-wide readahead, so a regression
	// to tr.SetReadahead(e.readahead) would be observable as != region.length.
	e := &Engine{
		readahead: engineWide,
		torrents:  map[string]*managedTorrent{},
	}
	mt := &managedTorrent{warmed: map[int]bool{}}

	fr := &fakeReader{length: tailWarm}
	region := warmRegion{
		name:   "tail",
		offset: fileLen - tailWarm,
		length: tailWarm,
		flag:   mt.warmed,
	}
	// Pre-seed the flag exactly like warmRegionAsync would, then run the
	// worker synchronously so we can assert on it without timing.
	region.flag[0] = true
	e.runWarmReader(mt, fr, region, 0)

	fr.mu.Lock()
	defer fr.mu.Unlock()
	if len(fr.readaheadCalls) != 1 {
		t.Fatalf("expected exactly one SetReadahead call, got %d (%v)", len(fr.readaheadCalls), fr.readaheadCalls)
	}
	if got := fr.readaheadCalls[0]; got != int64(tailWarm) {
		t.Errorf("SetReadahead = %d, want region.length=%d (not engine-wide %d)", got, tailWarm, engineWide)
	}
	if fr.responsiveCalls != 1 {
		t.Errorf("SetResponsive calls = %d, want 1", fr.responsiveCalls)
	}
	if !fr.closed {
		t.Error("reader was not closed")
	}
	if len(fr.seekCalls) != 1 || fr.seekCalls[0] != int64(fileLen-tailWarm) {
		t.Errorf("seek calls = %v, want [%d]", fr.seekCalls, fileLen-tailWarm)
	}
}

func TestContiguousBytesAheadCore(t *testing.T) {
	t.Parallel()

	// allComplete / noneComplete / specific-holes helpers — keep test cases
	// declarative.
	allComplete := func(int) bool { return true }
	noneComplete := func(int) bool { return false }
	hasHoleAt := func(holes ...int) func(int) bool {
		set := make(map[int]bool, len(holes))
		for _, h := range holes {
			set[h] = true
		}
		return func(pi int) bool { return !set[pi] }
	}

	const (
		piece   int64 = 16 << 20 // 16 MiB pieces
		fileLen int64 = 100 << 20
		fileOff int64 = 5 * piece // 80 MiB into the torrent
	)
	// File occupies pieces [5..11) (5,6,7,8,9,10) — 6 pieces total.
	// piece 5: [80, 96) MiB
	// piece 6: [96, 112)
	// piece 7: [112, 128)
	// piece 10: [160, 176) — file ends at 180 MiB, so piece 10 only carries 4 MiB of file.

	cases := []struct {
		name       string
		pieceLen   int64
		fileOff    int64
		fileLen    int64
		fromOff    int64
		numPieces  int
		isComplete func(int) bool
		want       int64
	}{
		{
			name:     "empty: nothing complete",
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: 0, numPieces: 12, isComplete: noneComplete,
			want: 0,
		},
		{
			name:     "all complete from start: returns file length",
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: 0, numPieces: 12, isComplete: allComplete,
			want: fileLen,
		},
		{
			name:     "all complete from middle: returns remaining file bytes",
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: 50 << 20, numPieces: 12, isComplete: allComplete,
			want: fileLen - (50 << 20),
		},
		{
			name:     "hole at start piece: returns 0 even with later pieces complete",
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: 0, numPieces: 12, isComplete: hasHoleAt(5),
			want: 0,
		},
		{
			name: "hole at middle piece: returns bytes up to start of hole",
			// fromOff=0 → absStart=80MiB (in piece 5). Pieces 5,6 complete,
			// piece 7 missing. lastCompleteEnd = (6+1)*16 = 112 MiB.
			// bytes = 112 - 80 = 32 MiB.
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: 0, numPieces: 12, isComplete: hasHoleAt(7),
			want: 32 << 20,
		},
		{
			name: "fromOff at exact piece boundary",
			// fromOff=16MiB → absStart=96MiB (start of piece 6). Pieces 6,7
			// complete, piece 8 missing. lastCompleteEnd = 8*16 = 128 MiB.
			// bytes = 128 - 96 = 32 MiB.
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: 16 << 20, numPieces: 12, isComplete: hasHoleAt(8),
			want: 32 << 20,
		},
		{
			name:     "fromOff past end of file: returns 0",
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: fileLen, numPieces: 12, isComplete: allComplete,
			want: 0,
		},
		{
			name:     "fromOff way past end of file: returns 0",
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: fileLen + (10 << 20), numPieces: 12, isComplete: allComplete,
			want: 0,
		},
		{
			name:     "negative fromOff: returns 0",
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: -1, numPieces: 12, isComplete: allComplete,
			want: 0,
		},
		{
			name:     "zero pieceLen: returns 0 (guard against missing metadata)",
			pieceLen: 0, fileOff: fileOff, fileLen: fileLen,
			fromOff: 0, numPieces: 12, isComplete: allComplete,
			want: 0,
		},
		{
			name: "last piece complete, clamp to file end",
			// fromOff near end of file: absStart=178MiB (inside piece 11).
			// Wait, piece 11 covers [176,192) but file ends at 180. So
			// absStart=fileOff+fromOff=80+99=179MiB, in piece 11.
			// piece 11 complete → lastCompleteEnd=192MiB, clamped to fileEnd=180MiB.
			// bytes = 180 - 179 = 1 MiB.
			pieceLen: piece, fileOff: fileOff, fileLen: fileLen,
			fromOff: 99 << 20, numPieces: 12, isComplete: allComplete,
			want: 1 << 20,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := contiguousBytesAheadCore(c.pieceLen, c.fileOff, c.fileLen, c.fromOff, c.numPieces, c.isComplete)
			if got != c.want {
				t.Errorf("got %d, want %d", got, c.want)
			}
		})
	}
}

// TestWarmNextGate_Decision documents the gate condition used inside
// Engine.WarmNext: a head warm is deferred when (1) the plugin supplied a
// non-zero current offset (opted into the gate) AND (2) the contiguous bytes
// ahead of that offset are below warmNextHealthThreshold. Either condition
// alone permits the warm.
func TestWarmNextGate_Decision(t *testing.T) {
	t.Parallel()
	shouldDefer := func(currentOffset, ahead int64) bool {
		return currentOffset > 0 && ahead < warmNextHealthThreshold
	}
	cases := []struct {
		name          string
		currentOffset int64
		ahead         int64
		wantDeferred  bool
	}{
		{"legacy: no offset passed → never defer", 0, 0, false},
		{"legacy: no offset passed even when ahead is tiny", 0, 1024, false},
		{"offset passed, ahead well above threshold → proceed", 100 << 20, 200 << 20, false},
		{"offset passed, ahead exactly at threshold → proceed", 100 << 20, warmNextHealthThreshold, false},
		{"offset passed, ahead one byte below threshold → defer", 100 << 20, warmNextHealthThreshold - 1, true},
		{"offset passed, ahead zero → defer", 100 << 20, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldDefer(c.currentOffset, c.ahead); got != c.wantDeferred {
				t.Errorf("shouldDefer(offset=%d, ahead=%d) = %v, want %v",
					c.currentOffset, c.ahead, got, c.wantDeferred)
			}
		})
	}
}

func TestSelectPrimaryIndex(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		files []fileMeta
		want  int
	}{
		{
			name: "largest video wins",
			files: []fileMeta{
				{"sample.mp4", 5 << 20},
				{"movie.mkv", 1500 << 20},
				{"readme.txt", 1 << 10},
			},
			want: 1,
		},
		{
			name: "ignores larger non-video file",
			files: []fileMeta{
				{"movie.mkv", 700 << 20},
				{"disk.iso", 4000 << 20},
			},
			want: 0,
		},
		{
			name: "falls back to largest file when no video present",
			files: []fileMeta{
				{"a.bin", 10},
				{"b.bin", 999},
			},
			want: 1,
		},
		{
			name:  "empty list",
			files: nil,
			want:  -1,
		},
	}
	for _, c := range cases {
		if got := selectPrimaryIndex(c.files); got != c.want {
			t.Errorf("%s: selectPrimaryIndex = %d, want %d", c.name, got, c.want)
		}
	}
}

// buildV1Metainfo builds a real multi-piece v1 torrent over a freshly generated
// content file and returns its metainfo (with InfoBytes set). The torrent is
// marked private so tests stay network-quiet (no public-tracker announces), and
// >1 piece is required to exercise the piece-layer path that tripped the bug.
func buildV1Metainfo(t *testing.T) metainfo.MetaInfo {
	t.Helper()
	contentFile := filepath.Join(t.TempDir(), "video.bin")
	if err := os.WriteFile(contentFile, make([]byte, 64<<10), 0o644); err != nil {
		t.Fatalf("write content: %v", err)
	}
	info := metainfo.Info{PieceLength: 16 << 10} // 64 KiB / 16 KiB = 4 pieces
	private := true
	info.Private = &private
	if err := info.BuildFromFilePath(contentFile); err != nil {
		t.Fatalf("BuildFromFilePath: %v", err)
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("marshal info: %v", err)
	}
	return metainfo.MetaInfo{InfoBytes: infoBytes}
}

// writeV1Torrent writes buildV1Metainfo to a .torrent file and returns its path,
// suitable for engine.Add (the local-file source branch).
func writeV1Torrent(t *testing.T) string {
	t.Helper()
	mi := buildV1Metainfo(t)
	torrentPath := filepath.Join(t.TempDir(), "test.torrent")
	f, err := os.Create(torrentPath)
	if err != nil {
		t.Fatalf("create torrent file: %v", err)
	}
	if err := mi.Write(f); err != nil {
		f.Close()
		t.Fatalf("write torrent file: %v", err)
	}
	f.Close()
	return torrentPath
}

// newTestEngine builds an Engine on throwaway temp dirs with the network-stall
// watchdog wired (monitor discards output).
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := NewEngine(t.TempDir(), t.TempDir(), readaheadMin, false, newResendErrorMonitor(io.Discard))
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(e.Close)
	return e
}

// TestRecreateClient_ReAddsV1Torrent is a regression guard for the network-stall
// watchdog. recreateClient re-adds every tracked torrent to the rebuilt client
// via t.Metainfo(); for a v1 torrent that reconstructed metainfo carries a
// non-nil but EMPTY PieceLayers map, which made anacrolix's AddTorrent fail with
// "no piece root set for file" — silently dropping the torrent so playback and
// download froze. This builds a real multi-piece v1 torrent, rebuilds the
// client, and asserts the torrent survives the swap.
func TestRecreateClient_ReAddsV1Torrent(t *testing.T) {
	e := newTestEngine(t)

	mt, err := e.Add(context.Background(), writeV1Torrent(t))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	oldT := mt.t.Load()
	if oldT == nil || oldT.Info() == nil {
		t.Fatal("torrent not loaded with info after Add")
	}
	wantIH := oldT.InfoHash()

	// Rebuild the client exactly as the watchdog does on a network stall.
	e.recreateClient()

	newT := mt.t.Load()
	if newT == nil {
		t.Fatal("torrent was dropped: mt.t is nil after recreateClient")
	}
	if newT == oldT {
		t.Fatal("torrent was not re-added: mt.t still points to the old (closed-client) torrent")
	}
	if got := newT.InfoHash(); got != wantIH {
		t.Errorf("re-added torrent infohash = %v, want %v", got, wantIH)
	}
}

// TestAdd_ReBindsHandleAfterClientSwap covers the self-heal path: when a tracked
// torrent has been orphaned on a since-closed client (the frozen-playback state
// before the fix), re-opening the same source must adopt a live handle on the
// current client instead of returning the dead one.
func TestAdd_ReBindsHandleAfterClientSwap(t *testing.T) {
	e := newTestEngine(t)
	torrentPath := writeV1Torrent(t)

	mt, err := e.Add(context.Background(), torrentPath)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	oldT := mt.t.Load()
	ih := oldT.InfoHash().HexString()

	// Simulate a watchdog rebuild that swapped the client but left this torrent
	// orphaned (mt.t still on the now-closed client). Match recreateClient's
	// order: close the old client before building the new one.
	oldClient := e.client.Load()
	oldClient.Close()
	newClient, err := buildTorrentClient(e.cacheDir, e.storage, e.seed)
	if err != nil {
		t.Fatalf("buildTorrentClient: %v", err)
	}
	e.client.Store(newClient)
	if mt.t.Load() != oldT {
		t.Fatal("test setup: handle should still point at the old torrent")
	}

	// Re-opening the same source must re-bind the handle to the new client.
	mt2, err := e.Add(context.Background(), torrentPath)
	if err != nil {
		t.Fatalf("Add (reopen): %v", err)
	}
	if mt2 != mt {
		t.Fatal("Add returned a different managedTorrent for the same infohash")
	}
	newT := mt.t.Load()
	if newT == oldT {
		t.Fatal("handle was not re-bound: mt.t still points at the closed-client torrent")
	}
	if got := newT.InfoHash().HexString(); got != ih {
		t.Errorf("re-bound infohash = %s, want %s", got, ih)
	}
	if _, ok := e.Status(ih); !ok {
		t.Error("Status unavailable after re-bind")
	}
}

// TestReaddToClient_InfohashFallback exercises the no-full-metainfo branch of
// readdToClient: it must register a live handle via AddTorrentInfoHash and
// re-supply the info bytes, so a torrent recovers even when the full-metainfo
// add path is unavailable.
func TestReaddToClient_InfohashFallback(t *testing.T) {
	e := newTestEngine(t)
	mi := buildV1Metainfo(t)
	wantIH := mi.HashInfoBytes()

	// hasInfo=false forces the infohash-add + SetInfoBytes fallback.
	s := reAddSnapshot{ih: wantIH.HexString(), mt: &managedTorrent{}, mi: mi, hasInfo: false}
	got, ok := e.readdToClient(e.client.Load(), s)
	if !ok {
		t.Fatal("readdToClient returned ok=false")
	}
	if got == nil {
		t.Fatal("readdToClient returned a nil handle")
	}
	if got.InfoHash() != wantIH {
		t.Errorf("infohash = %v, want %v", got.InfoHash(), wantIH)
	}
	if got.Info() == nil {
		t.Error("info bytes were not re-supplied: Info() is nil")
	}
}

func TestResolveInfohash(t *testing.T) {
	mi := buildV1Metainfo(t)
	want := mi.HashInfoBytes()

	// Info bytes present → hash derived from the bytes (map key ignored).
	if got, ok := resolveInfohash("ignored", &mi); !ok || got != want {
		t.Errorf("resolveInfohash(with InfoBytes) = %v ok=%v, want %v true", got, ok, want)
	}
	// No info bytes → fall back to the hex map key.
	hexKey := want.HexString()
	if got, ok := resolveInfohash(hexKey, &metainfo.MetaInfo{}); !ok || got.HexString() != hexKey {
		t.Errorf("resolveInfohash(hex fallback) = %v ok=%v, want %s true", got, ok, hexKey)
	}
	// Neither usable → ok=false.
	if _, ok := resolveInfohash("not-a-hash", &metainfo.MetaInfo{}); ok {
		t.Error("resolveInfohash(garbage) ok=true, want false")
	}
}
