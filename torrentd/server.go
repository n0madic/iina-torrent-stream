package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// server wires the HTTP API onto the engine and the lifecycle tracker.
type server struct {
	engine *Engine
	lc     *lifecycle
	mux    *http.ServeMux
}

func newServer(engine *Engine, lc *lifecycle, enableDebug bool) *server {
	s := &server{engine: engine, lc: lc, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /torrents", s.handleAddTorrent)
	s.mux.HandleFunc("GET /torrents/{ih}", s.handleStatus)
	s.mux.HandleFunc("DELETE /torrents/{ih}", s.handleRemove)
	s.mux.HandleFunc("POST /torrents/{ih}/pause", s.handlePause)
	s.mux.HandleFunc("POST /torrents/{ih}/resume", s.handleResume)
	s.mux.HandleFunc("GET /stream/{ih}/{idx}", s.handleStream)
	// Same handler with a trailing file-name segment (ignored). The name lets
	// the player derive a readable title from the URL.
	s.mux.HandleFunc("GET /stream/{ih}/{idx}/{name}", s.handleStream)
	// All-in-one playback endpoint: adds the torrent (if not already), waits
	// for metadata, then streams the primary video file. Used by the plugin
	// when a magnet/.torrent is opened directly into a player window so mpv
	// has something to load right away — it sits in "buffering" until
	// metadata is ready, which avoids the visible "cannot open URL" flash
	// from mpv trying to play a magnet: URL itself.
	s.mux.HandleFunc("GET /play", s.handlePlay)
	s.mux.HandleFunc("POST /torrents/{ih}/files/{idx}/prewarm", s.handlePrewarm)
	s.mux.HandleFunc("POST /torrents/{ih}/warm-next", s.handleWarmNext)
	s.mux.HandleFunc("POST /heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("POST /shutdown", s.handleShutdown)

	if enableDebug {
		// Diagnostic endpoints. /debug/memstats is a cheap JSON snapshot of
		// runtime memory + live torrent/reader counts; /debug/pprof/* are the
		// standard Go pprof endpoints for heap/goroutine/CPU profiling
		// ("go tool pprof http://127.0.0.1:<port>/debug/pprof/heap"). Off by
		// default — they expose internal process state and should only be on
		// while actively debugging. The plugin enables them via the
		// "debugEndpoints" preference (--debug-endpoints CLI flag).
		s.mux.HandleFunc("GET /debug/memstats", s.handleMemStats)
		s.mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		s.mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		s.mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		s.mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		s.mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// A health probe means the plugin is actively trying to use the daemon, so
	// it counts as activity — this keeps a freshly started, still-torrentless
	// daemon alive through the emptyGrace window until the first torrent is added.
	s.lc.touch()
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": version})
}

func (s *server) handleAddTorrent(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	// The source is accepted as a query parameter (used by the plugin, which
	// avoids HTTP-body encoding ambiguity) or as a JSON body (convenient for
	// manual testing with curl).
	source := r.URL.Query().Get("source")
	if source == "" {
		var body struct {
			Source string `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
			source = body.Source
		}
	}
	if source == "" {
		writeError(w, http.StatusBadRequest, "missing 'source' (query parameter or JSON body)")
		return
	}
	// 30s is for the upfront bits Add does synchronously: loadRemoteMetainfo
	// for an http(s):// .torrent URL, and metainfo.LoadFromFile for a local
	// path. AddMagnet itself is non-blocking. Metadata fetching happens in
	// a background goroutine and the plugin polls /torrents/{ih} for it.
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	mt, err := s.engine.Add(ctx, source)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	// Return the partial status (just the infohash and peer counts) without
	// waiting for metadata, so the HTTP request always completes inside
	// IINA's hidden request timeout. The plugin polls for the file list.
	status, ok := s.engine.Status(mt.t.InfoHash().HexString())
	if !ok {
		writeError(w, http.StatusInternalServerError, "torrent vanished after add")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	status, ok := s.engine.Status(r.PathValue("ih"))
	if !ok {
		writeError(w, http.StatusNotFound, "torrent not found")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *server) handleRemove(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	if !s.engine.Remove(r.PathValue("ih")) {
		writeError(w, http.StatusNotFound, "torrent not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (s *server) handlePause(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	if !s.engine.Pause(r.PathValue("ih")) {
		writeError(w, http.StatusNotFound, "torrent not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *server) handleResume(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	if !s.engine.Resume(r.PathValue("ih")) {
		writeError(w, http.StatusNotFound, "torrent not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "playing"})
}

func (s *server) handleStream(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	defer s.lc.touch()
	idx, err := strconv.Atoi(r.PathValue("idx"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid file index")
		return
	}
	s.engine.Stream(w, r, r.PathValue("ih"), idx)
}

// handlePlay is an all-in-one endpoint mpv hits with a single GET. The daemon
// adds the torrent if necessary and waits for its metadata; if the torrent
// holds a single video file it streams that file directly, and if it holds
// multiple videos it responds with an #EXTM3U playlist of /stream URLs (with
// readable #EXTINF titles) — mpv natively unpacks that into a playlist. The
// player window opens immediately and shows "buffering" until metadata is
// ready, instead of needing the plugin to first POST /torrents.
func (s *server) handlePlay(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	defer s.lc.touch()
	source := r.URL.Query().Get("source")
	if source == "" {
		http.Error(w, "missing 'source' query parameter", http.StatusBadRequest)
		return
	}
	// Bound the metadata wait so a magnet pointing at an offline swarm does
	// not pin a request open indefinitely. The handler-side context already
	// fires when mpv closes the connection.
	ctx, cancel := context.WithTimeout(r.Context(), metadataTimeout)
	defer cancel()
	mt, primaryIdx, err := s.engine.Play(ctx, source)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	t := mt.t
	ih := t.InfoHash().HexString()
	files := t.Files()

	// Build the list of video file indices to decide single-file vs playlist.
	// isVideoFile matches the same set the plugin's videoFiles() uses on the
	// client side, so the two views of the torrent agree.
	var videoIdx []int
	for i, f := range files {
		if isVideoFile(f.DisplayPath()) {
			videoIdx = append(videoIdx, i)
		}
	}
	if len(videoIdx) <= 1 {
		s.engine.Stream(w, r, ih, primaryIdx)
		return
	}

	w.Header().Set("Content-Type", "audio/x-mpegurl")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "#EXTM3U")
	for _, i := range videoIdx {
		name := filepath.Base(files[i].DisplayPath())
		fmt.Fprintf(w, "#EXTINF:0,%s\n", name)
		fmt.Fprintf(w, "http://%s/stream/%s/%d/%s\n", r.Host, ih, i, url.PathEscape(name))
	}
}

// handlePrewarm downloads a single file inside a torrent to completion and
// returns its absolute on-disk path. Used by the plugin to extract subtitle
// files: IINA's core.subtitle.loadTrack rejects HTTP URLs ("Unsupported
// external subtitles") so the file has to be materialised locally first.
func (s *server) handlePrewarm(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	defer s.lc.touch()
	idx, err := strconv.Atoi(r.PathValue("idx"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid file index")
		return
	}
	// Bounded so a stuck swarm does not hold the request open forever — and
	// kept well below the daemon's idle timeout so the prewarm cannot
	// outlive a window that has already closed.
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	path, err := s.engine.Prewarm(ctx, r.PathValue("ih"), idx)
	if err != nil {
		writeError(w, http.StatusGatewayTimeout, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"path": path})
}

// handleWarmNext kicks off a background prewarm of the next video file in a
// torrent after the given file index. The plugin calls this when playback of
// the current episode crosses ~90 %, so when mpv switches playlist items the
// next episode's head + tail are already on disk.
//
// Optional current_offset (bytes into the file at `after`, from mpv's
// stream-pos) enables a bandwidth-health gate: when the active stream's
// contiguous buffer is small, the 128 MiB head warm is deferred and the
// plugin is expected to retry on its next 5 s tick.
func (s *server) handleWarmNext(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	defer s.lc.touch()
	after, err := strconv.Atoi(r.URL.Query().Get("after"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid or missing 'after' query parameter")
		return
	}
	var currentOffset int64
	if raw := r.URL.Query().Get("current_offset"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
			currentOffset = v
		}
	}
	nextIdx, started, deferred, err := s.engine.WarmNext(r.PathValue("ih"), after, currentOffset)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"nextIndex": nextIdx,
		"started":   started,
		"deferred":  deferred,
	})
}

func (s *server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	s.lc.touch()
	w.WriteHeader(http.StatusNoContent)
}

// handleMemStats returns a small, cheap JSON snapshot of the daemon's
// runtime memory + the engine's live torrent/reader counts. Designed to be
// pollable from the plugin or a curl one-liner without the cost of a full
// pprof heap dump.
func (s *server) handleMemStats(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	tc, rc := s.engine.countTorrentsAndReaders()
	writeJSON(w, http.StatusOK, map[string]any{
		"goroutines":   runtime.NumGoroutine(),
		"heapAlloc":    ms.HeapAlloc,
		"heapInuse":    ms.HeapInuse,
		"heapIdle":     ms.HeapIdle,
		"heapReleased": ms.HeapReleased,
		"heapSys":      ms.HeapSys,
		"sys":          ms.Sys,
		"numGC":        ms.NumGC,
		"torrents":     tc,
		"readers":      rc,
	})
}

func (s *server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "shutting down"})
	// Defer the trigger so this response can flush first.
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.lc.trigger()
	}()
}
