package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
