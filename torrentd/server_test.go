package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServerRequiresToken(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"
	s := newServer(nil, newLifecycle(time.Second), false, token)

	without := httptest.NewRecorder()
	s.ServeHTTP(without, httptest.NewRequest(http.MethodGet, "/health", nil))
	if without.Code != http.StatusUnauthorized {
		t.Fatalf("request without token = %d, want %d", without.Code, http.StatusUnauthorized)
	}

	wrong := httptest.NewRecorder()
	s.ServeHTTP(wrong, httptest.NewRequest(http.MethodGet, "/health?token=wrong", nil))
	if wrong.Code != http.StatusUnauthorized {
		t.Fatalf("request with wrong token = %d, want %d", wrong.Code, http.StatusUnauthorized)
	}

	valid := httptest.NewRecorder()
	s.ServeHTTP(valid, httptest.NewRequest(http.MethodGet, "/health?token="+token, nil))
	if valid.Code != http.StatusOK {
		t.Fatalf("request with valid token = %d, want %d", valid.Code, http.StatusOK)
	}
}

func TestSanitizeM3UTitle(t *testing.T) {
	got := sanitizeM3UTitle("episode\r\nhttp://attacker.invalid/video.mkv")
	if strings.ContainsAny(got, "\r\n") {
		t.Fatalf("sanitized title still contains a line break: %q", got)
	}
	if got != "episode  http://attacker.invalid/video.mkv" {
		t.Fatalf("sanitized title = %q", got)
	}
}

// mpv never joins a root-relative playlist entry with the playlist's own URL,
// and the plugin matches stream URLs against ^http://127.0.0.1:<port>/ — so
// every entry has to carry the daemon's absolute base URL.
func TestWriteM3UEmitsAbsoluteLoopbackURLs(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"
	const ih = "aabbccddeeff00112233445566778899aabbccdd"
	s := newServer(nil, newLifecycle(time.Second), false, token)
	s.setPort(51234)

	var buf strings.Builder
	s.writeM3U(&buf, ih, []m3uEntry{
		{index: 0, name: "S01E01 pilot.mkv"},
		{index: 3, name: "S01E02.mkv"},
	})

	want := "#EXTM3U\n" +
		"#EXTINF:0,S01E01 pilot.mkv\n" +
		"http://127.0.0.1:51234/stream/" + ih + "/0/S01E01%20pilot.mkv?token=" + token + "\n" +
		"#EXTINF:0,S01E02.mkv\n" +
		"http://127.0.0.1:51234/stream/" + ih + "/3/S01E02.mkv?token=" + token + "\n"
	if got := buf.String(); got != want {
		t.Fatalf("playlist =\n%q\nwant\n%q", got, want)
	}
}

func TestNewAPIToken(t *testing.T) {
	first, err := newAPIToken()
	if err != nil {
		t.Fatalf("newAPIToken: %v", err)
	}
	second, err := newAPIToken()
	if err != nil {
		t.Fatalf("newAPIToken second call: %v", err)
	}
	if len(first) != 64 {
		t.Fatalf("token length = %d, want 64 hex characters", len(first))
	}
	if first == second {
		t.Fatal("two generated API tokens are identical")
	}
}

func TestWriteStateIsPrivateAndComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torrentd.json")
	const token = "0123456789abcdef0123456789abcdef"
	if err := writeState(path, 4111, token); err != nil {
		t.Fatalf("writeState: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat state: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("state permissions = %o, want 600", got)
	}
	var state struct {
		Port  int    `json:"port"`
		Token string `json:"token"`
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("state JSON is incomplete: %v", err)
	}
	if state.Port != 4111 || state.Token != token {
		t.Fatalf("state = %+v", state)
	}
}
