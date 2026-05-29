package main

import (
	"bytes"
	"io"
	"sync/atomic"
)

// resendErrorSubstr is the prefix used by github.com/anacrolix/utp (send.go:70)
// when it cannot push a queued packet to a peer (typically EHOSTUNREACH or
// ENETUNREACH on macOS after a VPN flap / network change / sleep-wake). The
// log line is emitted directly through Go's std log package, so the only way
// to silence it without forking anacrolix is to filter at the writer layer.
var resendErrorSubstr = []byte("error resending packet:")

// routingFailureSubstrs narrows the count to entries that look like a
// routing-table / local-interface failure (not e.g. EAGAIN). Recovery is only
// worth attempting for these cases — otherwise we would tear down the client
// on transient buffer hiccups. A resend error matching ANY of these warrants a
// rebuild:
//   - "network is unreachable" (ENETUNREACH): the route to the peer is gone.
//   - "can't assign requested address" (EADDRNOTAVAIL): the local uTP socket is
//     still bound to an address that no longer exists after a sleep/wake, Wi-Fi
//     reconnect, or VPN flap. Observed in the wild as the dominant failure when
//     the network changes underneath a long-running daemon.
var routingFailureSubstrs = [][]byte{
	[]byte("network is unreachable"),
	[]byte("can't assign requested address"),
}

// resendErrorMonitor wraps an io.Writer and intercepts the uTP resend-failure
// spam. Matching lines are dropped from the sink (otherwise they drown the
// log when every peer becomes unroutable) and counted atomically; the engine's
// recoveryLoop polls the counter to decide whether to rebuild the torrent
// client. Non-matching writes are forwarded to inner unchanged.
type resendErrorMonitor struct {
	inner io.Writer
	count atomic.Int64
}

func newResendErrorMonitor(inner io.Writer) *resendErrorMonitor {
	return &resendErrorMonitor{inner: inner}
}

// Write implements io.Writer. A single Write may contain multiple log lines
// (the std log package emits one record per Write but multiSink may batch in
// theory). The check is on the whole buffer rather than per-line because each
// resend-error record is a self-contained Write from log.Printf.
func (m *resendErrorMonitor) Write(p []byte) (int, error) {
	if bytes.Contains(p, resendErrorSubstr) && containsRoutingFailure(p) {
		m.count.Add(1)
		return len(p), nil
	}
	return m.inner.Write(p)
}

// containsRoutingFailure reports whether p mentions any of the routing-table /
// interface error strings that warrant a torrent-client rebuild.
func containsRoutingFailure(p []byte) bool {
	for _, sub := range routingFailureSubstrs {
		if bytes.Contains(p, sub) {
			return true
		}
	}
	return false
}

// LoadAndReset returns the number of suppressed resend errors since the last
// call and resets the counter to zero. Called from recoveryLoop.
func (m *resendErrorMonitor) LoadAndReset() int64 {
	return m.count.Swap(0)
}
