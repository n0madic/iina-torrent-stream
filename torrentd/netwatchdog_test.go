package main

import (
	"bytes"
	"testing"
	"time"
)

func TestResendErrorMonitor_DropsMatchingLines(t *testing.T) {
	t.Parallel()
	// Each line carries the resend prefix plus one of the routing-failure
	// substrings — all must be dropped from the sink and counted.
	cases := []struct {
		name string
		line string
	}{
		{
			name: "ENETUNREACH (network is unreachable)",
			line: "2026/01/01 12:00:00 error resending packet: write udp4 0.0.0.0:64965->1.2.3.4:6881: sendto: network is unreachable\n",
		},
		{
			name: "EADDRNOTAVAIL (can't assign requested address)",
			// Observed after a sleep/wake or VPN flap: the local socket is bound
			// to an address that no longer exists. Must count toward recovery.
			line: "2026/01/01 12:00:00 error resending packet: write udp4 0.0.0.0:52047->176.98.8.34:41103: sendto: can't assign requested address\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var inner bytes.Buffer
			m := newResendErrorMonitor(&inner)
			matching := []byte(c.line)
			n, err := m.Write(matching)
			if err != nil {
				t.Fatalf("Write: unexpected error: %v", err)
			}
			if n != len(matching) {
				t.Errorf("Write returned %d, want %d (must report full input length even when dropped)", n, len(matching))
			}
			if got := inner.Len(); got != 0 {
				t.Errorf("inner sink received %d bytes, want 0 (line should be dropped)", got)
			}
			if got := m.LoadAndReset(); got != 1 {
				t.Errorf("counter = %d, want 1", got)
			}
			if got := m.LoadAndReset(); got != 0 {
				t.Errorf("counter after reset = %d, want 0", got)
			}
		})
	}
}

func TestResendErrorMonitor_ForwardsNonMatchingLines(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		line string
	}{
		{
			name: "ordinary daemon log line",
			line: "2026/01/01 12:00:00 listening on 127.0.0.1:54321\n",
		},
		{
			name: "uTP resend but with a different error (not routing)",
			// Has the resend prefix but NOT 'network is unreachable' — must
			// be forwarded so the operator still sees real socket errors.
			line: "2026/01/01 12:00:00 error resending packet: connection refused\n",
		},
		{
			name: "network-unreachable but not from the uTP resend log",
			// Has 'network is unreachable' but NOT the resend prefix — must
			// be forwarded; this is a real error from another component.
			line: "2026/01/01 12:00:00 tracker: write udp 1.2.3.4:80: network is unreachable\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var inner bytes.Buffer
			m := newResendErrorMonitor(&inner)
			if _, err := m.Write([]byte(c.line)); err != nil {
				t.Fatalf("Write: unexpected error: %v", err)
			}
			if got := inner.String(); got != c.line {
				t.Errorf("inner sink = %q, want %q", got, c.line)
			}
			if got := m.LoadAndReset(); got != 0 {
				t.Errorf("counter = %d, want 0", got)
			}
		})
	}
}

func TestResendErrorMonitor_CountsAccumulate(t *testing.T) {
	t.Parallel()
	var inner bytes.Buffer
	m := newResendErrorMonitor(&inner)

	line := []byte("error resending packet: sendto: network is unreachable\n")
	const writes = 17
	for range writes {
		if _, err := m.Write(line); err != nil {
			t.Fatalf("Write: unexpected error: %v", err)
		}
	}
	if got := m.LoadAndReset(); got != writes {
		t.Errorf("counter = %d, want %d", got, writes)
	}
}

func TestRecoveryDecision(t *testing.T) {
	t.Parallel()
	const (
		threshold = int64(30)
		backoff   = 60 * time.Second
	)
	cases := []struct {
		name             string
		sum              int64
		sinceLastRebuild time.Duration
		want             recoveryAction
	}{
		{
			name:             "no errors → no action",
			sum:              0,
			sinceLastRebuild: 24 * time.Hour,
			want:             recoveryNone,
		},
		{
			name:             "few errors below threshold → report only",
			sum:              5,
			sinceLastRebuild: 24 * time.Hour,
			want:             recoveryReport,
		},
		{
			name:             "one error below threshold → report only",
			sum:              1,
			sinceLastRebuild: 24 * time.Hour,
			want:             recoveryReport,
		},
		{
			name:             "exactly threshold, well outside backoff → rebuild",
			sum:              threshold,
			sinceLastRebuild: 24 * time.Hour,
			want:             recoveryRebuild,
		},
		{
			name:             "well above threshold, outside backoff → rebuild",
			sum:              500,
			sinceLastRebuild: 24 * time.Hour,
			want:             recoveryRebuild,
		},
		{
			name:             "above threshold but inside backoff → cool-down",
			sum:              500,
			sinceLastRebuild: backoff / 2,
			want:             recoveryCoolDown,
		},
		{
			name:             "exactly threshold, exactly at backoff edge → rebuild",
			sum:              threshold,
			sinceLastRebuild: backoff,
			want:             recoveryRebuild,
		},
		{
			name:             "above threshold, one nanosecond before backoff edge → cool-down",
			sum:              threshold,
			sinceLastRebuild: backoff - time.Nanosecond,
			want:             recoveryCoolDown,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := recoveryDecision(c.sum, c.sinceLastRebuild, threshold, backoff)
			if got != c.want {
				t.Errorf("recoveryDecision(sum=%d, sinceLastRebuild=%s) = %v, want %v",
					c.sum, c.sinceLastRebuild, got, c.want)
			}
		})
	}
}
