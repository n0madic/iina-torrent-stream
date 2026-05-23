package main

import (
	"sync"
	"time"
)

// defaultMonitorInterval is how often the daemon re-evaluates whether it is
// still needed. Tests override this through lifecycle.monitorInterval.
const defaultMonitorInterval = 5 * time.Second

// lifecycle tracks daemon activity and triggers a graceful shutdown once the
// daemon is no longer needed. This is the primary cleanup mechanism: the IINA
// plugin API cannot kill a spawned process, so the daemon must terminate itself
// (purging its cache on the way out) when no longer needed.
type lifecycle struct {
	idleTimeout     time.Duration
	monitorInterval time.Duration
	shutdown        chan struct{}

	mu           sync.Mutex
	lastActivity time.Time
	closed       bool
}

func newLifecycle(idleTimeout time.Duration) *lifecycle {
	return &lifecycle{
		idleTimeout:     idleTimeout,
		monitorInterval: defaultMonitorInterval,
		shutdown:        make(chan struct{}),
		lastActivity:    time.Now(),
	}
}

// touch records activity, resetting the idle timer.
func (l *lifecycle) touch() {
	l.mu.Lock()
	l.lastActivity = time.Now()
	l.mu.Unlock()
}

// trigger requests a graceful shutdown. It is safe to call multiple times.
func (l *lifecycle) trigger() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.shutdown)
	}
}

// monitor shuts the daemon down once it has been idle — no /heartbeat, no
// status poll, no /stream request — for longer than idleTimeout. While any
// player window is open the plugin polls torrent status roughly every 1.5s and
// sends heartbeats, so the timer only elapses once every window using this
// daemon has closed. This is the daemon's self-cleanup: shutting down purges
// its disk cache. It returns when a shutdown is triggered.
func (l *lifecycle) monitor() {
	ticker := time.NewTicker(l.monitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.shutdown:
			return
		case <-ticker.C:
			l.mu.Lock()
			idle := time.Since(l.lastActivity)
			l.mu.Unlock()
			if idle >= l.idleTimeout {
				l.trigger()
				return
			}
		}
	}
}
