package main

import "github.com/anacrolix/torrent/storage"

// newStreamStorage builds the piece storage for the torrent client.
//
// It uses anacrolix's standard file-based storage: each torrent's data is
// written to plain files under cacheDir, which the daemon treats as a temporary
// scratch area and purges on shutdown.
//
// A capacity-capped cache (filecache + resource pieces) was tried first, but it
// desynchronises piece-completion state from LRU-evicted blobs once a torrent
// exceeds the cache size — the streaming reader then reads evicted pieces and
// fails with EOF. Plain file storage has no eviction and no such desync.
//
// Returns the Closer variant so the engine can explicitly close the underlying
// piece-completion (BoltDB) handle on shutdown — torrent.Client.Close only
// drops torrents, it does not close the shared DefaultStorage, so without this
// the BoltDB file at cacheDir/.torrent.bolt.db would still have an open writer
// while the cache purge runs and the cleanup could leave stale files behind.
func newStreamStorage(cacheDir string) storage.ClientImplCloser {
	return storage.NewFile(cacheDir)
}
