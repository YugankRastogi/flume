package store

// LocalStore is a disk-backed SnapshotStore for the POC. Each namespace gets
// its own subdirectory under rootDir; each snapshot is one file named
// "<seqStart>-<seqEnd>.snap", so List can enumerate snapshots by parsing
// filenames directly off the filesystem -- no separate index file that could
// drift out of sync with what's actually on disk.
//
// Writes are not fsync'd: durable here means "handed off from the hot
// buffer to the store," not "guaranteed against power loss." The loss
// window is the flush interval, by design, per the README's core tradeoff.
type LocalStore struct {
	rootDir string
}
