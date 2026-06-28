package store

import "io"

// SnapshotMeta describes one flushed snapshot's identity and location, as
// returned by List, without requiring its contents to be read.
type SnapshotMeta struct {
	Namespace string
	SeqStart  uint64
	SeqEnd    uint64
	Size      int64
}

// SnapshotStore is the durable backing store that flushed buffers are
// written to. LocalStore is the disk-backed POC implementation; S3Store,
// GCSStore, AzureBlobStore, and MultiStore implement the same interface
// later, per the README.
type SnapshotStore interface {
	// Write persists the byte range [seqStart, seqEnd] for namespace,
	// streaming from r until EOF. r is expected to be backed by something
	// like the buffer package's pooled multi-slot reader, so implementations
	// should stream from it directly (e.g. via io.Copy) rather than
	// buffering the whole payload themselves.
	Write(namespace string, seqStart, seqEnd uint64, r io.Reader) error

	// List returns metadata for snapshots in namespace whose seq range
	// overlaps [from, to], without reading their contents.
	List(namespace string, from, to uint64) ([]SnapshotMeta, error)

	// Read returns the raw bytes for the snapshot covering exactly
	// [seqStart, seqEnd]. Used by the read path's gap-detection replay.
	Read(namespace string, seqStart, seqEnd uint64) ([]byte, error)

	// Delete removes the snapshot covering exactly [seqStart, seqEnd].
	Delete(namespace string, seqStart, seqEnd uint64) error
}
