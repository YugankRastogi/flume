package buffer

import "sync/atomic"

// bufferState tracks where a Buffer is in its lifecycle within the Pool.
type bufferState int32

const (
	stateFree    bufferState = iota // sitting in Pool.free, ready to be activated
	stateActive                     // currently the active buffer, accepting writes
	stateFlushing                   // handed off to Pool.onFlush, being written to the store
)

// slot holds a single message. buf is pre-allocated to MESSAGE_CAP bytes once,
// at Buffer construction, and reused for the life of the process — this is what
// gives the write path its "no GC pressure" property.
type slot struct {
	seq uint64 // global sequence number assigned to this message by Pool.seq
	n   uint32 // bytes actually written into buf (<= MESSAGE_CAP); 0 means unwritten
	buf []byte // fixed-capacity backing array, len == MESSAGE_CAP
}

// Buffer is one fixed-size slab in the Pool. Exactly one Buffer is "active"
// (accepting writes) at a time; the rest are either free or mid-flush.
type Buffer struct {
	slots []slot // len == BUFFER_SIZE, allocated once and never resized

	// writeIdx is bumped via a single atomic fetch-and-add per write — the
	// only synchronization on the write hot path. It only ever increases for
	// a given activation; once it exceeds len(slots), the buffer is full and
	// callers must trigger a swap to the next buffer.
	writeIdx atomic.Uint64

	// seqLo is the sequence number of slots[0] for the current activation.
	// Combined with writeIdx, it gives the seq range this buffer covers.
	// Only written by the single goroutine performing the swap, before the
	// buffer is published as active — so no atomic is needed for it.
	seqLo uint64

	// state is the current bufferState (stateFree/stateActive/stateFlushing),
	// stored atomically since it's read/written from the writer goroutines
	// and the flush goroutine concurrently.
	state atomic.Int32
}
