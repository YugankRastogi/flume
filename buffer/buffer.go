package buffer

import (
	"io"
	"sync/atomic"
)

// bufferState tracks where a buffer is in its lifecycle within the Pool.
type bufferState int32

const (
	stateActive        bufferState = iota // currently the active buffer, accepting writes
	stateReadyForFlush                    // full and retired, waiting to be picked up for flush
	stateFlushing                         // handed off to its Flusher, being written to the store
)

// slot holds a single message. buf is pre-allocated to MESSAGE_CAP bytes once,
// at buffer construction, and reused for the life of the process — this is what
// gives the write path its "no GC pressure" property.
type slot struct {
	seq uint64 // global sequence number assigned to this message by Pool.seq
	n   uint32 // bytes actually written into buf (<= MESSAGE_CAP); 0 means unwritten
	buf []byte // fixed-capacity backing array, len == MESSAGE_CAP
}

// buffer is one fixed-size slab in the Pool. Exactly one buffer is "active"
// (accepting writes) at a time; the rest are either free or mid-flush.
type buffer struct {
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

	// state is the current bufferState (stateActive/stateReadyForFlush/
	// stateFlushing), stored atomically since it's read/written from the
	// writer goroutines and the flush goroutine concurrently.
	state atomic.Int32

	// flusher is this buffer's hook for draining itself once retired. It's a
	// placeholder field for now -- nothing constructs a buffer with one set
	// yet -- but the extension point is in place here, per-buffer, rather
	// than on Pool, so each buffer can be wired to its own flush destination
	// (e.g. the flush-contract/store component) without Pool needing to know
	// about it.
	flusher Flusher
}

func (b *buffer) write(reader io.Reader) error {
	//This stub is supposed to actually do a zero copy to the slot
	//Selection of slot is dependent upon the total number of slots
	return nil
}

// flush is an internal implementation detail and any changes made to this should be handled by external flush function.
func (b *buffer) flush() error {
	if b.state.CompareAndSwap(int32(stateReadyForFlush), int32(stateFlushing)) {
		return nil
	}
	defer b.state.Store(int32(stateActive))
	return b.flusher.Flush()
}
