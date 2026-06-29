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
	// seq is this slot's claim marker, not the message's global sequence
	// number. A writer for global sequence s may only claim this slot by
	// CAS-ing seq from s-ringSize to s-ringSize+1 -- i.e. the slot's previous
	// occupant (one lap of the ring ago) must already be retired. The CAS
	// operands are computed from s and ringSize directly rather than from a
	// freshly-loaded value, so two competing writers always compute the same
	// expected-old and only one can ever win. Closing the gap from
	// s-ringSize+1 up to s is done by a later retire-after-flush step.
	seq atomic.Uint64
	n   uint32 // bytes actually written into buf (<= MESSAGE_CAP); 0 means unwritten
	buf []byte // fixed-capacity backing array, len == MESSAGE_CAP
}

// buffer is one fixed-size slab in the Pool. Exactly one buffer is "active"
// (accepting writes) at a time; the rest are either free or mid-flush.
type buffer struct {
	slots []slot // len == BUFFER_SIZE, allocated once and never resized

	// readIdx is bumped via a single atomic fetch-and-add per write — the
	// only synchronization on the write hot path. It is only advanced once a
	// slot's read from the caller's io.Reader has completed, not merely
	// claimed, since the fill-counter must track data that has actually
	// landed. It only ever increases for a given activation; once it exceeds
	// len(slots), the buffer is full and callers must trigger a swap to the
	// next buffer.
	// This readIdx is cyclic so essentially after hitting the required number of slots
	// it turns back on itself.
	readIdx atomic.Uint32

	// seqLo is the sequence number of slots[0] for the current activation.
	// Combined with readIdx, it gives the seq range this buffer covers.
	// Only written by the single goroutine performing the swap, before the
	// buffer is published as active — so no atomic is needed for it.
	//
	// Not used for slot indexing in write -- that's now derived purely from
	// seq and ringSize. Retained for flush-time bookkeeping of a buffer's
	// seq range.
	seqLo uint64

	// ringSize is the total capacity of the whole Pool (poolSize*slotCount),
	// set once at construction in CreatePool. A given slot in this buffer is
	// only revisited once every ringSize global sequence numbers, so write
	// uses it to compute the claim CAS's expected-old/new operands.
	ringSize uint64

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

func (b *buffer) write(reader io.Reader, seq uint64) error {
	idx := seq & (uint64(len(b.slots)) - 1)
	s := &b.slots[idx]

	expectedOld := seq - b.ringSize
	expectedNew := expectedOld + 1
	if !s.seq.CompareAndSwap(expectedOld, expectedNew) {
		return ErrSlotClaimFailed
	}

	n, err := reader.Read(s.buf)
	if err != nil && err != io.EOF {
		return err
	}
	s.n = uint32(n)

	return nil
}

func (b *buffer) read(seq uint64) error {
	b.readIdx.Add(1)
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
