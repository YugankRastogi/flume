package buffer

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/yugank/flume/flusher"
	"golang.org/x/sys/cpu"
)

// bufferState tracks where a Buffer is in its lifecycle within the Pool.
type bufferState int32

const (
	stateActive        bufferState = iota // currently the active buffer, accepting writes
	stateReadyForFlush                    // full and retired, waiting to be picked up for flush
	stateFlushing                         // handed off to its Flusher, being written to the store
)

// slot holds a single message. buf is pre-allocated to slotSize bytes once,
// at buffer construction, and reused for the life of the process — this is
// what gives the write path its "no GC pressure" property.
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
	n   atomic.Uint32 // bytes actually written into buf (<= slotSize); 0 means unwritten
	buf []byte        // fixed-capacity backing array, len == slotSize
}

// Buffer is one fixed-size slab in the Pool. Exactly one Buffer is "active"
// (accepting writes) at a time; the rest are either free or mid-flush.
type Buffer struct {
	// readCount is bumped via a single atomic fetch-and-add per read — once a
	// slot's data has been consumed by the caller and the read CAS chain has
	// completed. It is a consumption counter, not a fill counter: it tracks
	// how many slots have been fully drained in this activation, not how many
	// have been written. Once it reaches len(slots), all slots have been read
	// and the buffer is ready to flush. It is reset to 0 by the flush path
	// before the buffer transitions back to stateActive.
	readCount atomic.Uint32
	_         cpu.CacheLinePad

	slots []slot // len == slotCount, allocated once and never resized

	// ringSize is the total capacity of the whole Pool (poolSize*slotCount),
	// set once at construction in New. A given slot in this buffer is only
	// revisited once every ringSize global sequence numbers, so Write uses it
	// to compute the claim CAS's expected-old/new operands.
	ringSize uint64

	// state is the current bufferState (stateActive/stateReadyForFlush/
	// stateFlushing), stored atomically since it's read/written from the
	// writer goroutines and the flush goroutine concurrently.
	state atomic.Int32

	// sink is this buffer's hook for draining itself once retired. It is
	// stored per-buffer rather than on Pool so each buffer can be wired to
	// its own flush destination without Pool needing to know about it.
	sink flusher.Flusher
}

// New allocates a Buffer with slotCount slots of slotSize bytes each,
// belonging to a ring of ringSize total slots. baseSeq is the initial
// sequence value for slot 0 of this buffer (i.e. bufferIndex * slotCount).
func New(slotCount, slotSize, ringSize, baseSeq uint64, f flusher.Flusher) *Buffer {
	slots := make([]slot, slotCount)
	for j := range slots {
		slots[j].buf = make([]byte, slotSize)
		slots[j].seq.Store(baseSeq + uint64(j))
	}
	return &Buffer{slots: slots, ringSize: ringSize, sink: f}
}

// IsActive reports whether this buffer is currently accepting writes.
func (b *Buffer) IsActive() bool {
	return bufferState(b.state.Load()) == stateActive
}

func (b *Buffer) Write(reader io.Reader, seq uint64) error {
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
	s.n.Store(uint32(n))

	readyForRead := expectedNew + 1
	if !s.seq.CompareAndSwap(expectedNew, readyForRead) {
		return ErrSlotClaimFailed
	}

	return nil
}

func (b *Buffer) Read(seq uint64, readArr []byte) (uint32, error) {
	idx := seq & (uint64(len(b.slots)) - 1)
	s := &b.slots[idx]

	expectedOld := (seq - b.ringSize) + 2
	expectedNew := expectedOld + 1
	if !s.seq.CompareAndSwap(expectedOld, expectedNew) {
		return 0, ErrSlotClaimFailed
	}
	copy(readArr, s.buf)

	n := s.n.Load()
	// Use the Add return value directly so only the goroutine that bumps
	// readCount to exactly len(slots) triggers the flush. A separate Load()
	// would let two goroutines racing on the last slot both see the threshold
	// and spawn two flush goroutines.
	if b.readCount.Add(1) == uint32(len(b.slots)) {
		if b.state.CompareAndSwap(int32(stateActive), int32(stateReadyForFlush)) {
			go b.flush()
		}
	}

	return n, nil
}

func (b *Buffer) flush() error {
	if !b.state.CompareAndSwap(int32(stateReadyForFlush), int32(stateFlushing)) {
		panic("flush called without correct state")
	}
	defer func() {
		for i := range b.slots {
			b.slots[i].seq.Add(uint64(b.ringSize - 3))
			clear(b.slots[i].buf[:b.slots[i].n.Load()])
		}
		actual := b.readCount.Load()
		if !b.readCount.CompareAndSwap(uint32(len(b.slots)), 0) {
			panic(fmt.Sprintf("unexpected number of slots encountered after flushing: got %d, want %d", actual, len(b.slots)))
		}

		if !b.state.CompareAndSwap(int32(stateFlushing), int32(stateActive)) {
			panic("unexpected state encountered during flushing")
		}
	}()

	return b.sink.Flush()
}
