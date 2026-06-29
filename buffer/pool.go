package buffer

import (
	"errors"
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"sync/atomic"
)

const (
	poolSizeBits = 10
	poolSizeMask = uint64(1<<poolSizeBits) - 1

	slotCountBits  = 16
	slotCountShift = poolSizeBits
	slotCountMask  = uint64(1<<slotCountBits) - 1

	slotSizeBits  = 32
	slotSizeShift = poolSizeBits + slotCountBits
	slotSizeMask  = uint64(1<<slotSizeBits) - 1
)

// Pool owns a fixed ring of pre-allocated buffers (BUFFER_COUNT, sized at
// construction). Buffer rotation -- which buffer is currently active, and
// when a retired buffer is safe to reuse -- is being redesigned around a
// head/tail cursor pair instead of an atomic active pointer plus a
// free-buffer channel, so the hot path stays pure atomics with no channel
// (and therefore no channel-internal mutex) involved. Those cursor fields
// are intentionally not added yet.
type Pool struct {
	buffers []*buffer // len == BUFFER_COUNT, all pre-allocated up front, never reallocated

	// seq is the global monotonic sequence counter for this Pool (i.e. this
	// writer instance). Incremented once per successful Write, independent
	// of which Buffer the message lands in.
	seq atomic.Uint64

	// poolSize is the number of buffers in the pool (decoded from a 10-bit
	// field, so it fits uint16). slotCount is the number of slots per buffer
	// (decoded from a 16-bit field, so it also fits uint16). slotSize is the
	// number of bytes per slot (decoded from a 32-bit field, so it needs
	// uint32). All three are the post-fieldCeiling values, i.e. the actual
	// sizes the pool was allocated with.
	poolSize  uint16
	slotCount uint16
	slotSize  uint32

	// slotShift is log2(slotCount), precomputed once since slotCount is
	// guaranteed a power of two by fieldCeiling. Write derives which buffer a
	// seq belongs to via seq >> slotShift instead of recomputing this on the
	// hot path.
	slotShift uint8
}

func (pool *Pool) Write(reader io.Reader) error {
	seq := pool.seq.Add(1)
	bufMask := uint64(pool.poolSize) - 1
	idx := (seq >> pool.slotShift) & bufMask
	buf := pool.buffers[idx]

	for {
		switch bufferState(buf.state.Load()) {
		case stateActive:
			err := buf.write(reader, seq)
			if errors.Is(err, ErrSlotClaimFailed) {
				runtime.Gosched()
			}
		default:
			runtime.Gosched()
		}
	}
}

// CreatePool decodes bufferDetails into (pool size, slots per buffer, bytes
// per slot) and allocates a Pool sized accordingly. Layout, from the low bit:
//
//	bits [0:10)  -> number of buffers in the pool
//	bits [10:26) -> slots per buffer
//	bits [26:58) -> bytes per slot
//
// For the first two fields, the topmost bit of the field is a max sentinel
// and is rejected outright: CreatePool panics rather than silently rounding
// up to the field's all-ones ceiling, since that ceiling, multiplied through
// slotCount and slotSize, would otherwise be sized large enough to OOM --
// and a deferred OOM panic from inside make() can balloon RSS first, which
// risks starving other processes on the same host before this one dies.
// Panicking at decode time means the bad config is rejected before any of
// that memory is ever requested. Below the top bit, the highest set bit
// among the remaining, lower bits defines the power-of-two ceiling, and any
// bits below that are ignored. Every other input is therefore valid -- no
// error is returned for a field that isn't an exact power of two -- and
// ceiling-1 doubles as the bitmask later code uses to wrap ring indices
// instead of using modulo. A field with no bits set at all falls back to 1,
// the smallest valid size.
//
// bufferID is reserved for future use and not yet interpreted.
func CreatePool(bufferDetails uint64, bufferID int64) (*Pool, error) {
	poolSize := fieldCeiling(bufferDetails&poolSizeMask, poolSizeBits, "pool size")
	if poolSize == 0 {
		poolSize = 1
	}

	slotCount := fieldCeiling((bufferDetails>>slotCountShift)&slotCountMask, slotCountBits, "slot count")
	if slotCount == 0 {
		slotCount = 1
	}
	slotShift := uint8(bits.Len64(slotCount) - 1)

	slotSize := (bufferDetails >> slotSizeShift) & slotSizeMask
	if slotSize == 0 {
		return nil, fmt.Errorf("slot size must be non-zero")
	}

	ringSize := poolSize * slotCount

	buffers := make([]*buffer, poolSize)
	for i := range buffers {
		slots := make([]slot, slotCount)
		for j := range slots {
			slots[j].buf = make([]byte, slotSize)
		}
		buffers[i] = &buffer{slots: slots, ringSize: ringSize}
	}
	// Every buffer defaults to stateActive (the atomic Int32 zero value),
	// which is exactly the bufferState each one should start in.

	return &Pool{
		buffers:   buffers,
		poolSize:  uint16(poolSize),
		slotCount: uint16(slotCount),
		slotSize:  uint32(slotSize),
		slotShift: slotShift,
	}, nil
}

// fieldCeiling rounds x down to the nearest power of two -- the value of its
// highest set bit, or 0 if x is 0 -- within a fieldBits-wide field, with one
// exception: the field's topmost bit is a max sentinel, not a value bearing
// bit, so finding it set panics name's caller instead of resolving to the
// field's all-ones ceiling, a value large enough to risk an OOM further down
// in CreatePool.
func fieldCeiling(x uint64, fieldBits int, name string) uint64 {
	topBit := uint64(1) << (fieldBits - 1)
	if x&topBit != 0 {
		panic(fmt.Sprintf("buffer: %s has its max sentinel bit set (bit %d) -- refusing to size a pool that large", name, fieldBits))
	}
	if x == 0 {
		return 0
	}
	return uint64(1) << (bits.Len64(x) - 1)
}
