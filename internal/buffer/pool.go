package buffer

import "sync/atomic"

// Pool owns a fixed set of pre-allocated Buffers (BUFFER_COUNT, sized at
// construction) and coordinates handing writes to the active one, swapping
// to a fresh buffer when it fills, and routing full buffers off for flushing.
type Pool struct {
	buffers []*Buffer // len == BUFFER_COUNT, all pre-allocated up front, never reallocated

	// active points at the Buffer currently accepting writes. Swapped via a
	// single CompareAndSwap when it fills — the "atomic pointer swap" the
	// README describes, no mutex involved.
	active atomic.Pointer[Buffer]

	// seq is the global monotonic sequence counter for this Pool (i.e. this
	// writer instance). Incremented once per successful Write, independent
	// of which Buffer the message lands in.
	seq atomic.Uint64

	// free holds buffers currently in stateFree, ready to be activated on the
	// next swap. Reading from an empty free blocks the swap — this is the
	// mechanism that implements backpressure: if every other buffer is still
	// stateFlushing, writers block until onFlush returns one here.
	// Buffered depth is BUFFER_COUNT-1 (one buffer is always active).
	free chan *Buffer

	// onFlush is invoked when a Buffer transitions to stateFlushing (handed
	// off because it filled and was swapped out). It's a placeholder hook for
	// now — the flush-contract/store component will be wired in here later.
	// The callback is responsible for eventually returning the Buffer to free.
	onFlush func(*Buffer)
}
