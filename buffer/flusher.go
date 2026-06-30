package buffer

// Flusher is implemented by consumers that want to drain a retired buffer
// (filled and rotated out of the active slot) to durable storage. A buffer
// holds its own Flusher -- rather than Pool holding one shared across every
// buffer -- so each buffer can be wired to its own flush destination, and
// new destinations are added by implementing this interface rather than by
// modifying buffer or Pool.
//
// Flush is expected to be called once a buffer has transitioned to
// stateReadyForFlush and then stateFlushing, after it has stopped accepting
// writes. Implementations
// should drain the buffer's slots (seqLo through readIdx) to their backing
// store -- e.g. the flush-contract/store component -- and return any error
// encountered so the caller can decide how to handle a failed flush (retry,
// drop, surface to the operator, etc.) rather than Flush deciding on the
// caller's behalf.
//
// Flush must not retain the buffer or its slots beyond the call: once it
// returns, the buffer is eligible to be recycled back to stateActive and
// its slots reused for a future activation.
type Flusher interface {
	Flush() error
}

type DummyFlusher struct{}

func (df *DummyFlusher) Flush() error {
	return nil
}
