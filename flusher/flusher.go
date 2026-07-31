package flusher

import (
	"io"
	"math/rand"
	"time"
)

// Flusher is implemented by consumers that want to drain a retired buffer
// (filled and rotated out of the active slot) to durable storage. A buffer
// holds its own Flusher -- rather than Pool holding one shared across every
// buffer -- so each buffer can be wired to its own flush destination, and
// new destinations are added by implementing this interface rather than by
// modifying buffer or Pool.
//
// Flush is expected to be called once a buffer has transitioned to
// stateReadyForFlush and then stateFlushing, after it has stopped accepting
// writes. Implementations should drain the buffer's slots to their backing
// store and return any error encountered so the caller can decide how to
// handle a failed flush (retry, drop, surface to the operator, etc.) rather
// than Flush deciding on the caller's behalf.
//
// Flush must not retain the buffer or its slots beyond the call: once it
// returns, the buffer is eligible to be recycled back to stateActive and
// its slots reused for a future activation.
//
// The reader begins with an 8-byte big-endian length prefix giving the
// number of payload bytes that follow (the same framing buffer.Buffer
// stores each slot in) -- implementations that need the payload size (e.g.
// to set Content-Length without buffering the whole body to measure it) can
// read that prefix off the front rather than buffering or seeking.
// A single trailing call with a nil reader marks the end of a flush pass.
type Flusher interface {
	Flush(io.Reader) error
}

// NoopFlusher discards retired buffer data instantly with no latency.
// Use it in tests and benchmarks that want to isolate the pool/transport
// hot path from storage-simulation overhead.
type NoopFlusher struct{}

func (NoopFlusher) Flush(io.Reader) error { return nil }

type DummyFlusher struct{}

func (df *DummyFlusher) Flush(io.Reader) error {
	<-time.After(simulateS3PutLatency())
	return nil
}

func simulateS3PutLatency() time.Duration {
	r := rand.Float64()
	switch {
	case r < 0.50:
		return time.Duration(50+rand.Intn(40)) * time.Millisecond // p50 zone: 50-90ms
	case r < 0.95:
		return time.Duration(90+rand.Intn(60)) * time.Millisecond // p95 zone: 90-150ms
	case r < 0.99:
		return time.Duration(150+rand.Intn(100)) * time.Millisecond // p99 zone: 150-250ms
	default:
		return time.Duration(250+rand.Intn(250)) * time.Millisecond // tail: occasional 250-500ms spike
	}
}
