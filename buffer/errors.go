package buffer

import "errors"

var ErrFlushStateInconsistent = errors.New("cannot flush inconsistent buffer")

// ErrSlotClaimFailed is returned when the CAS claiming a slot for a write
// fails because the slot's previous occupant (one lap of the ring ago)
// hasn't been retired yet. This is expected backpressure under load, not a
// bug -- callers should retry once the flush path catches up.
var ErrSlotClaimFailed = errors.New("failed to claim slot for write")
