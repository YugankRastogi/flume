package buffer

import (
	"strings"
	"sync"
	"testing"
)

// TestBufferWrite_SlotClaimExclusion verifies that when multiple goroutines
// race to claim the same slot for the same global seq, exactly one wins the
// CAS and the rest get ErrSlotClaimFailed -- the core exclusion property the
// fixed-arithmetic claim (seq-ringSize -> seq-ringSize+1) is meant to give.
func TestBufferWrite_SlotClaimExclusion(t *testing.T) {
	const slotCount = 8
	const ringSize = uint64(slotCount)
	const writers = 16

	slots := make([]slot, slotCount)
	for i := range slots {
		slots[i].buf = make([]byte, 64)
	}
	b := &buffer{slots: slots, ringSize: ringSize}

	// seq == ringSize makes expectedOld == 0, matching each slot's
	// zero-initialized claim marker without needing bootstrap seeding.
	seq := ringSize

	var wg sync.WaitGroup
	results := make([]error, writers)
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = b.write(strings.NewReader("payload"), seq)
		}(i)
	}
	wg.Wait()

	successes := 0
	for _, err := range results {
		switch err {
		case nil:
			successes++
		case ErrSlotClaimFailed:
			// expected for every loser of the race
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 successful claim, got %d", successes)
	}

	idx := seq & (uint64(slotCount) - 1)
	if got := b.slots[idx].seq.Load(); got != seq-ringSize+1 {
		t.Fatalf("slot seq = %d, want %d", got, seq-ringSize+1)
	}
	if got := b.readIdx.Load(); got != 1 {
		t.Fatalf("readIdx = %d, want 1 (bumped only on the single successful claim)", got)
	}
}
