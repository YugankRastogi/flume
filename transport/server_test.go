package transport_test

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yugank/flume/flusher"
	"github.com/yugank/flume/pool"
	"github.com/yugank/flume/transport"
)

const (
	testPoolSize  = uint64(8)
	testSlotCount = uint64(32)
	testSlotSize  = uint64(4096)

	// Bit layout from pool.CreatePool:
	//   bits [0:10)  → pool size
	//   bits [10:26) → slot count
	//   bits [26:58) → slot size
	testPoolDetails = testPoolSize | (testSlotCount << 10) | (testSlotSize << 26)
)

func newTestPool(t *testing.T) *pool.Pool {
	t.Helper()
	p, err := pool.CreatePool(testPoolDetails, 0, flusher.NoopFlusher{})
	if err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	return p
}

func startTestServer(t *testing.T, p *pool.Pool) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := transport.NewServer(p)
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { lis.Close() })
	return lis.Addr().String()
}

// TestTCP_SingleRoundtrip verifies that a single write comes back unchanged.
func TestTCP_SingleRoundtrip(t *testing.T) {
	addr := startTestServer(t, newTestPool(t))

	c, err := transport.Dial("tcp", addr, int(testSlotSize))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	want := []byte("hello flume")
	if err := c.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	buf := make([]byte, testSlotSize)
	got, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("content mismatch: got %q, want %q", got, want)
	}
}

// TestTCP_LargePayloadRoundtrip verifies that a message far larger than one TCP
// segment (and larger than the historical 64KB bufio buffer) is filled into the
// slot in full, not truncated at the first Read. A single non-looping read on
// the server side would record only the first chunk; io.ReadFull fills the slot.
func TestTCP_LargePayloadRoundtrip(t *testing.T) {
	const largeSlotSize = uint64(256 * 1024) // 256KB > one segment > 64KB
	details := testPoolSize | (testSlotCount << 10) | (largeSlotSize << 26)
	p, err := pool.CreatePool(details, 0, flusher.NoopFlusher{})
	if err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	addr := startTestServer(t, p)

	c, err := transport.Dial("tcp", addr, int(largeSlotSize))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	// Fill a full slot with a recognizable, position-dependent pattern so any
	// truncation or misalignment is caught byte-for-byte.
	want := make([]byte, largeSlotSize)
	for i := range want {
		want[i] = byte(i*31 + 7)
	}

	buf := make([]byte, largeSlotSize)
	got, err := c.WriteRead(want, buf)
	if err != nil {
		t.Fatalf("WriteRead: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got %d bytes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("content mismatch at byte %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

// TestTCP_WriteReadPipelined verifies the pipelined WriteRead method.
// Single client is the only one talking to a fresh pool, so the message
// written in each call is guaranteed to be the one read back.
func TestTCP_WriteReadPipelined(t *testing.T) {
	addr := startTestServer(t, newTestPool(t))

	c, err := transport.Dial("tcp", addr, int(testSlotSize))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	buf := make([]byte, testSlotSize)
	for i := range 64 {
		want := []byte(fmt.Sprintf("pipelined-msg-%04d", i))
		got, err := c.WriteRead(want, buf)
		if err != nil {
			t.Fatalf("WriteRead[%d]: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("WriteRead[%d]: got %q, want %q", i, got, want)
		}
	}
}

// TestTCP_DecoupledNoDataLoss is the primary black-box correctness test.
// It mirrors the "TCP decoupled" benchmark: N writer goroutines and N reader
// goroutines each hold their own connection and run simultaneously.
//
// Invariant checked: every message successfully written is read back exactly
// once. Ordering is not required.
//
// 8 writers × 8 readers × 32 ops = 256 total, fitting inside ringSize=256,
// so no ring cycling occurs and flush contention is minimal.
func TestTCP_DecoupledNoDataLoss(t *testing.T) {
	runDecoupledTest(t, 8, 8, 32)
}

// TestTCP_DecoupledNoDataLoss_RingCycles repeats the invariant across
// 512 total operations (2× ringSize), forcing the pool ring to cycle
// and flushers to run. Verifies correctness across multiple full rotations.
func TestTCP_DecoupledNoDataLoss_RingCycles(t *testing.T) {
	runDecoupledTest(t, 8, 8, 64)
}

// runDecoupledTest spins up nWriters writers and nReaders readers,
// each with their own TCP connection. Every write embeds a globally-unique
// ID. After both sides complete, the set of IDs read must equal the set
// of IDs written (no loss, no duplication).
func runDecoupledTest(t *testing.T, nWriters, nReaders, opsEach int) {
	t.Helper()

	addr := startTestServer(t, newTestPool(t))

	totalOps := nWriters * opsEach // == nReaders * opsEach by design

	var idCounter atomic.Int64

	writtenMu := sync.Mutex{}
	written := make(map[uint64]struct{}, totalOps)

	readMu := sync.Mutex{}
	read := make(map[uint64]struct{}, totalOps)

	var writeWg, readWg sync.WaitGroup

	// Writers: each goroutine claims unique IDs and embeds them in the payload.
	for g := range nWriters {
		writeWg.Add(1)
		go func(g int) {
			defer writeWg.Done()

			c, err := transport.Dial("tcp", addr, int(testSlotSize))
			if err != nil {
				t.Errorf("writer %d: Dial: %v", g, err)
				return
			}
			defer c.Close()

			payload := make([]byte, 8) // first 8 bytes carry the ID
			for range opsEach {
				id := uint64(idCounter.Add(1) - 1)
				binary.BigEndian.PutUint64(payload, id)

				if err := c.Write(payload); err != nil {
					t.Errorf("writer %d: Write id=%d: %v", g, id, err)
					continue
				}
				writtenMu.Lock()
				written[id] = struct{}{}
				writtenMu.Unlock()
			}
		}(g)
	}

	// Readers: collect IDs from incoming messages.
	for g := range nReaders {
		readWg.Add(1)
		go func(g int) {
			defer readWg.Done()

			c, err := transport.Dial("tcp", addr, int(testSlotSize))
			if err != nil {
				t.Errorf("reader %d: Dial: %v", g, err)
				return
			}
			defer c.Close()

			buf := make([]byte, testSlotSize)
			for range opsEach {
				data, err := c.Read(buf)
				if err != nil {
					t.Errorf("reader %d: Read: %v", g, err)
					continue
				}
				if len(data) < 8 {
					t.Errorf("reader %d: short payload (%d bytes)", g, len(data))
					continue
				}
				id := binary.BigEndian.Uint64(data[:8])
				readMu.Lock()
				read[id] = struct{}{}
				readMu.Unlock()
			}
		}(g)
	}

	writeWg.Wait()
	readWg.Wait()

	// Verify: no data loss, no duplication.
	if len(written) != len(read) {
		t.Errorf("set size mismatch: wrote %d distinct IDs, read %d distinct IDs",
			len(written), len(read))
	}
	for id := range written {
		if _, ok := read[id]; !ok {
			t.Errorf("data loss: id %d was written but never read", id)
		}
	}
	for id := range read {
		if _, ok := written[id]; !ok {
			t.Errorf("phantom read: id %d was read but never written", id)
		}
	}
}
