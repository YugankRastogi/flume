package buffer

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// encodeDetails packs (poolSize, slotCount, slotSize) into the uint64 that
// CreatePool expects.
func encodeDetails(poolSize, slotCount, slotSize uint64) uint64 {
	return poolSize | (slotCount << slotCountShift) | (slotSize << slotSizeShift)
}

func mustCreatePool(tb testing.TB, poolSize, slotCount, slotSize uint64) *Pool {
	tb.Helper()
	pool, err := CreatePool(encodeDetails(poolSize, slotCount, slotSize), 0, &DummyFlusher{})
	if err != nil {
		tb.Fatalf("CreatePool: %v", err)
	}
	return pool
}

// TestPoolWrite_BasicRoundtrip verifies a single Write followed by a Read
// returns the exact bytes that were written.
// Ring: poolSize=8, slotCount=16, ringSize=128 > 64, slotCount=16.
func TestPoolWrite_BasicRoundtrip(t *testing.T) {
	pool := mustCreatePool(t, 8, 16, 256)

	want := []byte("hello, flume")
	if err := pool.Write(bytes.NewReader(want)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := make([]byte, 256)
	n, err := pool.Read(got)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got[:n], want) {
		t.Fatalf("content mismatch: got %q, want %q", got[:n], want)
	}
}

// TestPoolWrite_SequentialOrder writes a full ring's worth of distinct messages
// and verifies they are read back in the same sequential order.
// Ring: poolSize=8, slotCount=16, ringSize=128 > 64.
func TestPoolWrite_SequentialOrder(t *testing.T) {
	const (
		poolSz  = 8
		slotCnt = 16
		slotSz  = 256
	)
	pool := mustCreatePool(t, poolSz, slotCnt, slotSz)
	ringSize := poolSz * slotCnt // 128

	for i := range ringSize {
		msg := []byte(fmt.Sprintf("msg-%04d", i))
		if err := pool.Write(bytes.NewReader(msg)); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	buf := make([]byte, slotSz)
	for i := range ringSize {
		n, err := pool.Read(buf)
		if err != nil {
			t.Fatalf("Read[%d]: %v", i, err)
		}
		want := fmt.Sprintf("msg-%04d", i)
		if string(buf[:n]) != want {
			t.Fatalf("Read[%d]: got %q, want %q", i, buf[:n], want)
		}
	}
}

// TestPoolWrite_MultipleRingCycles verifies the pool stays correct across
// multiple full rotations (slot reuse after flush).
// Ring: poolSize=8, slotCount=16, ringSize=128 > 64.
func TestPoolWrite_MultipleRingCycles(t *testing.T) {
	const (
		poolSz  = 8
		slotCnt = 16
		slotSz  = 256
		cycles  = 4
	)
	pool := mustCreatePool(t, poolSz, slotCnt, slotSz)
	ringSize := poolSz * slotCnt // 128

	buf := make([]byte, slotSz)
	for cycle := range cycles {
		for i := range ringSize {
			msg := []byte(fmt.Sprintf("c%d-%04d", cycle, i))
			if err := pool.Write(bytes.NewReader(msg)); err != nil {
				t.Fatalf("cycle %d Write[%d]: %v", cycle, i, err)
			}
		}
		for i := range ringSize {
			n, err := pool.Read(buf)
			if err != nil {
				t.Fatalf("cycle %d Read[%d]: %v", cycle, i, err)
			}
			want := fmt.Sprintf("c%d-%04d", cycle, i)
			if string(buf[:n]) != want {
				t.Fatalf("cycle %d Read[%d]: got %q, want %q", cycle, i, buf[:n], want)
			}
		}
	}
}

// TestPoolWrite_Concurrent fires concurrent writers and readers and checks
// that all messages are processed without errors or deadlocks. Readers may
// spin-wait on ErrSlotClaimFailed (handled inside Pool.Read) until the
// matching write lands; correctness is preserved by the slot CAS protocol.
// Ring: poolSize=8, slotCount=32, ringSize=256 > 64.
func TestPoolWrite_Concurrent(t *testing.T) {
	const (
		poolSz  = 8
		slotCnt = 32
		slotSz  = 256
	)
	pool := mustCreatePool(t, poolSz, slotCnt, slotSz)
	ringSize := poolSz * slotCnt // 256

	procs := runtime.GOMAXPROCS(0)
	total := ringSize * 4 // 1024 base; round up so it divides evenly
	if total%procs != 0 {
		total = (total/procs + 1) * procs
	}
	perGoroutine := total / procs

	var wg sync.WaitGroup
	wg.Add(procs * 2)

	for w := range procs {
		go func(w int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("writer-%02d", w))
			for range perGoroutine {
				if err := pool.Write(bytes.NewReader(payload)); err != nil {
					t.Errorf("writer %d: Write: %v", w, err)
					return
				}
			}
		}(w)
	}

	for r := range procs {
		go func(r int) {
			defer wg.Done()
			buf := make([]byte, slotSz)
			for range perGoroutine {
				if _, err := pool.Read(buf); err != nil {
					t.Errorf("reader %d: Read: %v", r, err)
					return
				}
			}
		}(r)
	}

	wg.Wait()
}

// BenchmarkPoolThroughput measures single-goroutine write+read throughput for
// various slot sizes. Ring: poolSize=8, slotCount=32, ringSize=256.
// Peak memory per pool: 256 * 64 KB = 16 MB.
func BenchmarkPoolThroughput(b *testing.B) {
	cases := []struct {
		name   string
		slotSz uint64
	}{
		{"slot=64B", 64},
		{"slot=256B", 256},
		{"slot=1KB", 1024},
		{"slot=4KB", 4096},
		{"slot=16KB", 16384},
		{"slot=64KB", 65536},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			pool, err := CreatePool(encodeDetails(8, 32, tc.slotSz), 0, &DummyFlusher{})
			if err != nil {
				b.Fatalf("CreatePool: %v", err)
			}

			payload := bytes.Repeat([]byte("x"), int(tc.slotSz/2))
			readBuf := make([]byte, tc.slotSz)
			r := bytes.NewReader(payload)

			b.ResetTimer()
			b.SetBytes(int64(len(payload)))

			for range b.N {
				r.Reset(payload)
				if err := pool.Write(r); err != nil {
					b.Fatal(err)
				}
				if _, err := pool.Read(readBuf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPoolParallelWriteRead benchmarks write and read throughput when
// dedicated writer goroutines and reader goroutines run simultaneously.
// Half of GOMAXPROCS goroutines write; the other half read.
// Ring: poolSize=8, slotCount=32, ringSize=256.
// Peak memory per pool: 256 * 16 KB = 4 MB.
func BenchmarkPoolParallelWriteRead(b *testing.B) {
	cases := []struct {
		name   string
		slotSz uint64
	}{
		{"slot=64B", 64},
		{"slot=256B", 256},
		{"slot=1KB", 1024},
		{"slot=4KB", 4096},
		{"slot=16KB", 16384},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			pool, err := CreatePool(encodeDetails(8, 32, tc.slotSz), 0, &DummyFlusher{})
			if err != nil {
				b.Fatalf("CreatePool: %v", err)
			}

			payload := bytes.Repeat([]byte("x"), int(tc.slotSz/2))
			b.ResetTimer()
			b.SetBytes(int64(len(payload)))

			nProcs := runtime.GOMAXPROCS(0)
			nWriters := max(nProcs/2, 1)
			nReaders := nProcs - nWriters

			ops := b.N
			writerOps := ops / 2
			readerOps := ops - writerOps

			var wg sync.WaitGroup

			for range nWriters {
				wg.Go(func() {
					r := bytes.NewReader(payload)
					perGoroutine := writerOps / nWriters
					for range perGoroutine {
						r.Reset(payload)
						if err := pool.Write(r); err != nil {
							b.Error(err)
							return
						}
					}
				})
			}

			for range nReaders {
				wg.Go(func() {
					readBuf := make([]byte, tc.slotSz)
					perGoroutine := readerOps / nReaders
					for range perGoroutine {
						if _, err := pool.Read(readBuf); err != nil {
							b.Error(err)
							return
						}
					}
				})
			}

			wg.Wait()
		})
	}
}

// BenchmarkPoolConcurrentThroughput benchmarks write+read throughput across
// GOMAXPROCS goroutines for various slot sizes.
// Ring: poolSize=8, slotCount=32, ringSize=256.
// Peak memory per pool: 256 * 16 KB = 4 MB.
func BenchmarkPoolConcurrentThroughput(b *testing.B) {
	cases := []struct {
		name   string
		slotSz uint64
	}{
		{"slot=64B", 64},
		{"slot=256B", 256},
		{"slot=1KB", 1024},
		{"slot=4KB", 4096},
		{"slot=16KB", 16384},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			pool, err := CreatePool(encodeDetails(8, 32, tc.slotSz), 0, &DummyFlusher{})
			if err != nil {
				b.Fatalf("CreatePool: %v", err)
			}

			payload := bytes.Repeat([]byte("x"), int(tc.slotSz/2))
			b.ResetTimer()
			b.SetBytes(int64(len(payload)))

			b.RunParallel(func(pb *testing.PB) {
				readBuf := make([]byte, tc.slotSz)
				r := bytes.NewReader(payload)
				for pb.Next() {
					r.Reset(payload)
					if err := pool.Write(r); err != nil {
						b.Error(err)
						return
					}
					if _, err := pool.Read(readBuf); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
