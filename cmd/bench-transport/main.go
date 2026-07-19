// TCP/Unix transport benchmark for the Flume pool server.
// Each goroutine maintains a dedicated connection and issues Write+Read pairs,
// measuring throughput, latency, and heap allocations.
// Compare against cmd/bench (gRPC) to observe the transport tax.
package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"net"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yugank/flume/transport"
)

func main() {
	network := flag.String("network", "tcp", "network: tcp or unix")
	addr := flag.String("addr", "localhost:50051", "server address (or unix socket path)")
	concurrency := flag.Int("concurrency", 16, "number of concurrent goroutines (each gets its own connection)")
	totalOps := flag.Int("ops", 100000, "total write+read pairs to issue")
	payloadSize := flag.Int("payload", 4096, "write payload size in bytes")
	readBufSize := flag.Int("read-buf", 131072, "read buffer size per goroutine (must be >= server slot_size)")
	warmupDur := flag.Duration("warmup-dur", 2*time.Second, "warmup duration at full concurrency before measuring (discarded)")
	connectTimeout := flag.Duration("connect-timeout", 30*time.Second, "time to wait for server readiness")
	flag.Parse()

	waitForServer(*network, *addr, *connectTimeout)

	payload := make([]byte, *payloadSize)
	if _, err := rand.Read(payload); err != nil {
		log.Fatalf("rand.Read: %v", err)
	}

	fmt.Println("Flume Transport Benchmark")
	fmt.Printf("  network:     %s\n", *network)
	fmt.Printf("  server:      %s\n", *addr)
	fmt.Printf("  concurrency: %d goroutines (1 connection each)\n", *concurrency)
	fmt.Printf("  payload:     %d bytes/write\n", *payloadSize)
	fmt.Printf("  ops:         %d write+read pairs\n", *totalOps)
	fmt.Println()

	fmt.Printf("Warming up (%v at full concurrency)...\n", *warmupDur)
	runWarmupDur(*network, *addr, payload, *readBufSize, *concurrency, *warmupDur)

	fmt.Printf("Benchmarking (%d pairs, %d goroutines)...\n", *totalOps, *concurrency)

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	lats, recvBytes, elapsed := runPairs(*network, *addr, payload, *readBufSize, *concurrency, *totalOps, false)

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	printStats(lats, recvBytes, elapsed, *payloadSize, mBefore, mAfter)
}

func waitForServer(network, addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	fmt.Printf("Waiting for server at %s://%s...\n", network, addr)
	for {
		c, err := net.DialTimeout(network, addr, time.Second)
		if err == nil {
			c.Close()
			fmt.Println("Server ready.")
			return
		}
		if time.Now().After(deadline) {
			log.Fatalf("server not ready after %v: %v", timeout, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func runWarmupDur(network, addr string, payload []byte, readBufSize, concurrency int, dur time.Duration) {
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	for g := range concurrency {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			client, err := transport.Dial(network, addr, readBufSize)
			if err != nil {
				log.Printf("warmup goroutine %d: dial: %v", g, err)
				return
			}
			defer client.Close()
			buf := make([]byte, readBufSize)
			for time.Now().Before(deadline) {
				if _, err := client.WriteRead(payload, buf); err != nil {
					if rerr := client.Reconnect(); rerr != nil {
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

func runPairs(network, addr string, payload []byte, readBufSize, concurrency, totalOps int, silent bool) ([][]time.Duration, uint64, time.Duration) {
	perGoroutine := totalOps / concurrency
	if perGoroutine == 0 {
		perGoroutine = 1
	}

	allLatencies := make([][]time.Duration, concurrency)
	for i := range allLatencies {
		allLatencies[i] = make([]time.Duration, 0, perGoroutine)
	}

	var wg sync.WaitGroup
	var totalRecv atomic.Uint64
	var errCount atomic.Int64

	start := time.Now()
	for g := range concurrency {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			client, err := transport.Dial(network, addr, readBufSize)
			if err != nil {
				log.Printf("goroutine %d: dial: %v", g, err)
				errCount.Add(int64(perGoroutine))
				return
			}
			defer client.Close()

			// One read buffer per goroutine — reused across all Read calls, zero allocs.
			readBuf := make([]byte, readBufSize)

			lats := allLatencies[g]
			var localRecv uint64

			for range perGoroutine {
				t0 := time.Now()
				data, err := client.WriteRead(payload, readBuf)
				if err != nil {
					errCount.Add(1)
					if rerr := client.Reconnect(); rerr != nil {
						log.Printf("goroutine %d: reconnect: %v", g, rerr)
						return
					}
					continue
				}
				localRecv += uint64(len(data))
				lats = append(lats, time.Since(t0))
			}

			allLatencies[g] = lats
			totalRecv.Add(localRecv)
		}(g)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if !silent {
		if n := errCount.Load(); n > 0 {
			fmt.Printf("  WARNING: %d errors during benchmark\n", n)
		}
	}
	return allLatencies, totalRecv.Load(), elapsed
}

func printStats(latencies [][]time.Duration, recvBytes uint64, elapsed time.Duration, payloadBytes int, mBefore, mAfter runtime.MemStats) {
	var all []time.Duration
	for _, ls := range latencies {
		all = append(all, ls...)
	}
	if len(all) == 0 {
		fmt.Println("No successful operations recorded.")
		return
	}
	slices.Sort(all)

	n := len(all)
	opsPerSec := float64(n) / elapsed.Seconds()

	totalSentBytes := uint64(n) * uint64(payloadBytes)
	sentMB := float64(totalSentBytes) / (1 << 20)
	recvMB := float64(recvBytes) / (1 << 20)
	writeMBps := sentMB / elapsed.Seconds()
	readMBps := recvMB / elapsed.Seconds()

	totalMallocs := mAfter.Mallocs - mBefore.Mallocs
	totalAllocBytes := mAfter.TotalAlloc - mBefore.TotalAlloc
	allocsPerOp := float64(totalMallocs) / float64(n)
	allocBytesPerOp := float64(totalAllocBytes) / float64(n)

	p := func(pct int) time.Duration {
		idx := n * pct / 100
		if idx >= n {
			idx = n - 1
		}
		return all[idx]
	}

	fmt.Println()
	fmt.Println("=== Results ===")
	fmt.Printf("  Successful pairs:     %d\n", n)
	fmt.Printf("  Wall time:            %v\n", elapsed.Round(time.Millisecond))
	fmt.Println()
	fmt.Printf("  Throughput:           %.0f ops/sec\n", opsPerSec)
	fmt.Println()
	fmt.Printf("  Data written:         %.2f MB  (%.2f MB/s)\n", sentMB, writeMBps)
	fmt.Printf("  Data read:            %.2f MB  (%.2f MB/s)\n", recvMB, readMBps)
	fmt.Println()
	fmt.Printf("  Allocs/op (client):   %.1f  (%.0f B/op)\n", allocsPerOp, allocBytesPerOp)
	fmt.Println()
	fmt.Printf("  Latency p50:          %v\n", p(50))
	fmt.Printf("  Latency p90:          %v\n", p(90))
	fmt.Printf("  Latency p99:          %v\n", p(99))
	fmt.Printf("  Latency min:          %v\n", all[0])
	fmt.Printf("  Latency max:          %v\n", all[n-1])
	fmt.Println()
	fmt.Println("  Note: allocs/op above counts client-side allocations only.")
	fmt.Println("  Each pair is one pipelined WriteRead (single flush, 1 RTT). Pool hot-path allocs/op = 0.")
}
