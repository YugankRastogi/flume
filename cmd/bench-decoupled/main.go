// Decoupled write/read benchmark for the Flume pool server.
// Writer goroutines and reader goroutines run simultaneously and independently.
// The only invariant: total successful writes == total successful reads.
// Compare against cmd/bench-transport (paired WriteRead) to observe throughput differences.
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
	writeConc := flag.Int("write-conc", 8, "number of writer goroutines (each gets its own connection)")
	readConc := flag.Int("read-conc", 8, "number of reader goroutines (each gets its own connection)")
	totalOps := flag.Int("ops", 100000, "total write ops to issue (equal number of reads will run)")
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

	fmt.Println("Flume Decoupled Benchmark")
	fmt.Printf("  network:      %s\n", *network)
	fmt.Printf("  server:       %s\n", *addr)
	fmt.Printf("  writers:      %d goroutines (1 connection each)\n", *writeConc)
	fmt.Printf("  readers:      %d goroutines (1 connection each)\n", *readConc)
	fmt.Printf("  payload:      %d bytes/write\n", *payloadSize)
	fmt.Printf("  ops:          %d writes + %d reads\n", *totalOps, *totalOps)
	fmt.Println()

	fmt.Printf("Warming up (%v at full concurrency)...\n", *warmupDur)
	runWarmupDur(*network, *addr, payload, *readBufSize, *writeConc+*readConc, *warmupDur)

	fmt.Printf("Benchmarking (%d writes across %d goroutines, %d reads across %d goroutines)...\n",
		*totalOps, *writeConc, *totalOps, *readConc)

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	writeLats, readLats, writtenBytes, readBytes, elapsed := runDecoupled(
		*network, *addr, payload, *readBufSize, *writeConc, *readConc, *totalOps,
	)

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	printStats(writeLats, readLats, writtenBytes, readBytes, elapsed, *payloadSize, mBefore, mAfter)
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

func runDecoupled(
	network, addr string,
	payload []byte,
	readBufSize, writeConc, readConc, totalOps int,
) (writeLats, readLats [][]time.Duration, writtenBytes, readBytes uint64, elapsed time.Duration) {
	writeLats = make([][]time.Duration, writeConc)
	readLats = make([][]time.Duration, readConc)

	writeOpsEach := totalOps / writeConc
	if writeOpsEach == 0 {
		writeOpsEach = 1
	}
	readOpsEach := totalOps / readConc
	if readOpsEach == 0 {
		readOpsEach = 1
	}

	for i := range writeLats {
		writeLats[i] = make([]time.Duration, 0, writeOpsEach)
	}
	for i := range readLats {
		readLats[i] = make([]time.Duration, 0, readOpsEach)
	}

	var (
		writeWg      sync.WaitGroup
		readWg       sync.WaitGroup
		totalWritten atomic.Uint64
		totalRead    atomic.Uint64
		writeErrs    atomic.Int64
		readErrs     atomic.Int64
	)

	start := time.Now()

	for g := range writeConc {
		writeWg.Add(1)
		go func(g int) {
			defer writeWg.Done()

			client, err := transport.Dial(network, addr, readBufSize)
			if err != nil {
				log.Printf("writer %d: dial: %v", g, err)
				writeErrs.Add(int64(writeOpsEach))
				return
			}
			defer client.Close()

			lats := writeLats[g]
			var localWritten uint64

			for range writeOpsEach {
				t0 := time.Now()
				if err := client.Write(payload); err != nil {
					writeErrs.Add(1)
					if rerr := client.Reconnect(); rerr != nil {
						log.Printf("writer %d: reconnect: %v", g, rerr)
						return
					}
					continue
				}
				localWritten += uint64(len(payload))
				lats = append(lats, time.Since(t0))
			}

			writeLats[g] = lats
			totalWritten.Add(localWritten)
		}(g)
	}

	for g := range readConc {
		readWg.Add(1)
		go func(g int) {
			defer readWg.Done()

			client, err := transport.Dial(network, addr, readBufSize)
			if err != nil {
				log.Printf("reader %d: dial: %v", g, err)
				readErrs.Add(int64(readOpsEach))
				return
			}
			defer client.Close()

			buf := make([]byte, readBufSize)
			lats := readLats[g]
			var localRead uint64

			for range readOpsEach {
				t0 := time.Now()
				data, err := client.Read(buf)
				if err != nil {
					readErrs.Add(1)
					if rerr := client.Reconnect(); rerr != nil {
						log.Printf("reader %d: reconnect: %v", g, rerr)
						return
					}
					continue
				}
				localRead += uint64(len(data))
				lats = append(lats, time.Since(t0))
			}

			readLats[g] = lats
			totalRead.Add(localRead)
		}(g)
	}

	writeWg.Wait()
	readWg.Wait()
	elapsed = time.Since(start)

	if n := writeErrs.Load(); n > 0 {
		fmt.Printf("  WARNING: %d write errors during benchmark\n", n)
	}
	if n := readErrs.Load(); n > 0 {
		fmt.Printf("  WARNING: %d read errors during benchmark\n", n)
	}

	writtenBytes = totalWritten.Load()
	readBytes = totalRead.Load()
	return
}

func printStats(
	writeLats, readLats [][]time.Duration,
	writtenBytes, readBytes uint64,
	elapsed time.Duration,
	payloadBytes int,
	mBefore, mAfter runtime.MemStats,
) {
	var allWrite, allRead []time.Duration
	for _, ls := range writeLats {
		allWrite = append(allWrite, ls...)
	}
	for _, ls := range readLats {
		allRead = append(allRead, ls...)
	}

	totalMallocs := mAfter.Mallocs - mBefore.Mallocs
	totalAllocBytes := mAfter.TotalAlloc - mBefore.TotalAlloc
	totalOps := len(allWrite) + len(allRead)
	var allocsPerOp, allocBytesPerOp float64
	if totalOps > 0 {
		allocsPerOp = float64(totalMallocs) / float64(totalOps)
		allocBytesPerOp = float64(totalAllocBytes) / float64(totalOps)
	}

	fmt.Println()
	fmt.Println("=== Results ===")
	fmt.Printf("  Wall time:            %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Allocs/op (client):   %.1f  (%.0f B/op)\n", allocsPerOp, allocBytesPerOp)
	fmt.Println()

	printSide("Writes", allWrite, writtenBytes, elapsed)
	printSide("Reads", allRead, readBytes, elapsed)

	fmt.Println("  Note: writes and reads ran simultaneously. Latency includes any")
	fmt.Println("  backpressure from the pool when one side outpaces the other.")
}

func printSide(label string, lats []time.Duration, bytes uint64, elapsed time.Duration) {
	fmt.Printf("  --- %s ---\n", label)
	if len(lats) == 0 {
		fmt.Printf("  No successful %s recorded.\n\n", label)
		return
	}
	slices.Sort(lats)
	n := len(lats)
	opsPerSec := float64(n) / elapsed.Seconds()
	mb := float64(bytes) / (1 << 20)
	mbps := mb / elapsed.Seconds()

	p := func(pct int) time.Duration {
		idx := n * pct / 100
		if idx >= n {
			idx = n - 1
		}
		return lats[idx]
	}

	fmt.Printf("  Successful ops:       %d\n", n)
	fmt.Printf("  Throughput:           %.0f ops/sec\n", opsPerSec)
	fmt.Printf("  Data:                 %.2f MB  (%.2f MB/s)\n", mb, mbps)
	fmt.Printf("  Latency p50:          %v\n", p(50))
	fmt.Printf("  Latency p90:          %v\n", p(90))
	fmt.Printf("  Latency p99:          %v\n", p(99))
	fmt.Printf("  Latency min:          %v\n", lats[0])
	fmt.Printf("  Latency max:          %v\n", lats[n-1])
	fmt.Println()
}
