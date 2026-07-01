// gRPC benchmark client for the Flume pool server.
// Issues concurrent Write+Read pairs and reports throughput, latency percentiles,
// and heap allocations — distinguishing pool-layer allocs from gRPC framework allocs.
package main

import (
	"context"
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

	pb "github.com/yugank/flume/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server address")
	concurrency := flag.Int("concurrency", 16, "number of concurrent goroutines")
	totalOps := flag.Int("ops", 100000, "total write+read pairs to issue")
	payloadSize := flag.Int("payload", 4096, "write payload size in bytes")
	warmupDur := flag.Duration("warmup-dur", 2*time.Second, "warmup duration at full concurrency before measuring (discarded)")
	connectTimeout := flag.Duration("connect-timeout", 30*time.Second, "time to wait for server readiness")
	flag.Parse()

	waitForServer(*addr, *connectTimeout)

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	payload := make([]byte, *payloadSize)
	if _, err := rand.Read(payload); err != nil {
		log.Fatalf("rand.Read: %v", err)
	}

	client := pb.NewFlumeClient(conn)

	fmt.Println("Flume gRPC Benchmark")
	fmt.Printf("  server:      %s\n", *addr)
	fmt.Printf("  concurrency: %d goroutines\n", *concurrency)
	fmt.Printf("  payload:     %d bytes/write\n", *payloadSize)
	fmt.Printf("  ops:         %d write+read pairs\n", *totalOps)
	fmt.Println()

	fmt.Printf("Warming up (%v at full concurrency)...\n", *warmupDur)
	runWarmupDur(client, payload, *concurrency, *warmupDur)

	fmt.Printf("Benchmarking (%d pairs, %d goroutines)...\n", *totalOps, *concurrency)

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	lats, recvBytes, elapsed := runPairs(client, payload, *concurrency, *totalOps, false)

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	printStats(lats, recvBytes, elapsed, *payloadSize, mBefore, mAfter)
}

// waitForServer probes addr via TCP until it accepts connections or the timeout elapses.
func waitForServer(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	fmt.Printf("Waiting for server at %s...\n", addr)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
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

func runWarmupDur(client pb.FlumeClient, payload []byte, concurrency int, dur time.Duration) {
	deadline := time.Now().Add(dur)
	req := &pb.WriteRequest{Data: payload}
	readReq := &pb.ReadRequest{}
	var wg sync.WaitGroup
	for g := range concurrency {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				ctx := context.Background()
				if _, err := client.Write(ctx, req); err != nil {
					continue
				}
				client.Read(ctx, readReq) //nolint:errcheck
			}
		}(g)
	}
	wg.Wait()
}

// runPairs issues totalOps write+read pairs across concurrency goroutines.
// Returns per-goroutine latency slices, total bytes received, and wall-clock duration.
func runPairs(client pb.FlumeClient, payload []byte, concurrency, totalOps int, silent bool) ([][]time.Duration, uint64, time.Duration) {
	perGoroutine := totalOps / concurrency
	if perGoroutine == 0 {
		perGoroutine = 1
	}

	allLatencies := make([][]time.Duration, concurrency)
	for i := range allLatencies {
		allLatencies[i] = make([]time.Duration, 0, perGoroutine)
	}

	req := &pb.WriteRequest{Data: payload}
	readReq := &pb.ReadRequest{}

	var wg sync.WaitGroup
	var totalRecv atomic.Uint64
	var errCount atomic.Int64

	start := time.Now()
	for g := range concurrency {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			lats := allLatencies[g]
			var localRecv uint64
			for range perGoroutine {
				ctx := context.Background()
				t0 := time.Now()
				if _, err := client.Write(ctx, req); err != nil {
					errCount.Add(1)
					continue
				}
				resp, err := client.Read(ctx, readReq)
				if err != nil {
					errCount.Add(1)
					continue
				}
				localRecv += uint64(len(resp.Data))
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
	fmt.Println("  Note: allocs/op above counts gRPC+protobuf framework allocations (unavoidable")
	fmt.Println("  at the transport layer). Pool hot-path allocs/op = 0 — see 'make bench'.")
}
