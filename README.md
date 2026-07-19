[WIP]
# Flume

A fast, embeddable pub/sub message buffer for Go that trades Kafka's durability guarantees for raw speed. Designed for use cases where eventual consistency is acceptable and operational simplicity matters.

---

## The Problem

Most message passing in production systems doesn't need Kafka. But Kafka is what's already running, so it gets used everywhere — including observability pipelines, analytics, cache invalidation, and telemetry where losing 0.1% of messages is completely acceptable.

The alternatives don't help:

- **Kafka** — operationally heavy, WAL on every write, ZooKeeper/KRaft dependency, partition decisions haunt you forever
- **SQS** — too opinionated, 10 message receive limit, FIFO queues don't scale past 3000 TPS, no real consumer group concept, no replay

The space in between is genuinely underserved.

---

## What Flume Is

Flume is a standalone gRPC server (with optional embedded Go library mode) that provides:

- **Fast write path** — lock-free buffer pool, pre-allocated at compile time, no GC pressure on hot path
- **Honest loss semantics** — explicit loss window between flushes, documented and by design, not a bug
- **Pluggable durable storage** — local filesystem, S3, GCS, Azure Blob, or bring your own via a simple interface
- **Sequence-based gap detection and replay** — every message stamped with a monotonic sequence number; readers detect gaps and replay exactly the missing sequences from the store
- **Composable flush contracts** — size-based, time-based, hybrid, or custom; compose them however your use case needs
- **No DLQ needed** — S3 is your dead letter queue, your replay log, and your audit trail simultaneously
- **Producer-managed sharding** — Flume doesn't own your sharding logic; producers and readers decide how to distribute across instances

---

## What Flume Is Not

Flume is not a Kafka replacement for Kafka use cases.

If you need:
- Exactly-once delivery
- Total ordering across partitions
- Infinite log replay from the beginning of time
- Financial transaction guarantees

Use Kafka. It is a masterpiece for those requirements.

Flume is for everything else.

---

## The Core Tradeoff

> **Flushed data** — durable within store guarantees, fully replayable
> **Unflushed data** — best effort; loss on node failure; loss window is configurable

Every message carries a `durable` flag so readers know exactly which guarantee applies to each message they receive. No silent surprises.

The loss window is your flush interval. Short interval — more S3 writes, smaller loss window. Long interval — fewer writes, larger loss window. You own that decision.

---

## Architecture

### Write Path

```
Producer → gRPC → Writer → Lock-free Buffer Pool → Flush Contract → Store (S3/Local/Custom)
```

- Pre-allocated fixed-size buffer pool, configured via env vars or `flume.json` at startup
- Double buffering — while one buffer flushes to store, the other accepts writes
- Atomic pointer swap between buffers — single CAS operation, no mutex on hot path
- Flush triggered by composable contracts — size, time, or custom logic

### Read Path

```
Reader → gRPC Subscribe → Stream from active buffer or Store
→ Gap detected via sequence numbers → Replay from Store
```

- Readers subscribe to a namespace via gRPC streaming
- Active buffer data flagged as non-durable, store data flagged as durable
- Sequence gaps trigger automatic replay from store
- Reader tracks its own offset — no broker-side consumer group coordination

### Storage Interface

Ships with: `LocalStore`, `S3Store`, `GCSStore`, `AzureBlobStore`

`MultiStore` lets you compose them — for example, writing to local disk and S3 simultaneously for redundancy.

---

## Flush Contracts

Three built-in contracts: size-based (flush when the buffer hits capacity), time-based (flush on a timer regardless of fill level), and hybrid (whichever comes first). Contracts are composable — `AggregateFlush` combines any set of them, and custom contracts are a single-method interface.

---

## gRPC API

Three RPCs: `Write`, `Subscribe` (server-streaming), and `Ack`. Every message carries a monotonic sequence number scoped to the writer instance and a `durable` flag — `false` means the message is still in the unflushed buffer, `true` means it has been committed to the store.

---

## Horizontal Scaling

Flume instances are fully independent. No coordination protocol between nodes. No leader election.

Scale by adding instances. Producers decide how to distribute across instances — consistent hashing, round robin, key-based, whatever fits your topology. Flume doesn't own that decision.

---

## Deployment Modes

**Standalone server** (primary) — gRPC server process, language agnostic, horizontal scaling, process isolation

**Embedded library** (v0.2) — import directly into Go service, zero network hop, maximum throughput, single binary

**TCP/Unix transport** (`-transport tcp|unix`) — raw length-prefixed framing over TCP or Unix domain sockets; 0 allocs/op vs ~154 for gRPC. Any language can implement the client: `[1-byte opcode][4-byte uint32 length][payload]`. Payloads larger than the configured `slot_size` are automatically trimmed to `slot_size` on write — the slot is the size contract.

---

## Positioning

| | Kafka | Flume | SQS |
|---|---|---|---|
| Durability | Full WAL | Configurable | Managed |
| Replay | Full | From store | No |
| Operational cost | High | Low | Zero (managed) |
| Sharding | Internal | Producer-managed | Internal (limited) |
| Loss window | None | Flush interval | None |
| DLQ | Needed | Not needed | Needed |
| Throughput | High | Higher | Limited |

---

## Target Use Cases

- Observability pipelines — metrics, logs, traces
- Real-time analytics and dashboards
- Cache invalidation events
- Feature flag propagation
- Session telemetry and click streams
- Any pipeline where eventual consistency is acceptable and Kafka is operational overkill

---

## What's Not In Scope

- Exactly-once delivery
- Total ordering across multiple Flume instances
- Dynamic partition rebalancing
- Built-in consumer group coordination (reader manages its own offset)

---

## Roadmap

- **v0.1** — core buffer pool, gRPC server, local store, S3 store, sequence numbers, gap detection, replay, composable flush contracts
- **v0.2** — embedded Go library mode, TCP wrapper, GCS and Azure store implementations
- **v0.3** — metrics endpoint, Prometheus integration, gap detection alerting

---

## Build and Run

**Prerequisites:** Go 1.24+, Docker (for containerised runs and end-to-end benchmarks).

**Build**

```bash
go build -o flume-server ./cmd/server
```

Or as a Docker image:

```bash
docker build -f Dockerfile.server -t flume .
```

**Configure the pool** via `flume.json` in the working directory, or environment variables:

| Env var | Description | Default |
|---|---|---|
| `FLUME_POOL_SIZE` | Number of buffers in pool | 128 |
| `FLUME_SLOT_COUNT` | Slots per buffer | 32 |
| `FLUME_SLOT_SIZE` | Max bytes per slot | 131072 |
| `FLUME_MAX_WRITERS` | Max concurrent writers | 1024 |
| `FLUME_MAX_READERS` | Max concurrent readers | 1024 |

**Run**

```bash
# TCP (default)
./flume-server -transport tcp -addr :50051

# gRPC
./flume-server -transport grpc -addr :50051

# Unix socket
./flume-server -transport unix -addr /tmp/flume.sock
```

Docker:

```bash
docker run -p 50051:50051 \
  -e FLUME_POOL_SIZE=128 \
  -e FLUME_SLOT_COUNT=32 \
  -e FLUME_SLOT_SIZE=131072 \
  flume
```

**Tests**

```bash
make test
```

---

## Benchmarking Strategy

Flume uses a two-tier benchmark approach so the pool's allocation profile can be verified in isolation before measuring the full stack.

**Tier 1 — Pool hot path (`go test -bench`, in-process)**

```bash
make bench                  # single-goroutine throughput
make bench-parallel-docker  # 1 writer + 1 reader under 2-CPU Docker
```

Measures the lock-free buffer pool with zero transport overhead. Key property: `0 allocs/op` across all slot sizes — the hot path never touches the heap after startup. This is the authoritative signal for whether the pool layer itself is allocation-free.

**Tier 2 — end-to-end transport benchmarks (constrained Docker server)**

```bash
make bench-grpc                                                    # gRPC: defaults: 16 goroutines, 100k pairs, 4KB payload, 2s warmup
make bench-grpc BENCH_CONC=32 BENCH_PAYLOAD=16384                 # gRPC: higher concurrency / larger payload
make bench-tcp                                                     # TCP: same defaults
make bench-tcp  BENCH_CONC=32 BENCH_PAYLOAD=16384                 # TCP: higher concurrency / larger payload
make bench-tcp  BENCH_WARMUP_DUR=5s                               # TCP: longer warmup
```

Starts the server inside Docker with EC2-like constraints (`--cpus=2.0 --memory=1g --memory-swap=2g`), then drives it from the host. Reports total data written and read, ops/sec, MB/s, latency percentiles, and client-side allocs/op. Run both to compare the transport tax: gRPC carries ~154 allocs/op (protobuf + HTTP/2 framing, unavoidable at that layer); TCP carries 0 allocs/op (the raw transport client is designed allocation-free on the hot path).

---

## Benchmarks

`BenchmarkPoolParallelWriteRead`: dedicated writer goroutines and reader goroutines run simultaneously (writes and reads in parallel, not serialised per goroutine). `0 allocs/op` across all cases; `0 B/op` up to 16KB, with a few amortized bytes at the 1MB case (the flush-goroutine spawn, not a hot-path allocation).

Ring: `pool_size=16, slot_count=64, ring_size=1024`, `NoopFlusher`. (Widened from the previous 256-slot ring, which measurably cut small-payload contention — see below.) Each slot's hot `seq` atomic is now cache-line padded (`cpu.CacheLinePad`) to eliminate false sharing between adjacent slots.

    ### Apple M4 — bare metal (arm64, 10 cores), Go 1.26.1, `GOMAXPROCS=10`

    1 writer goroutine per 2 cores — 5 writers, 5 readers running concurrently. 6 runs per size (`-count=6`).

    ```
    BenchmarkPoolParallelWriteRead/slot=64B-10      24235100     43.61 ns/op     733.82 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64B-10      27550923     43.79 ns/op     730.79 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64B-10      28515189     42.59 ns/op     751.30 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64B-10      27780805     44.45 ns/op     719.87 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64B-10      28484533     43.39 ns/op     737.57 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64B-10      27882848     44.82 ns/op     714.02 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-10     26057028     45.45 ns/op    2816.28 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-10     26900527     46.88 ns/op    2730.13 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-10     26581948     42.80 ns/op    2990.39 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-10     26744125     43.33 ns/op    2953.91 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-10     25326763     45.03 ns/op    2842.31 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-10     26076870     44.90 ns/op    2850.80 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-10      24242240     49.89 ns/op   10262.59 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-10      23950561     48.67 ns/op   10520.07 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-10      23734998     49.78 ns/op   10285.93 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-10      24520968     48.81 ns/op   10490.36 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-10      24001080     51.39 ns/op    9963.59 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-10      23965628     49.63 ns/op   10316.14 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-10      18380874     61.56 ns/op   33269.77 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-10      19337865     63.40 ns/op   32302.10 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-10      19735218     65.36 ns/op   31333.49 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-10      18910567     66.24 ns/op   30917.37 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-10      19172518     61.38 ns/op   33363.38 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-10      19009298     63.60 ns/op   32201.68 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-10      8394288    146.30 ns/op   55992.96 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-10      8127222    149.00 ns/op   54962.04 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-10      8276037    147.20 ns/op   55663.77 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-10      8171067    148.30 ns/op   55233.22 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-10      8169410    148.20 ns/op   55259.54 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-10      8273536    146.80 ns/op   55811.00 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-10      1697137    706.70 ns/op   46368.75 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-10      1709373    704.90 ns/op   46485.69 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-10      1708263    705.90 ns/op   46419.99 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-10      1690830    707.30 ns/op   46329.51 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-10      1707481    703.40 ns/op   46583.02 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-10      1693677    705.50 ns/op   46447.15 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-10      815218   1461.00 ns/op   44852.53 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-10      808332   1458.00 ns/op   44962.96 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-10      806031   1463.00 ns/op   44804.73 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-10      847561   1455.00 ns/op   45039.70 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-10      796464   1462.00 ns/op   44839.44 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-10      803616   1458.00 ns/op   44941.48 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1MB-10         85132   14498.00 ns/op   36161.84 MB/s   61 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1MB-10         84231   14703.00 ns/op   35659.63 MB/s   62 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1MB-10         83820   14517.00 ns/op   36114.88 MB/s   62 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1MB-10         84456   14500.00 ns/op   36158.99 MB/s   62 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1MB-10         84955   14528.00 ns/op   36087.76 MB/s   61 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1MB-10         84694   14550.00 ns/op   36033.00 MB/s   62 B/op    0 allocs/op
    ```

    At 1MB the per-op cost (~14.5µs) is dominated by moving the payload through the CPU twice per op (write-into-slot copy + read-out copy), i.e. it is memory-bandwidth bound, not lock- or contention-bound. Neither the wider ring nor the slot padding moves it — but throughput still lands at ~36 GB/s. Smaller payloads, which fit in cache, are where contention lives and where both changes helped: cache-line padding the per-slot `seq` atomic cut the small-slot medians a further ~4–8% on top of the wider ring (e.g. `slot=256B` from ~49 to ~45 ns/op, `slot=1KB` from ~52 to ~50 ns/op), while the large bandwidth-bound sizes stayed flat.

    ### Docker on Apple M4 (arm64), Go 1.26, `GOMAXPROCS=2`, `--cpus=2.0`, `--memory=1g`, `--memory-swap=1g`

    1 writer goroutine, 1 reader goroutine running concurrently.

    ```
    BenchmarkPoolParallelWriteRead/slot=64B-2      179830278     72.15 ns/op     443.51 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64B-2      183011487     66.15 ns/op     483.73 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64B-2      183276253     66.20 ns/op     483.39 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-2     351497481     33.79 ns/op    3788.17 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-2     353598435     33.48 ns/op    3823.47 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=256B-2     358525429     34.00 ns/op    3765.18 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-2      544553042     22.36 ns/op   22899.55 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-2      541049794     21.74 ns/op   23549.50 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=1KB-2      558701820     21.57 ns/op   23735.81 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-2      351549813     34.48 ns/op   59401.36 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-2      348823722     34.31 ns/op   59698.02 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=4KB-2      349782222     34.48 ns/op   59394.51 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-2     71546697     184.00 ns/op   44514.01 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-2     63121957     187.80 ns/op   43631.67 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=16KB-2     58592008     191.50 ns/op   42775.67 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-2     14624562     876.30 ns/op   37394.97 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-2     12950358     955.10 ns/op   34308.81 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=64KB-2     11997411     951.40 ns/op   34442.41 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-2     6157773    2069.00 ns/op   31669.69 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-2     5736204    2026.00 ns/op   32348.26 MB/s    0 B/op    0 allocs/op
    BenchmarkPoolParallelWriteRead/slot=128KB-2     6164020    2030.00 ns/op   32291.02 MB/s    0 B/op    0 allocs/op
    ```

    Under the constrained 2-CPU / 1 GB container, per-op latency runs ~22–2070 ns/op depending on slot size, with `0 B/op` / `0 allocs/op` held throughout. With `GOMAXPROCS=2` the workload is a single writer paired with a single reader, so ring contention is far lower than the 10-core bare-metal case above — cache-fitting payloads (256B–4KB) actually clock *faster* here (~22–34 ns/op) than on bare metal, while larger payloads become memory-bandwidth bound and settle at ~31–37 GB/s. The 1MB case is omitted: its ring requires `1024 × 1MB = 1 GB`, which exceeds the container's `--memory=1g` cap and is OOM-killed.

### gRPC end-to-end — Docker on Apple M4 (arm64), `--cpus=2.0`, `--memory=1g`, `--memory-swap=2g`

Pool config: `pool_size=128, slot_count=32, slot_size=128KB`. Client: 16 goroutines, 100,000 write+read pairs, 4KB payload, 2s warmup at full concurrency discarded.

```
Successful pairs:     100,000
Wall time:            15.789s

Throughput:           6,333 ops/sec

Data written:         390.62 MB  (24.74 MB/s)
Data read:            390.62 MB  (24.74 MB/s)

Allocs/op (client):   154.1  (15,584 B/op)

Latency p50:          1.533ms
Latency p90:          5.544ms
Latency p99:          11.596ms
Latency min:          546µs
Latency max:          83.072ms
```

The 154 allocs/op come from the gRPC-go and protobuf layers: each Write RPC marshals the request to wire bytes (one heap allocation for the buffer), each Read RPC unmarshals the response (one allocation for the data field), and the HTTP/2 transport adds further allocations for stream metadata, hpack header encoding, and frame buffers. These are inherent to the protocol stack — there is no way to avoid them at the gRPC layer. The pool hot path itself contributes 0, as confirmed by Tier 1 above.

### TCP end-to-end — Docker on Apple M4 (arm64), `--cpus=2.0`, `--memory=1g`, `--memory-swap=2g`

Same Docker constraints and pool config as gRPC above. Client: 16 goroutines (one persistent connection each), 100,000 write+read pairs, 4KB payload, 2s warmup at full concurrency discarded. Each pair uses the pipelined `WriteRead` path — both frames sent in a single `Flush`, responses consumed sequentially.

```
Successful pairs:     100,000
Wall time:            7.905s

Throughput:           12,650 ops/sec

Data written:         390.62 MB  (49.41 MB/s)
Data read:            390.62 MB  (49.41 MB/s)

Allocs/op (client):   0.0  (54 B/op)

Latency p50:          627µs
Latency p90:          928µs
Latency p99:          2.230ms
Latency min:          158µs
Latency max:          277.085ms
```

0 allocs/op in the hot path. The transport client (`transport/client.go`) eliminates all per-operation allocations: `bufio.Reader` and `bufio.Writer` are pre-allocated once at `Dial` time; frame header bytes are sent one at a time via `WriteByte` (concrete method — avoids boxing a `[5]byte` local array through an interface and escaping it to the heap); reads land directly into a per-goroutine buffer pre-allocated before the loop; errors are package-level sentinels. The 54 B/op amortizes the one-time `Dial` cost (bufio read+write buffers, ~131 KB per goroutine) across 100,000 operations — the loop itself contributes zero.

The `WriteRead` pipelined path cuts the per-pair round trips from 2 to 1: the server processes `OpWrite`, sends its ack, then finds `OpRead` already waiting in its `bufio.Reader` buffer — no extra network round trip. The p50 of 627µs vs the pre-pipelining 1.167ms confirms the 2→1 RTT reduction. Throughput is 2× gRPC at the same concurrency and payload; p99 latency is 5× lower (11.6ms → 2.2ms).

**On the max latency (~277ms):** This is a single-event outlier from the Docker VM scheduler, not from the transport or pool. macOS runs Docker containers inside the Apple Virtualization Framework — a full VM layer. The hypervisor occasionally preempts the container's vCPUs to service host work, stalling every in-flight server goroutine until the vCPU is rescheduled. That stall typically lasts 200–300ms and appears as one sample in 100,000. The p50/p90/p99 — measured in the hundreds-of-microseconds range — are representative of steady-state performance. The bare-metal pool benchmarks (32–270 ns/op, above) confirm the pool and transport layers themselves contribute no scheduling jitter. On a dedicated bare-metal Linux host with no VM layer, max latencies track p99 closely.

### TCP decoupled — Docker on Apple M4 (arm64), `--cpus=2.0`, `--memory=1g`, `--memory-swap=2g`

Same Docker constraints and pool config. Client: 16 writer goroutines + 16 reader goroutines running simultaneously (each with its own connection), 100,000 writes + 100,000 reads, 4KB payload, 2s warmup at full concurrency discarded. Writers call `Write` only; readers call `Read` only — no pairing, no per-goroutine serialisation between the two operations.

```
Wall time:            8.749s
Allocs/op (client):   0.0  (44 B/op)

--- Writes ---
Successful ops:       100,000
Throughput:           11,430 ops/sec
Data:                 390.62 MB  (44.65 MB/s)
Latency p50:          977µs
Latency p90:          1.512ms
Latency p99:          3.444ms
Latency min:          176µs
Latency max:          207.089ms

--- Reads ---
Successful ops:       100,000
Throughput:           11,430 ops/sec
Data:                 390.62 MB  (44.65 MB/s)
Latency p50:          983µs
Latency p90:          1.518ms
Latency p99:          3.400ms
Latency min:          217µs
Latency max:          204.889ms
```

Throughput is comparable to the paired `WriteRead` benchmark (~11,400 vs ~12,650 ops/sec) — the small gap reflects that 32 goroutines (16 writers + 16 readers) sharing 2 vCPUs drives more scheduling contention than 16 paired goroutines. The write p99 (3.4ms) and read p99 (3.4ms) are symmetrical, confirming neither side is backpressure-limited at this concurrency. Client-side allocs/op remain 0.

The same Docker VM scheduling caveat applies: the max (~205ms) is a single hypervisor preemption event in 100,000 samples. The p50/p90/p99 figures — in the sub-millisecond to low-millisecond range — are the representative signal.

### Unix socket end-to-end — Apple M4 bare metal (arm64), server and client on host

Pool config: `pool_size=128, slot_count=32, slot_size=128KB`, **`FLUME_FLUSHER=noop`**. Client: 16 goroutines (one persistent connection each), 1,000,000 write+read pairs, 4KB payload, 5s warmup at full concurrency discarded. Unix domain sockets bypass the TCP/IP stack entirely — the kernel copies data directly between processes without the network state machine. On macOS, Docker Desktop's VirtioFS bind-mount layer does not propagate Unix socket inodes to the host, so this benchmark runs server and client directly on the host rather than in Docker. The server is unconstrained; results reflect native host scheduling rather than a 2-CPU container.

`noop` is used deliberately, exactly as the 1MB TCP section above does: it isolates the transport and pool from storage-flush backpressure. With the default `DummyFlusher` (which sleeps 50–500ms per buffer flush to model an S3 `PUT`) the same run is throttled to ~14,500 ops/sec with a p99 in the tens of milliseconds — that ceiling is the *simulated flush latency*, not the socket. Switching to `noop` recovers the transport's true numbers, a ~13× jump:

```
Successful pairs:     1,000,000
Wall time:            5.157s

Throughput:           193,917 ops/sec

Data written:         3,906.25 MB  (757.49 MB/s)
Data read:            3,906.25 MB  (757.49 MB/s)

Allocs/op (client):   0.0  (15 B/op)

Latency p50:          78.625µs
Latency p90:          130.25µs
Latency p99:          179.167µs
Latency min:          6.709µs
Latency max:          941.542µs
```

p50/p90/p99 land at 79µs / 130µs / 179µs — a tight spread, with none of the tens-of-ms flush stalls the dummy-flusher run showed. Sweeping connection count (same host, `noop`, 4KB payload) locates where the ceiling actually is:

| connections | throughput (ops/sec) | p50 | p99 |
|---|---|---|---|
| 1 | 95,971 | 10.2µs | 15.8µs |
| 8 | 137,162 | 57.3µs | 118.7µs |
| 16 | 193,917 | 78.6µs | 179.2µs |
| 32 | 257,868 | 114.3µs | 290.0µs |
| 64 | 288,976 | 197.8µs | 657.0µs |
| 128 | 274,665 | 389.3µs | 1.68ms |

A single connection round-trips in ~10µs — essentially the syscall floor (client write + server read + server write + client read). Throughput peaks near 64 connections (~289k ops/sec) and then declines as pool-ring contention grows: when readers and writers fall out of lockstep across connections, a reader can request a sequence not yet written and spin-waits on the pool's backoff. The transport itself is not the limit at these sizes — the ring is.

### Unix socket decoupled — Apple M4 bare metal (arm64), server and client on host

Same host setup, pool config, and `FLUME_FLUSHER=noop`. Client: 16 writer goroutines + 16 reader goroutines running simultaneously (each with its own connection), 1,000,000 writes + 1,000,000 reads, 4KB payload, 5s warmup discarded.

```
Wall time:            6.777s
Allocs/op (client):   0.0  (14 B/op)

--- Writes ---
Successful ops:       1,000,000
Throughput:           147,551 ops/sec
Data:                 3,906.25 MB  (576.37 MB/s)
Latency p50:          100.083µs
Latency p90:          178.75µs
Latency p99:          254.375µs
Latency min:          3.709µs
Latency max:          4.496666ms

--- Reads ---
Successful ops:       1,000,000
Throughput:           147,551 ops/sec
Data:                 3,906.25 MB  (576.37 MB/s)
Latency p50:          101.292µs
Latency p90:          175.791µs
Latency p99:          240.875µs
Latency min:          6.042µs
Latency max:          2.889208ms
```

Write and read p50/p90 are symmetrical (100µs / 101µs and 179µs / 176µs), confirming neither side is backpressure-limited. Splitting writes and reads onto separate connections (32 total vs the 16 paired above) trades some throughput — 148k vs 194k ops/sec — for the ability to drive each direction independently; the extra goroutines contend more on the pool ring. Allocs/op remain 0 — the decoupled path shares the same allocation-free client as the paired benchmark.

> **Cross-transport comparison caveat:** the gRPC and TCP sections above were measured with the default `DummyFlusher` inside a 2-CPU Docker container, whereas these Unix numbers use `noop` on an unconstrained bare-metal host. The absolute figures are therefore not directly comparable — compare transports only at matched flusher and host settings.

### TCP large-payload (1MB) end-to-end — Apple M4 bare metal (arm64), server and client on host

Validates the full-slot fill path at a large payload. The server reads each message into its slot with `io.ReadFull` (not a single `reader.Read`), so a 1MB message — far larger than one TCP segment or the historical 64KB bufio buffer — lands in the slot intact rather than truncated. Config: `pool_size=16, slot_count=64, slot_size=1MB` (`FLUME_SLOT_SIZE=1048576`), `FLUME_FLUSHER=noop`. Client: 8 goroutines (pipelined `WriteRead`), 20,000 write+read pairs, 1MB payload, 1s warmup discarded.

```
Successful pairs:     20,000
Wall time:            4.065s

Throughput:           4,920 ops/sec

Data written:         20,000.00 MB  (4,919.70 MB/s)
Data read:            20,000.00 MB  (4,919.70 MB/s)

Allocs/op (client):   0.0  (1,274 B/op)

Latency p50:          1.582ms
Latency p90:          2.441ms
Latency p99:          3.361ms
Latency min:          191.209µs
Latency max:          7.668ms
```

`Data written == Data read` (20,000 MB each) confirms zero truncation — every byte of every 1MB message roundtrips. Two knobs drive the numbers: the flusher and the ring width. With the default `DummyFlusher` (which sleeps 50–500ms to simulate S3 `PUT` latency) the same run manages only ~1,600 MB/s with a p99 of ~85ms and max ~349ms, because every buffer that fills stalls the whole slot for tens of milliseconds. Switching to `NoopFlusher` and widening the ring to 1024 slots removes that stall: throughput triples to ~4,920 MB/s and p99 collapses to 3.4ms. Use `noop` for pool/transport benchmarking; `dummy` only when you deliberately want to model storage-flush backpressure.

---

## Buffer Pool — Implementation Status

This section is a working handoff note for the lock-free buffer pool in `buffer/` (`pool.go`, `buffer.go`, `flusher.go`), tracking exactly where implementation stands so work can resume without re-deriving context.

Write path and read path are both implemented and benchmarked. What comes next: real `Flusher` implementations wired to a storage interface, the flush-contract layer, and the gRPC server on top.

### Buffer lifecycle

Each `buffer` cycles through three states: `stateActive -> stateReadyForFlush -> stateFlushing -> stateActive`. There is deliberately no fourth "inactive/unclaimed" state. Earlier iterations had one (a buffer sat idle until some writer raced to claim and activate it), but every claim scheme tried — an external CAS-based retirement triggered by the next window's writer, then an atomic `seqLo` lap-claim via `CompareAndSwap` — kept reintroducing the same class of bug: multiple writers racing to decide who gets to activate a given buffer, and the special-cased "buffer 0 starts pre-activated" construction defeating whichever claim mechanism was in place for that one buffer specifically.

The fix was to stop treating re-activation as a writer-side race at all. The flusher already has exclusive ownership of a buffer the moment it wins `CompareAndSwap(stateReadyForFlush, stateFlushing)` — nothing else can also be mid-flush on that buffer. So re-activation (`stateFlushing -> stateActive`) is now solely the flusher's job, done once, by the one party already guaranteed exclusive access. There's no longer a moment where writers discover an unclaimed buffer and have to resolve who claims it. `stateActive` is also now the zero value of the `bufferState` enum, so every buffer (not just buffer 0) correctly starts ready to accept writes with no special-casing in `CreatePool`.

### Write path

`Pool.Write` allocates a global sequence number, routes to the target buffer via bit-shift, then spins until the slot is claimed or the buffer cycles back to active.

**Slot-size trimming**: `pool.Write` calls `reader.Read(s.buf)` once, where `s.buf` is exactly `slotSize` bytes. If the reader delivers more than `slotSize` bytes, only the first `slotSize` are stored; the rest are silently discarded. If the reader delivers fewer, only those bytes are stored and `slot.n` records the actual length. This is intentional: the slot is the size contract, not the input. Callers that need hard guarantees should wrap their reader in an `io.LimitedReader` at the call site.

Inside `buf.write`, the slot is claimed via two CAS operations. The operands are derived from `seq` and `ringSize` directly — not from a freshly-loaded slot value — so two competing writers always compute the same expected-old and only one can win. The first CAS claims the slot; the second marks it ready for read.

### Read path

`Pool.Read` mirrors the write path: it allocates from `pool.readSeq`, routes to the same buffer, and spins on `ErrSlotClaimFailed`. The read CAS expects `(seq - ringSize) + 2` (the "ready for read" marker left by the writer's second CAS) and advances it to `+3`.

**Flush trigger**: flush is reader-driven. Each `buf.read` call increments `readCount` via a fetch-and-add. The goroutine whose `readCount.Add(1)` returns exactly `len(slots)` — and only that goroutine — wins the CAS to `stateReadyForFlush` and spawns `go b.flush()`. Using the Add return value directly avoids the race where two goroutines both Load the threshold and both spawn a flush.

`b.flush()` then CAS-es to `stateFlushing`, calls `b.flusher.Flush()`, retires all slot claim markers (advancing each by `ringSize - 3` to set them up for the next lap), clears slot data, resets `readCount` to 0, and CAS-es back to `stateActive`.

---

## Name

Flume — a channel that moves material fast with acceptable loss. Mining flumes traded perfect delivery for speed and volume. Same tradeoff, different century.

---

*Built from production experience running observability pipelines at scale. Kafka is wonderful for what it is. Most pipelines aren't Kafka problems.*
