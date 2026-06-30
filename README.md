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

```
Flushed data    → durable within store guarantees, fully replayable
Unflushed data  → best effort, loss on node failure, loss window is configurable
```

Every message carries a `durable` flag so readers know exactly which guarantee applies to each message they receive. No silent surprises.

The loss window is your flush interval. Short interval — more S3 writes, smaller loss window. Long interval — fewer writes, larger loss window. You own that decision.

---

## Architecture

### Write Path

```
Producer → gRPC → Writer → Lock-free Buffer Pool → Flush Contract → Store (S3/Local/Custom)
```

- Pre-allocated fixed-size buffer pool, defined at compile time via Makefile
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

```go
type SnapshotStore interface {
    Write(namespace string, seqStart, seqEnd uint64, data []byte) error
    List(namespace string, from, to uint64) ([]SnapshotMeta, error)
    Read(namespace string, seqStart, seqEnd uint64) ([]byte, error)
    Delete(namespace string, seqStart, seqEnd uint64) error
}
```

Ship with: `LocalStore`, `S3Store`, `GCSStore`, `AzureBlobStore`

Compose for durability:

```go
// Write to local AND S3 simultaneously
store := MultiStore{stores: []SnapshotStore{localStore, s3Store}}
```

---

## Flush Contracts

```go
type FlushContract interface {
    ShouldFlush(b *Buffer) bool
}

// Built-in contracts
SizeFlush{}    // flush when buffer hits capacity
TimeFlush{}    // flush on timer regardless of size
HybridFlush{}  // size OR time, whichever comes first

// Compose them
AggregateFlush{contracts: []FlushContract{sizeFlush, timeFlush}}
```

---

## gRPC API

```protobuf
service Flume {
    rpc Write(WriteRequest) returns (WriteResponse);
    rpc Subscribe(SubscribeRequest) returns (stream Message);
    rpc Ack(AckRequest) returns (AckResponse);
}

message Message {
    uint64 seq       = 1;  // monotonic, per writer instance
    bytes  data      = 2;
    string namespace = 3;
    bool   durable   = 4;  // false = still in unflushed buffer
}
```

---

## Compile-Time Configuration

Buffer pool topology is defined at compile time via Makefile — no runtime discovery overhead, no dynamic allocation, predictable memory footprint.

```makefile
# config
BUFFER_COUNT  ?= 8      # number of buffers in pool
BUFFER_SIZE   ?= 1024   # message slots per buffer
MESSAGE_CAP   ?= 4096   # max bytes per message
STORE         ?= local  # local | s3 | gcs | azure

generate:
    go generate ./...

validate:
    @echo "Memory footprint: $$(( $(BUFFER_COUNT) * $(BUFFER_SIZE) * $(MESSAGE_CAP) )) bytes"

deploy:
    make validate
    make generate
    docker build -t flume .
```

---

## Horizontal Scaling

Flume instances are fully independent. No coordination protocol between nodes. No leader election.

Scale by adding instances. Producers decide how to distribute across instances — consistent hashing, round robin, key-based, whatever fits your topology. Flume doesn't own that decision.

```bash
# need more throughput? add an instance
docker run flume --namespace metrics-1
docker run flume --namespace metrics-2
# update producer to hash across both
```

---

## Deployment Modes

**Standalone server** (primary) — gRPC server process, language agnostic, horizontal scaling, process isolation

**Embedded library** (v0.2) — import directly into Go service, zero network hop, maximum throughput, single binary

**TCP wrapper** (v0.2) — for environments that can't use gRPC

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

## Benchmarks

Measured on Apple M4 (arm64, 10 cores), Go 1.24, `GOMAXPROCS=10`.

```
BenchmarkPoolConcurrentThroughput/slot=64B-10     26590573    220.1 ns/op     145.37 MB/s
BenchmarkPoolConcurrentThroughput/slot=256B-10    27150472    221.3 ns/op     578.30 MB/s
BenchmarkPoolConcurrentThroughput/slot=1KB-10     27856284    225.4 ns/op    2271.77 MB/s
BenchmarkPoolConcurrentThroughput/slot=4KB-10     25323440    235.3 ns/op    8703.71 MB/s
BenchmarkPoolConcurrentThroughput/slot=16KB-10    17359354    345.2 ns/op   23730.35 MB/s
```

Write latency stays flat at ~220–235 ns/op from 64B through 4KB slots; throughput scales linearly with slot size. The 16KB case ticks up to 345 ns — still sub-microsecond — while sustaining ~23 GB/s aggregate across all goroutines.

---

## Buffer Pool — Implementation Status (in progress)

This section is a working handoff note for the lock-free buffer pool in `buffer/` (`pool.go`, `buffer.go`, `flusher.go`), tracking exactly where implementation stands so work can resume without re-deriving context.

### Current design

Each `buffer` cycles through three states: `stateActive -> stateReadyForFlush -> stateFlushing -> stateActive`. There is deliberately no fourth "inactive/unclaimed" state. Earlier iterations had one (a buffer sat idle until some writer raced to claim and activate it), but every claim scheme tried — an external CAS-based retirement triggered by the next window's writer, then an atomic `seqLo` lap-claim via `CompareAndSwap` — kept reintroducing the same class of bug: multiple writers racing to decide who gets to activate a given buffer, and the special-cased "buffer 0 starts pre-activated" construction defeating whichever claim mechanism was in place for that one buffer specifically.

The fix was to stop treating re-activation as a writer-side race at all. The flusher already has exclusive ownership of a buffer the moment it wins `CompareAndSwap(stateReadyForFlush, stateFlushing)` — nothing else can also be mid-flush on that buffer. So re-activation (`stateFlushing -> stateActive`) is now solely the flusher's job, done once, by the one party already guaranteed exclusive access. There's no longer a moment where writers discover an unclaimed buffer and have to resolve who claims it. `stateActive` is also now the zero value of the `bufferState` enum, so every buffer (not just buffer 0) correctly starts ready to accept writes with no special-casing in `CreatePool`.

`Pool.Write` is correspondingly simple now:
```go
switch bufferState(buf.state.Load()) {
case stateActive:
    return buf.write(reader)
default: // ReadyForFlush or Flushing -- wait for the flusher to cycle it back
    runtime.Gosched()
}
```

---

## Name

Flume — a channel that moves material fast with acceptable loss. Mining flumes traded perfect delivery for speed and volume. Same tradeoff, different century.

---

*Built from production experience running observability pipelines at scale. Kafka is wonderful for what it is. Most pipelines aren't Kafka problems.*
