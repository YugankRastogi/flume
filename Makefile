IMAGE      ?= flume-bench
BENCH      ?= .
GOMAXPROCS ?= 1
CPUS       ?= 1
MEM        ?= 1g
BENCHTIME  ?= 10s
COUNT      ?= 3

# gRPC server image + benchmark client settings
SERVER_IMAGE   ?= flume-server
SERVER_NAME    ?= flume-server
GRPC_ADDR      ?= localhost:50051
TCP_ADDR       ?= localhost:50051
UNIX_SOCK_DIR  ?= /tmp/flume-bench
UNIX_SOCK_PATH ?= $(UNIX_SOCK_DIR)/flume.sock
POOL_SIZE      ?= 128
SLOT_COUNT     ?= 32
SLOT_SIZE      ?= 131072
MAX_WRITERS    ?= 64
MAX_READERS    ?= 64
BENCH_CONC     ?= 16
BENCH_OPS      ?= 100000
BENCH_PAYLOAD  ?= 4096
BENCH_WARMUP_DUR ?= 2s

.PHONY: bench-docker-build bench-docker-run bench-parallel-docker test bench \
        bench-grpc-server-build bench-grpc-server-up bench-grpc-server-down \
        bench-grpc-run bench-grpc \
        bench-tcp-server-up bench-tcp-server-down bench-tcp-run bench-tcp \
        bench-decoupled-run bench-decoupled \
        bench-unix-server-up bench-unix-server-down bench-unix-run bench-unix \
        bench-decoupled-unix

bench-docker-build:
	docker build -t $(IMAGE) .

bench-docker-run: bench-docker-build
	docker run --rm \
		--cpus=$(CPUS) \
		--memory=$(MEM) \
		--memory-swap=$(MEM) \
		-e GOMAXPROCS=$(GOMAXPROCS) \
		-e BENCH=$(BENCH) \
		-e BENCHTIME=$(BENCHTIME) \
		-e COUNT=$(COUNT) \
		$(IMAGE)

test:
	go test -v -race ./...

bench:
	go test -bench=$(BENCH) -benchmem -benchtime=$(BENCHTIME) -count=$(COUNT) -run=^$$ ./buffer/

bench-parallel-docker: bench-docker-build
	docker run --rm \
		--cpus="2.0" \
		--memory="1g" \
		--memory-swap="1g" \
		-e GOMAXPROCS=2 \
		-e BENCH=BenchmarkPoolParallelWriteRead \
		-e BENCHTIME=$(BENCHTIME) \
		-e COUNT=$(COUNT) \
		-e PKG=./pool/ \
		$(IMAGE)

# ── gRPC end-to-end benchmark (server in Docker, client on host) ──────────────

bench-grpc-server-build:
	docker build -f Dockerfile.server -t $(SERVER_IMAGE) .

# Start the constrained server container (2 vCPU, 1 GB RAM, 1 GB swap).
# --memory-swap=2g means total(RAM+swap)=2g → 1 GB of actual swap headroom.
bench-grpc-server-up: bench-grpc-server-build
	docker rm -f $(SERVER_NAME) 2>/dev/null || true
	docker run -d --name $(SERVER_NAME) \
		--cpus="2.0" \
		--memory="1g" \
		--memory-swap="2g" \
		-p 50051:50051 \
		-e FLUME_POOL_SIZE=$(POOL_SIZE) \
		-e FLUME_SLOT_COUNT=$(SLOT_COUNT) \
		-e FLUME_SLOT_SIZE=$(SLOT_SIZE) \
		-e FLUME_MAX_WRITERS=$(MAX_WRITERS) \
		-e FLUME_MAX_READERS=$(MAX_READERS) \
		$(SERVER_IMAGE) -transport grpc

bench-grpc-server-down:
	docker stop $(SERVER_NAME) && docker rm $(SERVER_NAME)

# Run the benchmark client against the already-running server.
bench-grpc-run:
	go run ./cmd/bench \
		-addr=$(GRPC_ADDR) \
		-concurrency=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR)

# One-shot: build server, run bench, tear down — exits with bench's status code.
bench-grpc: bench-grpc-server-up
	go run ./cmd/bench \
		-addr=$(GRPC_ADDR) \
		-concurrency=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR); \
	EXIT=$$?; \
	docker stop $(SERVER_NAME); docker rm $(SERVER_NAME); \
	exit $$EXIT

# ── TCP transport end-to-end benchmark (server in Docker, client on host) ─────

bench-tcp-server-up: bench-grpc-server-build
	docker rm -f $(SERVER_NAME) 2>/dev/null || true
	docker run -d --name $(SERVER_NAME) \
		--cpus="2.0" \
		--memory="1g" \
		--memory-swap="2g" \
		-p 50051:50051 \
		-e FLUME_POOL_SIZE=$(POOL_SIZE) \
		-e FLUME_SLOT_COUNT=$(SLOT_COUNT) \
		-e FLUME_SLOT_SIZE=$(SLOT_SIZE) \
		-e FLUME_MAX_WRITERS=$(MAX_WRITERS) \
		-e FLUME_MAX_READERS=$(MAX_READERS) \
		$(SERVER_IMAGE) -transport tcp -addr :50051

bench-tcp-server-down:
	docker stop $(SERVER_NAME) && docker rm $(SERVER_NAME)

bench-tcp-run:
	go run ./cmd/bench-transport \
		-network=tcp \
		-addr=$(TCP_ADDR) \
		-concurrency=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR) \
		-read-buf=$(SLOT_SIZE)

bench-tcp: bench-tcp-server-up
	go run ./cmd/bench-transport \
		-network=tcp \
		-addr=$(TCP_ADDR) \
		-concurrency=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR) \
		-read-buf=$(SLOT_SIZE); \
	EXIT=$$?; \
	docker stop $(SERVER_NAME); docker rm $(SERVER_NAME); \
	exit $$EXIT

# ── Decoupled write/read benchmark (writers and readers run simultaneously) ───

bench-decoupled-run:
	go run ./cmd/bench-decoupled \
		-network=tcp \
		-addr=$(TCP_ADDR) \
		-write-conc=$(BENCH_CONC) \
		-read-conc=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR) \
		-read-buf=$(SLOT_SIZE)

bench-decoupled: bench-tcp-server-up
	go run ./cmd/bench-decoupled \
		-network=tcp \
		-addr=$(TCP_ADDR) \
		-write-conc=$(BENCH_CONC) \
		-read-conc=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR) \
		-read-buf=$(SLOT_SIZE); \
	EXIT=$$?; \
	docker stop $(SERVER_NAME); docker rm $(SERVER_NAME); \
	exit $$EXIT

# ── Unix socket end-to-end benchmark (server in Docker, client on host via bind mount) ──

bench-unix-server-up: bench-grpc-server-build
	mkdir -p $(UNIX_SOCK_DIR)
	rm -f $(UNIX_SOCK_PATH)
	docker rm -f $(SERVER_NAME) 2>/dev/null || true
	docker run -d --name $(SERVER_NAME) \
		--cpus="2.0" \
		--memory="1g" \
		--memory-swap="2g" \
		-v $(UNIX_SOCK_DIR):$(UNIX_SOCK_DIR) \
		-e FLUME_POOL_SIZE=$(POOL_SIZE) \
		-e FLUME_SLOT_COUNT=$(SLOT_COUNT) \
		-e FLUME_SLOT_SIZE=$(SLOT_SIZE) \
		-e FLUME_MAX_WRITERS=$(MAX_WRITERS) \
		-e FLUME_MAX_READERS=$(MAX_READERS) \
		$(SERVER_IMAGE) -transport unix -addr $(UNIX_SOCK_PATH)

bench-unix-server-down:
	docker stop $(SERVER_NAME) && docker rm $(SERVER_NAME)

bench-unix-run:
	go run ./cmd/bench-transport \
		-network=unix \
		-addr=$(UNIX_SOCK_PATH) \
		-concurrency=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR) \
		-read-buf=$(SLOT_SIZE)

bench-unix: bench-unix-server-up
	go run ./cmd/bench-transport \
		-network=unix \
		-addr=$(UNIX_SOCK_PATH) \
		-concurrency=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR) \
		-read-buf=$(SLOT_SIZE); \
	EXIT=$$?; \
	docker stop $(SERVER_NAME); docker rm $(SERVER_NAME); \
	exit $$EXIT

bench-decoupled-unix: bench-unix-server-up
	go run ./cmd/bench-decoupled \
		-network=unix \
		-addr=$(UNIX_SOCK_PATH) \
		-write-conc=$(BENCH_CONC) \
		-read-conc=$(BENCH_CONC) \
		-ops=$(BENCH_OPS) \
		-payload=$(BENCH_PAYLOAD) \
		-warmup-dur=$(BENCH_WARMUP_DUR) \
		-read-buf=$(SLOT_SIZE); \
	EXIT=$$?; \
	docker stop $(SERVER_NAME); docker rm $(SERVER_NAME); \
	exit $$EXIT
