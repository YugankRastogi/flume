IMAGE      ?= flume-bench
BENCH      ?= .
GOMAXPROCS ?= 1
CPUS       ?= 1
MEM        ?= 1g
BENCHTIME  ?= 10s
COUNT      ?= 3

.PHONY: bench-docker-build bench-docker-run bench-parallel-docker test bench

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
