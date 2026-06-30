FROM golang:1.26-bookworm

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV GOFLAGS=-mod=readonly

CMD ["sh", "-c", \
     "GOMAXPROCS=${GOMAXPROCS:-1} go test \
       -bench=${BENCH:-.} \
       -benchmem \
       -benchtime=${BENCHTIME:-10s} \
       -count=${COUNT:-3} \
       -run=^$ \
       ./buffer/"]
