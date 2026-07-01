package main

import (
	"flag"
	"log"
	"net"

	grpcserver "github.com/yugank/flume/grpc"
	"github.com/yugank/flume/pool"
	pb "github.com/yugank/flume/proto"
	"github.com/yugank/flume/transport"
	"google.golang.org/grpc"
)

func main() {
	transportType := flag.String("transport", "tcp", "transport to use: tcp, unix, or grpc")
	addr := flag.String("addr", ":50051", "listen address (for unix: socket path, e.g. /tmp/flume.sock)")
	flag.Parse()

	if pool.DefaultPool == nil {
		log.Fatal("pool not initialized: provide flume.json or set FLUME_POOL_SIZE/SLOT_COUNT/SLOT_SIZE")
	}

	switch *transportType {
	case "tcp", "unix":
		srv := transport.NewServer(pool.DefaultPool)
		log.Fatal(srv.ListenAndServe(*transportType, *addr))

	case "grpc":
		lis, err := net.Listen("tcp", *addr)
		if err != nil {
			log.Fatalf("listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterFlumeServer(s, grpcserver.NewServer(pool.DefaultConfig.MaxWriters, pool.DefaultConfig.MaxReaders))
		log.Printf("flume gRPC listening on %s (writers=%d readers=%d)",
			*addr, pool.DefaultConfig.MaxWriters, pool.DefaultConfig.MaxReaders)
		log.Fatal(s.Serve(lis))

	default:
		log.Fatalf("unknown transport %q: use tcp, unix, or grpc", *transportType)
	}
}
