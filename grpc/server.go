package grpcserver

import (
	"bytes"
	"context"

	"github.com/yugank/flume/pool"
	pb "github.com/yugank/flume/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the Flume gRPC service backed by pool.DefaultPool.
// writers and readers are channel semaphores: each token represents one
// available worker. A non-blocking receive acquires a worker; the deferred
// send returns it. No token available → ResourceExhausted.
type Server struct {
	pb.UnimplementedFlumeServer
	writers chan struct{}
	readers chan struct{}
}

func NewServer(maxWriters, maxReaders int) *Server {
	w := make(chan struct{}, maxWriters)
	r := make(chan struct{}, maxReaders)
	for i := 0; i < maxWriters; i++ {
		w <- struct{}{}
	}
	for i := 0; i < maxReaders; i++ {
		r <- struct{}{}
	}
	return &Server{writers: w, readers: r}
}

func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	select {
	case <-s.writers:
		defer func() { s.writers <- struct{}{} }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "writer pool exhausted")
	}
	if err := pool.DefaultPool.Write(bytes.NewReader(req.Data)); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.WriteResponse{}, nil
}

func (s *Server) Read(ctx context.Context, _ *pb.ReadRequest) (*pb.ReadResponse, error) {
	select {
	case <-s.readers:
		defer func() { s.readers <- struct{}{} }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "reader pool exhausted")
	}
	buf := make([]byte, pool.DefaultPool.SlotSize())
	n, err := pool.DefaultPool.Read(buf)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.ReadResponse{Data: buf[:n]}, nil
}
