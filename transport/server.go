// Package transport implements the flume wire protocol over raw TCP or Unix
// domain sockets. The protocol is intentionally minimal:
//
//	Client → Server: [1-byte opcode][4-byte uint32 payload length][payload]
//	Server → Client (write ack): [1-byte status]
//	Server → Client (read resp): [1-byte status][4-byte uint32 length][data]
//
// OpWrite (0x01): client pushes a message into the pool. Length is the payload
// size; payload follows immediately. Server replies 0x00 on success, 0x01 on
// error.
//
// OpRead (0x02): client pulls a slot from the pool. Length must be 0; no
// payload follows. Server replies 0x00 + [4-byte length][data] on success,
// 0x01 on error.
//
// Per-connection allocations (bufio buffers, readBuf) are made once at
// connection setup. The hot path per message is zero allocations.
package transport

import (
	"bufio"
	"io"
	"log"
	"net"
	"sync"

	"github.com/yugank/flume/pool"
)

const (
	OpWrite   byte = 0x01
	OpRead    byte = 0x02
	StatusOK  byte = 0x00
	StatusErr byte = 0x01

	// frameHeaderSize is the wire framing overhead per message: a 1-byte opcode
	// plus a 4-byte length prefix.
	frameHeaderSize = 5

	// connBufSize is the floor for a per-connection bufio buffer. Buffers are
	// sized to hold a whole slot's payload plus framing (see bufSizeFor) so a
	// full message fits without a refill; this floor keeps tiny slot sizes from
	// producing pathologically small buffers.
	connBufSize = 64*1024 + 8
)

// bufSizeFor returns the per-connection bufio buffer size that holds one
// slotSize payload plus its frame header, never smaller than connBufSize. This
// is what lets the bufio buffers mimic the pool's slot size.
func bufSizeFor(slotSize int) int {
	if n := slotSize + frameHeaderSize; n > connBufSize {
		return n
	}
	return connBufSize
}

// limitedReaderPool recycles io.LimitedReader values so that the server's
// write path doesn't allocate one per message (io.LimitReader would otherwise
// allocate on every call).
var limitedReaderPool = sync.Pool{
	New: func() any { return new(io.LimitedReader) },
}

// Server serves the flume transport protocol over any net.Listener.
type Server struct {
	p *pool.Pool
}

func NewServer(p *pool.Pool) *Server {
	return &Server{p: p}
}

// Serve accepts connections from lis and serves each in its own goroutine.
// Blocks until lis is closed or an accept error occurs.
func (s *Server) Serve(lis net.Listener) error {
	defer lis.Close()
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		// Disable Nagle on TCP — we flush explicitly after each response, so
		// coalescing only adds latency.
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		go s.serveConn(conn)
	}
}

// ListenAndServe accepts connections on network/addr and serves each in its
// own goroutine. Blocks until the listener is closed or an accept error occurs.
func (s *Server) ListenAndServe(network, addr string) error {
	lis, err := net.Listen(network, addr)
	if err != nil {
		return err
	}
	log.Printf("flume transport listening on %s://%s", network, addr)
	return s.Serve(lis)
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()

	bufSize := bufSizeFor(int(s.p.SlotSize()))
	r := bufio.NewReaderSize(conn, bufSize)
	w := bufio.NewWriterSize(conn, bufSize)

	// readBuf is allocated once per connection and reused for every OpRead.
	readBuf := make([]byte, s.p.SlotSize())

	for {
		// Read the 5-byte frame header one byte at a time via ReadByte so that
		// no local array needs to escape through an io.Reader interface call.
		op, err := r.ReadByte()
		if err != nil {
			return
		}
		b0, _ := r.ReadByte()
		b1, _ := r.ReadByte()
		b2, _ := r.ReadByte()
		b3, err := r.ReadByte()
		if err != nil {
			return
		}
		n := uint32(b0)<<24 | uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3)

		switch op {
		case OpWrite:
			lr := limitedReaderPool.Get().(*io.LimitedReader)
			lr.R = r
			lr.N = int64(n)

			writeErr := s.p.Write(lr, int(n))

			// Drain any bytes not consumed by pool.Write (payload > slotSize).
			// Keeps the stream in sync regardless of write outcome.
			if lr.N > 0 {
				_, _ = io.Copy(io.Discard, lr)
			}
			lr.R = nil
			limitedReaderPool.Put(lr)

			if writeErr != nil {
				_ = w.WriteByte(StatusErr)
			} else {
				_ = w.WriteByte(StatusOK)
			}
			_ = w.Flush()

		case OpRead:
			read, readErr := s.p.Read(readBuf)
			if readErr != nil {
				_ = w.WriteByte(StatusErr)
				_ = w.Flush()
				continue
			}
			// Write the 5-byte response header byte-by-byte to avoid a
			// [5]byte local array escaping through a bufio.Writer.Write call.
			_ = w.WriteByte(StatusOK)
			_ = w.WriteByte(byte(read >> 24))
			_ = w.WriteByte(byte(read >> 16))
			_ = w.WriteByte(byte(read >> 8))
			_ = w.WriteByte(byte(read))
			_, _ = w.Write(readBuf[:read])
			_ = w.Flush()

		default:
			return
		}
	}
}
