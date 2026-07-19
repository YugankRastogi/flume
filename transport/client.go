package transport

import (
	"bufio"
	"errors"
	"io"
	"net"
)

// Sentinel errors returned on the hot path — pre-allocated so that error
// returns don't cause heap allocations.
var (
	errServerWrite    = errors.New("transport: server error on write")
	errServerRead     = errors.New("transport: server error on read")
	errBufferTooSmall = errors.New("transport: read buffer too small for response")
)

// Client is a single connection to a flume transport server. It is not safe
// for concurrent use — give each goroutine its own Client.
type Client struct {
	network string
	addr    string
	conn    net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
}

// Dial connects to a flume transport server at network/addr. bufSize sizes the
// per-connection bufio read/write buffers so they can hold a whole slot-sized
// message plus framing; pass the server's slot_size (or the read buffer size
// you intend to use). It is floored at connBufSize via bufSizeFor.
// network is "tcp" or "unix".
func Dial(network, addr string, bufSize int) (*Client, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	bs := bufSizeFor(bufSize)
	return &Client{
		network: network,
		addr:    addr,
		conn:    conn,
		r:       bufio.NewReaderSize(conn, bs),
		w:       bufio.NewWriterSize(conn, bs),
	}, nil
}

// Write sends data to the server and waits for an acknowledgement.
//
// Hot-path allocation goal: zero. All framing bytes are written one at a time
// via WriteByte (concrete method, no interface boxing of a local array) so
// that no [5]byte header escapes to the heap.
func (c *Client) Write(data []byte) error {
	n := uint32(len(data))
	_ = c.w.WriteByte(OpWrite)
	_ = c.w.WriteByte(byte(n >> 24))
	_ = c.w.WriteByte(byte(n >> 16))
	_ = c.w.WriteByte(byte(n >> 8))
	_ = c.w.WriteByte(byte(n))
	if _, err := c.w.Write(data); err != nil {
		return err
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	status, err := c.r.ReadByte()
	if err != nil {
		return err
	}
	if status != StatusOK {
		return errServerWrite
	}
	return nil
}

// Read requests one slot from the server and copies the data into buf.
// buf must be at least as large as the server's slot_size.
// Returns the populated sub-slice of buf.
//
// Hot-path allocation goal: zero. Response header bytes are read one at a
// time via ReadByte to avoid passing a local array through an interface call.
func (c *Client) Read(buf []byte) ([]byte, error) {
	_ = c.w.WriteByte(OpRead)
	// Length field is zero — read requests carry no payload.
	_ = c.w.WriteByte(0)
	_ = c.w.WriteByte(0)
	_ = c.w.WriteByte(0)
	_ = c.w.WriteByte(0)
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	status, err := c.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if status != StatusOK {
		return nil, errServerRead
	}
	b0, _ := c.r.ReadByte()
	b1, _ := c.r.ReadByte()
	b2, _ := c.r.ReadByte()
	b3, _ := c.r.ReadByte()
	n := uint32(b0)<<24 | uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3)
	if uint32(len(buf)) < n {
		return nil, errBufferTooSmall
	}
	if _, err := io.ReadFull(c.r, buf[:n]); err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// WriteRead sends a Write frame immediately followed by a Read frame in a
// single Flush, then reads both responses. This halves the round trips
// compared to calling Write + Read separately: the server processes OpWrite,
// sends its ack, then finds OpRead already waiting in its bufio buffer.
//
// Hot-path allocation goal: zero. Same byte-at-a-time header writes as Write
// and Read individually; no intermediate buffer.
func (c *Client) WriteRead(data []byte, buf []byte) ([]byte, error) {
	n := uint32(len(data))
	_ = c.w.WriteByte(OpWrite)
	_ = c.w.WriteByte(byte(n >> 24))
	_ = c.w.WriteByte(byte(n >> 16))
	_ = c.w.WriteByte(byte(n >> 8))
	_ = c.w.WriteByte(byte(n))
	if _, err := c.w.Write(data); err != nil {
		return nil, err
	}
	_ = c.w.WriteByte(OpRead)
	_ = c.w.WriteByte(0)
	_ = c.w.WriteByte(0)
	_ = c.w.WriteByte(0)
	_ = c.w.WriteByte(0)
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	writeStatus, err := c.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if writeStatus != StatusOK {
		return nil, errServerWrite
	}
	readStatus, err := c.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if readStatus != StatusOK {
		return nil, errServerRead
	}
	b0, _ := c.r.ReadByte()
	b1, _ := c.r.ReadByte()
	b2, _ := c.r.ReadByte()
	b3, _ := c.r.ReadByte()
	m := uint32(b0)<<24 | uint32(b1)<<16 | uint32(b2)<<8 | uint32(b3)
	if uint32(len(buf)) < m {
		return nil, errBufferTooSmall
	}
	if _, err := io.ReadFull(c.r, buf[:m]); err != nil {
		return nil, err
	}
	return buf[:m], nil
}

// Reconnect closes the current connection and dials a new one, reusing the
// existing bufio buffers (Reset is cheaper than allocating new ones).
func (c *Client) Reconnect() error {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	conn, err := net.Dial(c.network, c.addr)
	if err != nil {
		return err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c.conn = conn
	c.r.Reset(conn)
	c.w.Reset(conn)
	return nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
