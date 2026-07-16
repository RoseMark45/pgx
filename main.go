package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

// MockServer simulates a PostgreSQL server for the COPY protocol.
type MockServer struct {
	conn net.Conn
}

func (s *MockServer) Run() {
	defer s.conn.Close()
	for {
		var msgType [1]byte
		_, err := io.ReadFull(s.conn, msgType[:])
		if err != nil {
			return
		}
		var lengthBytes [4]byte
		_, err = io.ReadFull(s.conn, lengthBytes[:])
		if err != nil {
			return
		}
		length := binary.BigEndian.Uint32(lengthBytes[:]) - 4
		body := make([]byte, length)
		_, err = io.ReadFull(s.conn, body)
		if err != nil {
			return
		}

		switch msgType[0] {
		case 'Q': // Query
			sql := string(body)
			if strings.Contains(sql, "CREATE TEMP TABLE") {
				s.writeMessage('C', []byte("CREATE TABLE"))
				s.writeMessage('Z', []byte{'I'})
			} else if strings.Contains(sql, "COPY test_copy_fail") {
				s.writeMessage('I', []byte{})
				s.handleCopy()
			} else if strings.Contains(sql, "SELECT 1") {
				s.writeMessage('T', []byte("?column?"))
				s.writeMessage('D', []byte("1"))
				s.writeMessage('C', []byte("SELECT 1"))
				s.writeMessage('Z', []byte{'I'})
			} else {
				s.writeMessage('E', []byte("unknown query"))
				s.writeMessage('Z', []byte{'I'})
			}
		}
	}
}

func (s *MockServer) handleCopy() {
	var copyErr error
	seenIDs := make(map[string]bool)
	for {
		var msgType [1]byte
		_, err := io.ReadFull(s.conn, msgType[:])
		if err != nil {
			return
		}
		var lengthBytes [4]byte
		_, err = io.ReadFull(s.conn, lengthBytes[:])
		if err != nil {
			return
		}
		length := binary.BigEndian.Uint32(lengthBytes[:]) - 4
		body := make([]byte, length)
		_, err = io.ReadFull(s.conn, body)
		if err != nil {
			return
		}

		switch msgType[0] {
		case 'd': // CopyData
			if copyErr == nil {
				row := string(body)
				parts := strings.Split(row, ",")
				if len(parts) > 0 {
					id := parts[0]
					if seenIDs[id] {
						copyErr = fmt.Errorf("duplicate key value violates unique constraint")
						s.writeMessage('E', []byte(copyErr.Error()))
					} else {
						seenIDs[id] = true
					}
				}
			}
		case 'f': // CopyFail
			s.writeMessage('Z', []byte{'I'})
			return
		case 'c': // CopyDone
			if copyErr != nil {
				s.writeMessage('Z', []byte{'I'})
			} else {
				s.writeMessage('C', []byte("COPY 1"))
				s.writeMessage('Z', []byte{'I'})
			}
			return
		}
	}
}

func (s *MockServer) writeMessage(msgType byte, body []byte) {
	s.conn.Write([]byte{msgType})
	var lengthBytes [4]byte
	binary.BigEndian.PutUint32(lengthBytes[:], uint32(len(body)+4))
	s.conn.Write(lengthBytes[:])
	s.conn.Write(body)
}

// Conn represents a connection to the database.
type Conn struct {
	netConn net.Conn
	closed  bool
	dirty   bool
}

func (c *Conn) Close() error {
	c.closed = true
	return c.netConn.Close()
}

func (c *Conn) IsClosed() bool {
	return c.closed
}

func (c *Conn) IsDirty() bool {
	return c.dirty
}

func (c *Conn) writeMessage(msgType byte, body []byte) error {
	var header [5]byte
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(body)+4))
	if _, err := c.netConn.Write(header[:]); err != nil {
		c.dirty = true
		return err
	}
	if _, err := c.netConn.Write(body); err != nil {
		c.dirty = true
		return err
	}
	return nil
}

func (c *Conn) readMessage() (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(c.netConn, header[:]); err != nil {
		c.dirty = true
		return 0, nil, err
	}
	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5]) - 4
	body := make([]byte, length)
	if _, err := io.ReadFull(c.netConn, body); err != nil {
		c.dirty = true
		return 0, nil, err
	}
	return msgType, body, nil
}

func (c *Conn) Query(sql string) (string, error) {
	if c.dirty {
		return "", fmt.Errorf("connection is dirty")
	}
	if err := c.writeMessage('Q', []byte(sql)); err != nil {
		return "", err
	}

	var result string
	var queryErr error
	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			return "", err
		}
		switch msgType {
		case 'T':
			// RowDescription (ignore)
		case 'D':
			// DataRow
			result = string(body)
		case 'C':
			// CommandComplete
		case 'E':
			queryErr = fmt.Errorf("database error: %s", string(body))
		case 'Z':
			return result, queryErr
		default:
			c.dirty = true
			return "", fmt.Errorf("unexpected message type: %c", msgType)
		}
	}
}

type CopyFromSource interface {
	Next() bool
	Values() ([]interface{}, error)
	Err() error
}

func (c *Conn) CopyFrom(tableName string, columnNames []string, rowSrc CopyFromSource) error {
	sql := fmt.Sprintf("COPY %s FROM STDIN", tableName)
	if err := c.writeMessage('Q', []byte(sql)); err != nil {
		c.dirty = true
		return err
	}

	msgType, _, err := c.readMessage()
	if err != nil {
		c.dirty = true
		return err
	}
	if msgType != 'I' {
		c.dirty = true
		return fmt.Errorf("expected CopyInResponse, got %c", msgType)
	}

	var copyErr error
	for rowSrc.Next() {
		values, err := rowSrc.Values()
		if err != nil {
			copyErr = err
			break
		}
		var valStrs []string
		for _, val := range values {
			valStrs = append(valStrs, fmt.Sprintf("%v", val))
		}
		data := strings.Join(valStrs, ",")
		if err := c.writeMessage('d', []byte(data)); err != nil {
		c.dirty = true
		return err
	}
	}

	if rowSrc.Err() != nil {
		copyErr = rowSrc.Err()
	}

	if copyErr != nil {
		if err := c.writeMessage('f', []byte(copyErr.Error())); err != nil {
			c.dirty = true
			c.Close()
			return err
		}
		return c.drain(true)
	}

	if err := c.writeMessage('c', []byte{}); err != nil {
		c.dirty = true
		return err
	}

	return c.drain(false)
}

func (c *Conn) drain(isError bool) error {
	var firstErr error
	if isError {
		firstErr = fmt.Errorf("copy failed")
	}
	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			c.dirty = true
			c.Close()
			return err
		}
		switch msgType {
		case 'E':
			if firstErr == nil {
				firstErr = fmt.Errorf("database error: %s", string(body))
			}
		case 'Z':
			return firstErr
		}
	}
}

type ConnPool struct {
	conns chan *Conn
	dial  func() (*Conn, error)
}

func NewConnPool(max int, dial func() (*Conn, error)) *ConnPool {
	p := &ConnPool{
		conns: make(chan *Conn, max),
		dial:  dial,
	}
	for i := 0; i < max; i++ {
		c, err := dial()
		if err != nil {
			panic(err)
		}
		p.conns <- c
	}
	return p
}

func (p *ConnPool) Acquire() (*Conn, error) {
	c := <-p.conns
	if c.IsClosed() || c.IsDirty() {
		c.Close()
		var err error
		c, err = p.dial()
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (p *ConnPool) Release(c *Conn) {
	if c.IsDirty() || c.IsClosed() {
		c.Close()
		newConn, err := p.dial()
		if err == nil {
			p.conns <- newConn
		} else {
			p.conns <- c
		}
	} else {
		p.conns <- c
	}
}

type sliceCopyFromSource struct {
	rows [][]interface{}
	idx  int
}

func (s *sliceCopyFromSource) Next() bool {
	s.idx++
	return s.idx <= len(s.rows)
}

func (s *sliceCopyFromSource) Values() ([]interface{}, error) {
	return s.rows[s.idx-1], nil
}

func (s *sliceCopyFromSource) Err() error {
	return nil
}

func main() {
	// Start mock server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server := &MockServer{conn: conn}
			go server.Run()
		}
	}()

	dial := func() (*Conn, error) {
		netConn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			return nil, err
		}
		return &Conn{netConn: netConn}, nil
	}

	// Initialize pool with MaxConns = 1
	pool := NewConnPool(1, dial)

	// 1. Create a temporary table
	conn, err := pool.Acquire()
	if err != nil {
		log.Fatalf("failed to acquire conn: %v", err)
	}
	_, err = conn.Query("CREATE TEMP TABLE test_copy_fail ( id INT PRIMARY KEY, name TEXT );")
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	pool.Release(conn)

	// 2. Perform CopyFrom with duplicate keys
	conn, err = pool.Acquire()
	if err != nil {
		log.Fatalf("failed to acquire conn: %v", err)
	}

	src := &sliceCopyFromSource{
		rows: [][]interface{}{
			{1, "Alice"},
			{1, "Bob"}, // Duplicate ID!
		},
		idx: 0,
	}

	err = conn.CopyFrom("test_copy_fail", []string{"id", "name"}, src)
	if err == nil {
		log.Fatalf("expected error from CopyFrom, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
		log.Fatalf("expected duplicate key error, got: %v", err)
	}
	pool.Release(conn)

	// 3. Immediately execute a simple query
	conn, err = pool.Acquire()
	if err != nil {
		log.Fatalf("failed to acquire conn: %v", err)
	}
	defer pool.Release(conn)

	res, err := conn.Query("SELECT 1")
	if err != nil {
		log.Fatalf("failed to execute SELECT 1: %v", err)
	}
	if res != "1" {
		log.Fatalf("expected '1', got '%s'", res)
	}

	fmt.Println("Test passed successfully!")
}
