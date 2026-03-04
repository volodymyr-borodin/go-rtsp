package rtsp

import (
	"bytes"
	"context"
	"errors"
	"github.com/pion/rtp"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCallRtsp(t *testing.T) {
	tests := []struct {
		name string

		conn            net.Conn
		expectedStatus  int
		expectedHeaders map[string]string
	}{
		{
			name: "OK response returned",
			conn: newMockNetConn(
				[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
				[]byte("RTSP/1.0 200 OK\r\nresh1: resv1\r\n\r\n")),

			expectedStatus:  200,
			expectedHeaders: map[string]string{"reqh1": "reqv1"},
		},
		{
			name: "Bad Request response returned",
			conn: newMockNetConn(
				[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
				[]byte("RTSP/1.0 400 Bad Request\r\nresh1: resv1\r\n\r\n")),

			expectedStatus:  400,
			expectedHeaders: map[string]string{"reqh1": "reqv1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTcpConnectionWithDialer("", newMockNetDialer(tt.conn))
			_ = conn.Open(context.Background())

			res, err := conn.DoCall(context.Background(), "DESCRIBE", "rtsp://1.1.1.1:554/stream1", map[string]string{
				"reqh1": "reqv1",
			})

			if err != nil {
				t.Fatal(err)
			}

			if res.StatusCode != tt.expectedStatus {
				t.Fatalf("expected status code: %v, got: %v", tt.expectedStatus, res.StatusCode)
			}

			if reflect.DeepEqual(res.Headers, tt.expectedHeaders) {
				t.Fatalf("expected headers: %v, got: %v", tt.expectedHeaders, res.Headers)
			}
		})
	}
}

func TestOnRTP(t *testing.T) {
	pktByte := []byte{
		0x80,       // V=2, P=0, X=0, CC=0
		0x60,       // M=0, PT=96
		0x00, 0x01, // Sequence Number = 1
		0x00, 0x00, 0x00, 0xA0, // Timestamp = 160
		0x12, 0x34, 0x56, 0x78, // SSRC = 0x12345678
		0xde, 0xad, 0xbe, 0xef, // Payload (example)
	}

	pkt := &rtp.Packet{}
	_ = pkt.Unmarshal(pktByte)

	tests := []struct {
		name string

		conn              net.Conn
		expectedRTPPacket *rtp.Packet
		expectedError     error
	}{
		{
			name: "RTP packet received",

			conn:              newMockReadWriteCloserRTP(append([]byte{'$', 0, 0, byte(len(pktByte))}, pktByte...)),
			expectedRTPPacket: pkt,
		},
		{
			name: "partial body RTP packet received",

			conn:              newMockReadWriteCloserRTP(append([]byte{'$', 0, 0, byte(len(pktByte))}, pktByte[:8]...)),
			expectedRTPPacket: nil,
			expectedError:     io.ErrUnexpectedEOF,
		},
		{
			name: "partial length RTP packet received",

			conn:              newMockReadWriteCloserRTP([]byte{'$', 0}),
			expectedRTPPacket: nil,
			expectedError:     io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTcpConnectionWithDialer("", newMockNetDialer(tt.conn))
			_ = conn.Open(context.Background())

			ch := make(chan *rtp.Packet)
			chErr := make(chan error)
			conn.OnRTPPacket(func(p *rtp.Packet) {
				ch <- p
			})
			conn.OnRTPError(func(err error) {
				chErr <- err
			})

			select {
			case p := <-ch:
				if !reflect.DeepEqual(p, tt.expectedRTPPacket) {
					t.Fatalf("expected: %v, got: %v", tt.expectedRTPPacket, p)
				}
			case err := <-chErr:
				if !errors.Is(err, tt.expectedError) {
					t.Fatalf("expected: %v, got: %v", tt.expectedError, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("timeout")
			}
		})
	}
}

type mockNetDialer struct {
	f func() net.Conn
}

func newMockNetDialer(conn net.Conn) *mockNetDialer {
	return &mockNetDialer{
		f: func() net.Conn {
			return conn
		},
	}
}

func (m mockNetDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return m.f(), nil
}

type mockNetConn struct {
	mu sync.Mutex

	// Buffers
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer

	// Expectations
	expectedWrite []byte
	response      []byte

	closed bool
}

func newMockNetConn(expectedWrite, response []byte) *mockNetConn {
	return &mockNetConn{
		readBuf:       &bytes.Buffer{},
		writeBuf:      &bytes.Buffer{},
		expectedWrite: expectedWrite,
		response:      response,
	}
}

func newMockReadWriteCloserRTP(response []byte) *mockNetConn {
	return &mockNetConn{
		readBuf:       bytes.NewBuffer(response),
		writeBuf:      &bytes.Buffer{},
		expectedWrite: make([]byte, 0),
		response:      response,
	}
}

func (m *mockNetConn) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, errors.New("write on closed mock")
	}

	m.writeBuf.Write(p)

	if bytes.Equal(p, m.expectedWrite) {
		m.readBuf.Write(m.response)
	}

	return len(p), nil
}

func (m *mockNetConn) Read(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, errors.New("read on closed mock")
	}

	if m.readBuf.Len() == 0 {
		return 0, io.EOF
	}

	return m.readBuf.Read(p)
}

func (m *mockNetConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockNetConn) LocalAddr() net.Addr {
	//TODO implement me
	panic("implement me")
}

func (m *mockNetConn) RemoteAddr() net.Addr {
	//TODO implement me
	panic("implement me")
}

func (m *mockNetConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockNetConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (m *mockNetConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}
