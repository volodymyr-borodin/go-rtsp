package rtsp

import (
	"bytes"
	"context"
	"errors"
	"github.com/pion/rtp"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCallRtsp(t *testing.T) {
	tests := []struct {
		name string

		conn            tcpConn
		expectedStatus  int
		expectedHeaders map[string]string
	}{
		{
			name: "OK response returned",
			conn: newMockReadWriteCloser(
				[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
				[]byte("RTSP/1.0 200 OK\r\nresh1: resv1\r\n\r\n")),

			expectedStatus:  200,
			expectedHeaders: map[string]string{"reqh1": "reqv1"},
		},
		{
			name: "Bad Request response returned",
			conn: newMockReadWriteCloser(
				[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
				[]byte("RTSP/1.0 400 Bad Request\r\nresh1: resv1\r\n\r\n")),

			expectedStatus:  400,
			expectedHeaders: map[string]string{"reqh1": "reqv1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTcpConnection(tt.conn)
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

		conn              tcpConn
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
			conn := newTcpConnection(tt.conn)

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

type MockReadWriteCloser struct {
	mu sync.Mutex

	// Buffers
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer

	// Expectations
	expectedWrite []byte
	response      []byte

	closed bool
}

func newMockReadWriteCloser(expectedWrite, response []byte) *MockReadWriteCloser {
	return &MockReadWriteCloser{
		readBuf:       &bytes.Buffer{},
		writeBuf:      &bytes.Buffer{},
		expectedWrite: expectedWrite,
		response:      response,
	}
}

func newMockReadWriteCloserRTP(response []byte) *MockReadWriteCloser {
	return &MockReadWriteCloser{
		readBuf:       bytes.NewBuffer(response),
		writeBuf:      &bytes.Buffer{},
		expectedWrite: make([]byte, 0),
		response:      response,
	}
}

func (m *MockReadWriteCloser) Write(p []byte) (int, error) {
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

func (m *MockReadWriteCloser) Read(p []byte) (int, error) {
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

func (m *MockReadWriteCloser) SetDeadline(t time.Time) error {
	return nil
}

func (m *MockReadWriteCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}
