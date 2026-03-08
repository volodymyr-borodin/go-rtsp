package rtsp

import (
	"bytes"
	"context"
	"errors"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestTcpConnectionOpen_DialContextFailed(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newErrMockNetDialer(errTransport))

	err := conn.Open(context.Background())
	if !errors.Is(err, errTransport) {
		t.Fatalf("expected error %s, got %s", errTransport, err)
	}
}

func TestTcpConnectionOpen_ConnectionAlreadyOpened(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newMockNetDialer(newMockNetConn(make([]byte, 0), make([]byte, 0), 0)))

	err := conn.Open(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}

	err = conn.Open(context.Background())
	if err == nil {
		t.Fatal("expected error, got none")
	}

	if !errors.Is(err, ErrConnectionOpened) {
		t.Fatalf("expected error %s, got %s", ErrConnectionOpened, err)
	}
}

func TestTcpConnectionOpenMedia_TracksMedia(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newMockNetDialer(newMockNetConn(make([]byte, 0), make([]byte, 0), 0)))
	err := conn.Open(context.Background())
	if err != nil {
		t.Fatalf("%v", err)
	}

	transport, err := conn.OpenMedia(context.Background(), "media1",
		func(pkt *rtp.Packet) {}, func(pkt *rtcp.Packet) {}, func(err error) {})
	if err != nil {
		t.Fatalf("%v", err)
	}

	if transport != "RTP/AVP/TCP;unicast;interleaved=0-1" {
		t.Fatalf("transport: %s", transport)
	}

	transport2, err := conn.OpenMedia(context.Background(), "media2",
		func(pkt *rtp.Packet) {}, func(pkt *rtcp.Packet) {}, func(err error) {})
	if err != nil {
		t.Fatalf("%v", err)
	}

	if transport2 != "RTP/AVP/TCP;unicast;interleaved=2-3" {
		t.Fatalf("transport: %s", transport)
	}
}

func TestTcpConnectionOpenMedia_MediaDuplicated(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newMockNetDialer(newMockNetConn(make([]byte, 0), make([]byte, 0), 0)))
	err := conn.Open(context.Background())
	if err != nil {
		t.Fatalf("%v", err)
	}

	transport, err := conn.OpenMedia(context.Background(), "media1",
		func(pkt *rtp.Packet) {}, func(pkt *rtcp.Packet) {}, func(err error) {})
	if err != nil {
		t.Fatalf("%v", err)
	}

	if transport != "RTP/AVP/TCP;unicast;interleaved=0-1" {
		t.Fatalf("transport: %s", transport)
	}

	transport, err = conn.OpenMedia(context.Background(), "media1",
		func(pkt *rtp.Packet) {}, func(pkt *rtcp.Packet) {}, func(err error) {})
	if !errors.Is(err, ErrMediaAlreadyExists) {
		t.Fatalf("%v", err)
	}

	if transport != "" {
		t.Fatalf("transport: %s", transport)
	}
}

func TestTcpConnectionDoCall(t *testing.T) {
	tests := []struct {
		name string

		conn net.Conn

		expectedStatus  int
		expectedHeaders map[string]string
		expectedError   error

		timeout time.Duration
	}{
		{
			name: "OK",
			conn: newMockNetConn(
				[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
				[]byte("RTSP/1.0 200 OK\r\nresh1: resv1\r\n\r\n"),
				0),

			expectedStatus:  200,
			expectedHeaders: map[string]string{"reqh1": "reqv1"},
		},
		{
			name: "Bad Request response returned",
			conn: newMockNetConn(
				[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
				[]byte("RTSP/1.0 400 Bad Request\r\nresh1: resv1\r\n\r\n"),
				0),

			expectedStatus:  400,
			expectedHeaders: map[string]string{"reqh1": "reqv1"},
		},
		{
			name: "timeout",
			conn: newMockNetConn(
				[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
				[]byte("RTSP/1.0 200 OK\r\nresh1: resv1\r\n\r\n"),
				time.Millisecond*200),

			expectedError: context.DeadlineExceeded,

			timeout: time.Millisecond * 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTcpConnectionWithDialer("", newMockNetDialer(tt.conn))
			err := conn.Open(context.Background())
			if err != nil {
				t.Fatalf("%v", err)
			}

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			res, err := conn.DoCall(ctx, "DESCRIBE", "rtsp://1.1.1.1:554/stream1", map[string]string{
				"reqh1": "reqv1",
			})

			if tt.expectedError != nil {
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}

				if res.StatusCode != tt.expectedStatus {
					t.Fatalf("expected status code: %v, got: %v", tt.expectedStatus, res.StatusCode)
				}

				if reflect.DeepEqual(res.Headers, tt.expectedHeaders) {
					t.Fatalf("expected headers: %v, got: %v", tt.expectedHeaders, res.Headers)
				}
			}
		})
	}
}

func TestTcpConnectionOnRTPPacket(t *testing.T) {
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

			err := conn.Open(context.Background())
			if err != nil {
				t.Fatalf("%v", err)
			}

			ch := make(chan *rtp.Packet)
			chErr := make(chan error)
			_, err = conn.OpenMedia(context.Background(), "media1",
				func(p *rtp.Packet) {
					ch <- p
				},
				func(p *rtcp.Packet) {},
				func(err error) {
					chErr <- err
				})

			if err != nil {
				t.Fatalf("%v", err)
			}

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

func TestTcpConnectionSendRTCP(t *testing.T) {
	tests := []struct {
		name string

		conn   *mockNetConn
		packet rtcp.Packet

		expectedBuffer []byte
	}{
		{
			name: "OK",

			conn:   newMockNetConn(make([]byte, 0), make([]byte, 0), 0),
			packet: &rtcp.PictureLossIndication{},

			expectedBuffer: []byte{'$', 1, 0, 12},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTcpConnectionWithDialer("", newMockNetDialer(tt.conn))
			err := conn.Open(context.Background())
			if err != nil {
				t.Fatalf("%v", err)
			}

			_, err = conn.OpenMedia(context.Background(), "media1",
				func(p *rtp.Packet) {},
				func(p *rtcp.Packet) {},
				func(err error) {})
			if err != nil {
				t.Fatalf("%v", err)
			}

			err = conn.SendRTCP(context.Background(), "media1", tt.packet)
			if err != nil {
				t.Fatalf("%v", err)
			}

			if !bytes.HasPrefix(tt.conn.writeBuf.Bytes(), tt.expectedBuffer) {
				t.Fatalf("expected: %v, got: %v", tt.expectedBuffer, tt.conn.writeBuf.Bytes())
			}
		})
	}
}

func TestTcpConnectionSendRTCP_NoMediaSetup(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newMockNetDialer(newMockNetConn(make([]byte, 0), make([]byte, 0), 0)))
	err := conn.Open(context.Background())
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = conn.SendRTCP(context.Background(), "media1", &rtcp.PictureLossIndication{})
	if !errors.Is(err, ErrMediaNotFound) {
		t.Fatalf("%v", err)
	}
}

func TestTcpConnectionSendRTCP_ConnectionClosed(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newMockNetDialer(newMockNetConn(make([]byte, 0), make([]byte, 0), 0)))

	err := conn.SendRTCP(context.Background(), "media1", &rtcp.PictureLossIndication{})
	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("%v", err)
	}
}

func TestTcpConnectionClose_Opened(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newMockNetDialer(newMockNetConn(
		[]byte("DESCRIBE rtsp://1.1.1.1:554/stream1 RTSP/1.0\r\nreqh1: reqv1\r\n\r\n"),
		[]byte("RTSP/1.0 200 OK\r\nresh1: resv1\r\n\r\n"), 0)))

	err := conn.Open(context.Background())
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func TestTcpConnectionClose_Closed(t *testing.T) {
	conn := newTcpConnectionWithDialer("", newMockNetDialer(newMockNetConn(make([]byte, 0), make([]byte, 0), 0)))

	err := conn.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
}

type mockNetDialer struct {
	err error
	f   func() net.Conn
}

func newMockNetDialer(conn net.Conn) *mockNetDialer {
	return &mockNetDialer{
		f: func() net.Conn {
			return conn
		},
	}
}

func newErrMockNetDialer(err error) *mockNetDialer {
	return &mockNetDialer{
		err: err,
	}
}

func (m mockNetDialer) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	if m.err != nil {
		return nil, m.err
	}

	return m.f(), nil
}

type mockNetConn struct {
	delay time.Duration
	mu    sync.Mutex

	// Buffers
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer

	// Expectations
	expectedWrite []byte
	response      []byte

	closed bool
}

func newMockNetConn(expectedWrite, response []byte, delay time.Duration) *mockNetConn {
	return &mockNetConn{
		delay:         delay,
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
		time.Sleep(m.delay)

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

func (m *mockNetConn) SetDeadline(_ time.Time) error {
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

func (m *mockNetConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (m *mockNetConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

func (m *mockNetConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}
