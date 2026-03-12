package rtsp

import (
	"context"
	"errors"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestUdpConnectionOpen_ConnectionOpened(t *testing.T) {
	ub := newMockUdpBinder([]udpReader{newMockUdpReader(make([][]byte, 0)), newMockUdpReader(make([][]byte, 0))})
	c := newUdpConnectionWithDialer(net.IP{}, nil, nil, nil, ub)

	err := c.Open(context.Background())
	if err != nil {
		t.Fatalf("expected error %s, got %s", errTransport, err)
	}

	if c.rtpConn == nil {
		t.Fatal("rtpConn is nil")
	}

	if c.rtcpConn == nil {
		t.Fatal("rtcpConn is nil")
	}
}

func TestUdpConnectionOpen_AllocationFailed(t *testing.T) {
	tests := []struct {
		name string

		readers           []udpReader
		failedReaderIndex int
	}{
		{
			name:              "RTP",
			readers:           []udpReader{newMockUdpReader(make([][]byte, 0)), newMockUdpReader(make([][]byte, 0))},
			failedReaderIndex: 0,
		},
		{
			name:              "RTCP",
			readers:           []udpReader{newMockUdpReader(make([][]byte, 0)), newMockUdpReader(make([][]byte, 0))},
			failedReaderIndex: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ub := newMockUdpBinderError(tt.readers, tt.failedReaderIndex)
			c := newUdpConnectionWithDialer(net.IP{}, nil, nil, nil, ub)

			err := c.Open(context.Background())
			if !errors.Is(err, errTransport) {
				t.Fatalf("expected error %s, got %s", errTransport, err)
			}

			if c.rtpConn != nil {
				t.Fatal("rtpConn is nil")
			}
			if c.rtcpConn != nil {
				t.Fatal("rtcpConn is nil")
			}
		})
	}
}

func TestUdpConnectionOnRTP(t *testing.T) {
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

		binder            udpBinder
		expectedRTPPacket *rtp.Packet
		expectedError     error
	}{
		{
			name: "RTP packet received",

			binder:            newMockUdpBinder([]udpReader{newMockUdpReader([][]byte{pktByte}), newMockUdpReader(make([][]byte, 0))}),
			expectedRTPPacket: pkt,
		},
		{
			name: "partial body RTP packet received",

			binder:            newMockUdpBinder([]udpReader{newMockUdpReader([][]byte{pktByte[:8]}), newMockUdpReader(make([][]byte, 0))}),
			expectedRTPPacket: nil,
			expectedError:     errors.New("RTP header size insufficient"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan *rtp.Packet)
			chErr := make(chan error)

			c := newUdpConnectionWithDialer(net.IP{}, func(p *rtp.Packet) {
				ch <- p
			},
				func(p rtcp.Packet) {},
				func(err error) {
					chErr <- err
				}, tt.binder)

			err := c.Open(context.Background())
			if err != nil {
				t.Fatalf("%v", err)
			}

			if err != nil {
				t.Fatalf("%v", err)
			}

			select {
			case p := <-ch:
				if !reflect.DeepEqual(p, tt.expectedRTPPacket) {
					t.Fatalf("expected: %v, got: %v", tt.expectedRTPPacket, p)
				}
			case err := <-chErr:
				if !strings.Contains(err.Error(), tt.expectedError.Error()) {
					t.Fatalf("expected: %v, got: %v", tt.expectedError, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("timeout")
			}
		})
	}
}

func TestUDPConnectionOnRTCPPacket(t *testing.T) {
	pkt := &rtcp.PictureLossIndication{}
	pktByte, _ := pkt.Marshal()

	tests := []struct {
		name string

		binder             udpBinder
		expectedRTCPPacket rtcp.Packet
		expectedError      error
	}{
		{
			name: "received",

			binder:             newMockUdpBinder([]udpReader{newMockUdpReader(make([][]byte, 0)), newMockUdpReader([][]byte{pktByte})}),
			expectedRTCPPacket: pkt,
		},
		{
			name: "partial received",

			binder:             newMockUdpBinder([]udpReader{newMockUdpReader(make([][]byte, 0)), newMockUdpReader([][]byte{pktByte[:8]})}),
			expectedRTCPPacket: nil,
			expectedError:      errors.New("packet too short"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan rtcp.Packet)
			chErr := make(chan error)
			c := newUdpConnectionWithDialer(net.IP{}, func(p *rtp.Packet) {},
				func(p rtcp.Packet) {
					ch <- p
				},
				func(err error) {
					chErr <- err
				}, tt.binder)

			err := c.Open(context.Background())
			if err != nil {
				t.Fatalf("%v", err)
			}

			if err != nil {
				t.Fatalf("%v", err)
			}

			select {
			case p := <-ch:
				if !reflect.DeepEqual(p, tt.expectedRTCPPacket) {
					t.Fatalf("expected: %v, got: %v", tt.expectedRTCPPacket, p)
				}
			case err := <-chErr:
				if !strings.Contains(err.Error(), tt.expectedError.Error()) {
					t.Fatalf("expected: %v, got: %v", tt.expectedError, err)
				}
			case <-time.After(time.Second):
				t.Fatalf("timeout")
			}
		})
	}
}

func TestUdpConnectionClose(t *testing.T) {
	ub := newMockUdpBinder([]udpReader{newMockUdpReader(make([][]byte, 0)), newMockUdpReader(make([][]byte, 0))})
	c := newUdpConnectionWithDialer(net.IP{}, nil, nil, nil, ub)

	err := c.Open(context.Background())
	if err != nil {
		t.Fatalf("expected error %s, got %s", errTransport, err)
	}

	err = c.Close()
	if err != nil {
		t.Fatalf("expected error %s, got %s", errTransport, err)
	}

	if c.rtpConn != nil {
		t.Fatal("rtpConn is not nil")
	}

	if c.rtcpConn != nil {
		t.Fatal("rtcpConn is not nil")
	}
}

type mockUdpBinder struct {
	readers  []udpReader
	errIndex int
	err      error
	inc      int
	mu       sync.Mutex
}

func newMockUdpBinder(readers []udpReader) *mockUdpBinder {
	return &mockUdpBinder{readers: readers}
}

func newMockUdpBinderError(readers []udpReader, i int) *mockUdpBinder {
	return &mockUdpBinder{errIndex: i, err: errTransport}
}

func (m *mockUdpBinder) ListenUDP(laddr *net.UDPAddr) (udpReader, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() {
		m.inc++
	}()

	if m.err != nil && m.inc == m.errIndex {
		return nil, m.err
	}

	return m.readers[m.inc], nil
}

type mockUdpReader struct {
	outputs [][]byte
	inc     int
	mu      sync.Mutex
}

func newMockUdpReader(outputs [][]byte) *mockUdpReader {
	return &mockUdpReader{outputs: outputs}
}

func (m *mockUdpReader) LocalAddr() net.Addr {
	return &net.UDPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	}
}

func (m *mockUdpReader) ReadFromUDP(b []byte) (int, *net.UDPAddr, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() {
		m.inc++
	}()

	if len(m.outputs) <= m.inc {
		return 0, nil, &net.OpError{}
	}

	output := m.outputs[m.inc]
	copy(b, output)

	return len(output), nil, nil
}

func (m *mockUdpReader) Close() error {
	return nil
}
