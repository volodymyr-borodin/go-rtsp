package rtsp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/pion/rtp"
	"io"
	"net"
	"sync"
	"time"
)

var (
	ErrConnectionOpened   = errors.New("connection already opened")
	ErrConnectionClosed   = errors.New("connection closed")
	ErrMediaAlreadyExists = errors.New("media already exists")
)

type dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type tcpConnection struct {
	address string
	dialer  dialer
	conn    net.Conn

	mutex sync.Mutex

	rtspResponse    chan callResult
	rtpHandler      func(pkt *rtp.Packet)
	rtpErrorHandler func(err error)

	mediaChannel map[int]int
}

func newTcpConnection(address string) *tcpConnection {
	return newTcpConnectionWithDialer(address, &net.Dialer{})
}

func newTcpConnectionWithDialer(address string, d dialer) *tcpConnection {
	c := &tcpConnection{
		address: address,
		dialer:  d,

		rtspResponse: make(chan callResult),

		mediaChannel: make(map[int]int),
	}

	return c
}

func (c *tcpConnection) Open(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn != nil {
		return ErrConnectionOpened
	}

	conn, err := c.dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return err
	}

	c.conn = conn

	go c.run()

	return err
}

func (c *tcpConnection) OpenMedia(mediaType int, ctx context.Context) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn == nil {
		return "", ErrConnectionClosed
	}

	if _, ok := c.mediaChannel[mediaType]; ok {
		return "", ErrMediaAlreadyExists
	}

	c.mediaChannel[mediaType] = len(c.mediaChannel) * 2

	return fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", c.mediaChannel[mediaType], c.mediaChannel[mediaType]+1), nil
}

func (c *tcpConnection) OnRTPPacket(f func(pkt *rtp.Packet)) {
	c.rtpHandler = f
}

func (c *tcpConnection) OnRTPError(f func(err error)) {
	c.rtpErrorHandler = f
}

func (c *tcpConnection) DoCall(ctx context.Context, method string, url string, headers map[string]string) (Response, error) {
	if c.conn == nil {
		return Response{}, ErrConnectionClosed
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer func(conn net.Conn) {
			_ = conn.SetDeadline(time.Time{})
		}(c.conn)
	}

	err := c.send(method, url, headers)
	if err != nil {
		return Response{}, err
	}

	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	case res := <-c.rtspResponse:
		return res.r, res.err
	}
}

func (c *tcpConnection) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.conn = nil

	return err
}

func (c *tcpConnection) run() {
	reader := bufio.NewReader(c.conn)

	for {
		b, err := reader.Peek(1)
		if err != nil {
			return
		}

		if b[0] == '$' {
			_, _ = reader.ReadByte() // consume '$'
			channel, _ := reader.ReadByte()
			_ = channel

			lenBytes := make([]byte, 2)
			_, err := io.ReadFull(reader, lenBytes)
			if err != nil {
				if c.rtpErrorHandler != nil {
					c.rtpErrorHandler(err)
					continue
				}
			}
			length := int(lenBytes[0])<<8 | int(lenBytes[1])

			payload := make([]byte, length)
			_, err = io.ReadFull(reader, payload)
			if err != nil {
				if c.rtpErrorHandler != nil {
					c.rtpErrorHandler(err)
					continue
				}
			}

			var r rtp.Packet
			err = r.Unmarshal(payload)
			if err != nil {
				if c.rtpErrorHandler != nil {
					c.rtpErrorHandler(err)
					continue
				}
			}

			if c.rtpHandler != nil {
				c.rtpHandler(&r)
			}
		} else {
			response, err := readRtspResponse(reader)
			c.rtspResponse <- callResult{
				r:   response,
				err: err,
			}
		}
	}
}

func (c *tcpConnection) send(method string, url string, headers map[string]string) error {
	req := fmt.Sprintf("%s %s RTSP/1.0\r\n", method, url)

	if headers != nil {
		for k, v := range headers {
			req += fmt.Sprintf("%s: %s\r\n", k, v)
		}
	}

	req += "\r\n"

	_, err := c.conn.Write([]byte(req))

	return err
}

type callResult struct {
	r   Response
	err error
}
