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

	done   chan struct{}
	doneWG sync.WaitGroup

	rtspResponse chan callResult

	mediaChannel        map[string]int
	channelOnRTPPackage map[int]func(pkt *rtp.Packet)
	channelOnRTPError   map[int]func(err error)
}

func newTcpConnection(address string) *tcpConnection {
	return newTcpConnectionWithDialer(address, &net.Dialer{})
}

func newTcpConnectionWithDialer(address string, d dialer) *tcpConnection {
	c := &tcpConnection{
		address: address,
		dialer:  d,

		done: make(chan struct{}),

		rtspResponse: make(chan callResult),

		mediaChannel:        make(map[string]int),
		channelOnRTPPackage: make(map[int]func(pkt *rtp.Packet)),
		channelOnRTPError:   make(map[int]func(err error)),
	}

	return c
}

func (c *tcpConnection) Open(ctx context.Context) error {
	if c.conn != nil {
		return ErrConnectionOpened
	}

	conn, err := c.dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return err
	}

	c.conn = conn

	c.doneWG.Add(1)
	go c.run()

	return nil
}

func (c *tcpConnection) OpenMedia(ctx context.Context, mediaType string, onRTPPackage func(pkt *rtp.Packet), onRTPError func(err error)) (string, error) {
	if c.conn == nil {
		return "", ErrConnectionClosed
	}

	if _, ok := c.mediaChannel[mediaType]; ok {
		return "", ErrMediaAlreadyExists
	}

	c.mediaChannel[mediaType] = len(c.mediaChannel) * 2
	c.channelOnRTPPackage[c.mediaChannel[mediaType]] = onRTPPackage
	c.channelOnRTPError[c.mediaChannel[mediaType]] = onRTPError

	return fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", c.mediaChannel[mediaType], c.mediaChannel[mediaType]+1), nil
}

func (c *tcpConnection) DoCall(ctx context.Context, method string, url string, headers map[string]string) (response, error) {
	if c.conn == nil {
		return response{}, ErrConnectionClosed
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer func(conn net.Conn) {
			_ = conn.SetDeadline(time.Time{})
		}(c.conn)
	}

	err := c.send(method, url, headers)
	if err != nil {
		return response{}, err
	}

	select {
	case <-ctx.Done():
		return response{}, ctx.Err()
	case res := <-c.rtspResponse:
		return res.r, res.err
	}
}

func (c *tcpConnection) Close() error {
	if c.conn == nil {
		return nil
	}

	close(c.done)
	c.doneWG.Wait()

	return nil
}

func (c *tcpConnection) run() {
	defer c.doneWG.Done()
	reader := bufio.NewReader(c.conn)

	for {
		select {
		case <-c.done:
			_ = c.conn.Close()
			c.conn = nil

			return
		default:
		}

		b, err := reader.Peek(1)
		if err != nil {
			return
		}

		if b[0] == '$' {
			_, _ = reader.ReadByte() // consume '$'
			channel, _ := reader.ReadByte()

			lenBytes := make([]byte, 2)
			_, err := io.ReadFull(reader, lenBytes)
			if err != nil {
				c.channelOnRTPError[int(channel)](err)

				continue
			}
			length := int(lenBytes[0])<<8 | int(lenBytes[1])

			payload := make([]byte, length)
			_, err = io.ReadFull(reader, payload)

			if channel%2 == 1 {
				// skip RTCP packets for now
				continue
			}

			if err != nil {
				c.channelOnRTPError[int(channel)](err)
				continue
			}

			var r rtp.Packet
			err = r.Unmarshal(payload)
			if err != nil {
				c.channelOnRTPError[int(channel)](err)
				continue
			}

			c.channelOnRTPPackage[int(channel)](&r)
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
	r   response
	err error
}
