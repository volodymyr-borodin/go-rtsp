package rtsp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/pion/rtcp"
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
	ErrMediaNotFound      = errors.New("media not found")
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

	commandCh            chan func()
	mediaChannel         map[string]int
	channelOnRTPPackage  map[int]func(pkt *rtp.Packet)
	channelOnRTCPPackage map[int]func(pkt *rtcp.Packet)
	channelOnRTPError    map[int]func(err error)
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

		commandCh:            make(chan func(), 1),
		mediaChannel:         make(map[string]int),
		channelOnRTPPackage:  make(map[int]func(pkt *rtp.Packet)),
		channelOnRTCPPackage: make(map[int]func(pkt *rtcp.Packet)),
		channelOnRTPError:    make(map[int]func(err error)),
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

func (c *tcpConnection) OpenMedia(ctx context.Context, mediaType string,
	onRTPPackage func(pkt *rtp.Packet),
	onRTCPPackage func(pkt *rtcp.Packet),
	onRTPError func(err error)) (string, error) {

	resCh := make(chan error)

	c.commandCh <- func() {
		if c.conn == nil {
			resCh <- ErrConnectionOpened
			return
		}

		if _, ok := c.mediaChannel[mediaType]; ok {
			resCh <- ErrMediaAlreadyExists
			return
		}

		c.mediaChannel[mediaType] = len(c.mediaChannel) * 2
		c.channelOnRTPPackage[c.mediaChannel[mediaType]] = onRTPPackage
		c.channelOnRTCPPackage[c.mediaChannel[mediaType]+1] = onRTCPPackage
		c.channelOnRTPError[c.mediaChannel[mediaType]] = onRTPError
		resCh <- nil
	}

	if err := <-resCh; err != nil {
		return "", err
	}

	return fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", c.mediaChannel[mediaType], c.mediaChannel[mediaType]+1), nil
}

func (c *tcpConnection) DoCall(ctx context.Context, method string, url string, headers map[string]string) (response, error) {
	if c.conn == nil {
		return response{}, ErrConnectionClosed
	}

	c.commandCh <- func() {
		if deadline, ok := ctx.Deadline(); ok {
			_ = c.conn.SetDeadline(deadline)
			defer func(conn net.Conn) {
				_ = conn.SetDeadline(time.Time{})
			}(c.conn)
		}

		err := c.send(method, url, headers)
		if err != nil {
			c.rtspResponse <- callResult{err: err}
		}
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
	case res := <-c.rtspResponse:
		return res.r, res.err
	case <-ctx.Done():
		return response{}, ctx.Err()
	}
}

func (c *tcpConnection) SendRTCP(ctx context.Context, mediaType string, pkt rtcp.Packet) error {
	if c.conn == nil {
		return ErrConnectionClosed
	}

	errCh := make(chan error, 1)
	c.commandCh <- func() {
		if deadline, ok := ctx.Deadline(); ok {
			_ = c.conn.SetDeadline(deadline)
			defer func(conn net.Conn) {
				_ = conn.SetDeadline(time.Time{})
			}(c.conn)
		}

		channel, ok := c.mediaChannel[mediaType]
		if !ok {
			errCh <- fmt.Errorf("%w: %s", ErrMediaNotFound, mediaType)
			return
		}
		rtcpChannel := channel + 1
		payload, err := pkt.Marshal()
		if err != nil {
			errCh <- err
			return
		}

		header := []byte{
			'$',
			byte(rtcpChannel),
			byte(len(payload) >> 8),
			byte(len(payload)),
		}
		if _, err := c.conn.Write(header); err != nil {
			errCh <- err
			return
		}
		if _, err := c.conn.Write(payload); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}

	return <-errCh
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
		case cmd := <-c.commandCh:
			cmd()
		default:
		}

		b, err := reader.Peek(1)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}

			return
		}

		if b[0] == '$' {
			_, _ = reader.ReadByte() // consume '$'
			channelByte, _ := reader.ReadByte()
			channel := int(channelByte)

			lenBytes := make([]byte, 2)
			_, err := io.ReadFull(reader, lenBytes)
			if err != nil {
				c.channelOnRTPError[channel](err)

				continue
			}
			length := int(lenBytes[0])<<8 | int(lenBytes[1])

			payload := make([]byte, length)
			_, err = io.ReadFull(reader, payload)

			if err != nil {
				c.channelOnRTPError[channel](err)
				continue
			}

			if channel%2 == 0 {
				var r rtp.Packet
				err = r.Unmarshal(payload)
				if err != nil {
					c.channelOnRTPError[channel](err)
					continue
				}

				c.channelOnRTPPackage[channel](&r)
			} else {
				pkts, err := rtcp.Unmarshal(payload)
				if err != nil {
					c.channelOnRTPError[channel](err)
					continue
				}

				for _, pkt := range pkts {
					c.channelOnRTCPPackage[channel](&pkt)
				}
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

	for k, v := range headers {
		req += fmt.Sprintf("%s: %s\r\n", k, v)
	}

	req += "\r\n"

	_, err := c.conn.Write([]byte(req))

	return err
}

type callResult struct {
	r   response
	err error
}
