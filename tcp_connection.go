package rtsp

import (
	"bufio"
	"context"
	"fmt"
	"github.com/pion/rtp"
	"io"
	"time"
)

type tcpConn interface {
	io.ReadWriteCloser
	SetDeadline(t time.Time) error
}

type tcpConnection struct {
	conn   tcpConn
	reader *bufio.Reader

	rtspResponse    chan callResult
	rtpHandler      func(pkt *rtp.Packet)
	rtpErrorHandler func(err error)
}

func newTcpConnection(conn tcpConn) *tcpConnection {
	c := &tcpConnection{
		conn:   conn,
		reader: bufio.NewReader(conn),

		rtspResponse: make(chan callResult),
	}

	go c.run()

	return c
}

func (c *tcpConnection) OnRTPPacket(f func(pkt *rtp.Packet)) {
	c.rtpHandler = f
}

func (c *tcpConnection) OnRTPError(f func(err error)) {
	c.rtpErrorHandler = f
}

func (c *tcpConnection) DoCall(ctx context.Context, method string, url string, headers map[string]string) (Response, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer func(conn tcpConn) {
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

func (c *tcpConnection) run() {
	for {
		b, err := c.reader.Peek(1)
		if err != nil {
			fmt.Println("Read error:", err)
			return
		}

		if b[0] == '$' {
			_, _ = c.reader.ReadByte() // consume '$'
			channel, _ := c.reader.ReadByte()
			_ = channel

			lenBytes := make([]byte, 2)
			_, err := io.ReadFull(c.reader, lenBytes)
			if err != nil {
				if c.rtpErrorHandler != nil {
					c.rtpErrorHandler(err)
					continue
				}
			}
			length := int(lenBytes[0])<<8 | int(lenBytes[1])

			payload := make([]byte, length)
			_, err = io.ReadFull(c.reader, payload)
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
			response, err := readRtspResponse(c.reader)
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

func (c *tcpConnection) Close() error {
	return c.conn.Close()
}

type callResult struct {
	r   Response
	err error
}
