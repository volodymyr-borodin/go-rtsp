package rtsp

import (
	"context"
	"errors"
	"github.com/pion/rtp"
	"net"
	"sync"
)

type udpConnection struct {
	ip     net.IP
	binder udpBinder

	rtpConn  *net.UDPConn
	rtcpConn *net.UDPConn

	mutex sync.Mutex

	rtpHandler      func(pkt *rtp.Packet)
	rtpErrorHandler func(err error)
}

func newUdpConnection(ip net.IP) *udpConnection {
	return newUdpConnectionWithDialer(ip, &udpBinderImpl{})
}

func newUdpConnectionWithDialer(ip net.IP, binder udpBinder) *udpConnection {
	return &udpConnection{
		ip:     ip,
		binder: binder,
	}
}

func (c *udpConnection) Open(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	rtpConn, rtcpConn, err := c.allocateRTPRTCPPair()
	if err != nil {
		return err
	}

	c.rtpConn = rtpConn
	c.rtcpConn = rtcpConn

	go c.readRTP()

	return err
}

func (c *udpConnection) RTPPort() int {
	return c.rtpConn.LocalAddr().(*net.UDPAddr).Port
}

func (c *udpConnection) RTCPPort() int {
	return c.rtcpConn.LocalAddr().(*net.UDPAddr).Port
}

func (c *udpConnection) allocateRTPRTCPPair() (rtpConn *net.UDPConn, rtcpConn *net.UDPConn, err error) {
	// TODO: try to allocate until success
	conn1, err := c.binder.ListenUDP(&net.UDPAddr{
		IP:   c.ip,
		Port: 0,
	})
	if err != nil {
		return nil, nil, err
	}

	conn1Addr := conn1.LocalAddr().(*net.UDPAddr)
	conn1Port := conn1Addr.Port

	conn2, err := c.binder.ListenUDP(&net.UDPAddr{
		IP:   c.ip,
		Port: conn1Port + 1,
	})
	if err != nil {
		return nil, nil, err
	}

	if conn1Port%2 == 0 {
		return conn1, conn2, nil
	}

	return conn2, conn1, nil
}

func (c *udpConnection) OnRTPPacket(f func(pkt *rtp.Packet)) {
	c.rtpHandler = f
}

func (c *udpConnection) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var rtpErr error
	if c.rtpConn != nil {
		rtpErr = c.rtpConn.Close()
		c.rtpConn = nil
	}

	var rtcpErr error
	if c.rtcpConn != nil {
		rtcpErr = c.rtcpConn.Close()
		c.rtcpConn = nil
	}

	return errors.Join(rtpErr, rtcpErr)
}

func (c *udpConnection) readRTP() {
	buf := make([]byte, 1500) // typical MTU size

	for {
		n, _, err := c.rtpConn.ReadFromUDP(buf)
		if err != nil {
			if c.rtpErrorHandler != nil {
				c.rtpErrorHandler(err)
			}
			return
		}

		var pkt rtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			if c.rtpErrorHandler != nil {
				c.rtpErrorHandler(err)
			}
			continue
		}

		if c.rtpHandler != nil {
			c.rtpHandler(&pkt)
		}
	}
}

type udpBinder interface {
	ListenUDP(laddr *net.UDPAddr) (*net.UDPConn, error)
}

type udpBinderImpl struct{}

func (u udpBinderImpl) ListenUDP(laddr *net.UDPAddr) (*net.UDPConn, error) {
	return net.ListenUDP("udp", laddr)
}
