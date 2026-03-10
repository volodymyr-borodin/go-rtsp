package rtsp

import (
	"context"
	"errors"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"net"
	"sync"
)

type udpConnection struct {
	ip     net.IP
	binder udpBinder

	rtpDone   chan struct{}
	rtpDoneWG sync.WaitGroup

	rtcpDone   chan struct{}
	rtcpDoneWG sync.WaitGroup

	rtpConn  *net.UDPConn
	rtcpConn *net.UDPConn

	onRTPPackage  func(pkt *rtp.Packet)
	onRTCPPackage func(pkt *rtcp.Packet)
	onRTPError    func(err error)
}

func newUdpConnection(ip net.IP, onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) *udpConnection {
	return newUdpConnectionWithDialer(ip, onRTPPackage, onRTCPPackage, onRTPError, &udpBinderImpl{})
}

func newUdpConnectionWithDialer(ip net.IP, onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error), binder udpBinder) *udpConnection {
	return &udpConnection{
		ip:     ip,
		binder: binder,

		rtpDone:  make(chan struct{}),
		rtcpDone: make(chan struct{}),

		onRTPPackage:  onRTPPackage,
		onRTCPPackage: onRTCPPackage,
		onRTPError:    onRTPError,
	}
}

func (c *udpConnection) Open(ctx context.Context) error {
	rtpConn, rtcpConn, err := c.allocateRTPRTCPPair()
	if err != nil {
		return err
	}

	c.rtpConn = rtpConn
	c.rtpDoneWG.Add(1)
	go c.readRTP()

	c.rtcpConn = rtcpConn
	c.rtcpDoneWG.Add(1)
	go c.readRTCP()

	return nil
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
	c.onRTPPackage = f
}

func (c *udpConnection) OnRTCPPacket(f func(pkt *rtcp.Packet)) {
	c.onRTCPPackage = f
}

func (c *udpConnection) OnRTPError(f func(err error)) {
	c.onRTPError = f
}

func (c *udpConnection) Close() error {
	close(c.rtpDone)
	c.rtpDoneWG.Wait()

	var rtpErr error
	if c.rtpConn != nil {
		rtpErr = c.rtpConn.Close()
		c.rtpConn = nil
	}

	close(c.rtcpDone)
	c.rtcpDoneWG.Wait()

	var rtcpErr error
	if c.rtcpConn != nil {
		rtcpErr = c.rtcpConn.Close()
		c.rtcpConn = nil
	}

	return errors.Join(rtpErr, rtcpErr)
}

func (c *udpConnection) readRTP() {
	defer c.rtpDoneWG.Done()
	buf := make([]byte, 1500) // typical MTU size

	for {
		select {
		case <-c.rtpDone:
			return
		default:
		}

		n, _, err := c.rtpConn.ReadFromUDP(buf)
		if err != nil {
			c.onRTPError(err)
			return
		}

		var pkt rtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			c.onRTPError(err)
			continue
		}

		c.onRTPPackage(&pkt)
	}
}

func (c *udpConnection) readRTCP() {
	defer c.rtcpDoneWG.Done()
	buf := make([]byte, 1500) // typical MTU size

	for {
		select {
		case <-c.rtcpDone:
			return
		default:
		}

		n, _, err := c.rtcpConn.ReadFromUDP(buf)
		if err != nil {
			c.onRTPError(err)
			return
		}

		pkts, err := rtcp.Unmarshal(buf[:n])
		if err != nil {
			c.onRTPError(err)
			continue
		}

		for _, pkt := range pkts {
			c.onRTCPPackage(&pkt)
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
