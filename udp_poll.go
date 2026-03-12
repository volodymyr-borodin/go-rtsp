package rtsp

import (
	"context"
	"errors"
	"fmt"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"net"
	"sync"
)

var (
	ErrMediaTypeAlreadyInitialized = errors.New("media type already initialized")
)

type udpPull struct {
	ip          net.IP
	connections map[string]*udpConnection

	mutex sync.Mutex

	onRTPPackage  func(pkt *rtp.Packet)
	onRTCPPackage func(pkt rtcp.Packet)
	onRTPError    func(err error)
}

func newUdpPull(ip net.IP, onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt rtcp.Packet), onRTPError func(err error)) *udpPull {
	return &udpPull{
		ip:          ip,
		connections: make(map[string]*udpConnection),

		onRTPPackage:  onRTPPackage,
		onRTCPPackage: onRTCPPackage,
		onRTPError:    onRTPError,
	}
}

func (u *udpPull) OpenMedia(ctx context.Context, mediaType string) (header string, err error) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	c := newUdpConnection(u.ip, u.onRTPPackage, u.onRTCPPackage, u.onRTPError)
	c.OnRTPPacket(u.onRTPPackage)
	c.OnRTCPPacket(u.onRTCPPackage)
	c.OnRTPError(u.onRTPError)

	err = c.Open(ctx)
	if err != nil {
		return "", err
	}

	if _, ok := u.connections[mediaType]; ok {
		return "", ErrMediaTypeAlreadyInitialized
	}

	u.connections[mediaType] = c
	return fmt.Sprintf("RTP/AVP;unicast;client_port=%d-%d", c.RTPPort(), c.RTCPPort()), nil
}

func (u *udpPull) SendRTCP(ctx context.Context, mediaType string, packet rtcp.Packet) error {
	panic("implement me")
}

func (u *udpPull) Close() error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	errs := make([]error, 0)
	for _, c := range u.connections {
		err := c.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	u.connections = make(map[string]*udpConnection)

	return errors.Join(errs...)
}
