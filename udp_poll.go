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
}

func newUdpPull(ip net.IP) *udpPull {
	return &udpPull{
		ip:          ip,
		connections: make(map[string]*udpConnection),
	}
}

func (u *udpPull) OpenMedia(ctx context.Context, mediaType string,
	onRTPPackage func(pkt *rtp.Packet),
	onRTCPPackage func(pkt *rtcp.Packet),
	onRTPError func(err error)) (header string, err error) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	c := newUdpConnection(u.ip)
	c.OnRTPPacket(onRTPPackage)
	c.OnRTCPPacket(onRTCPPackage)
	c.OnRTPError(onRTPError)

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
