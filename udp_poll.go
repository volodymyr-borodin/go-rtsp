package rtsp

import (
	"context"
	"errors"
	"fmt"
	"github.com/pion/rtp"
	"net"
	"sync"
)

var (
	MediaTypeAlreadyInitialized = errors.New("media type already initialized")
)

type udpPull struct {
	ip          net.IP
	connections map[int]*udpConnection

	mutex sync.Mutex
}

func newUdpPull(ip net.IP) *udpPull {
	return &udpPull{
		ip:          ip,
		connections: make(map[int]*udpConnection),
	}
}

func (u *udpPull) OpenMedia(mediaType int, ctx context.Context) (header string, err error) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	c := newUdpConnection(u.ip)
	err = c.Open(ctx)
	if err != nil {
		return "", err
	}

	if _, ok := u.connections[mediaType]; ok {
		return "", MediaTypeAlreadyInitialized
	}

	u.connections[mediaType] = c
	return fmt.Sprintf("RTP/AVP;unicast;client_port=%d-%d", c.RTPPort(), c.RTCPPort()), nil
}

func (u *udpPull) OnRTPPacket(f func(pkt *rtp.Packet)) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	for _, c := range u.connections {
		c.OnRTPPacket(f)
	}
}

func (u *udpPull) GetMediaHeader(mediaType int) string {
	c := u.connections[mediaType]

	return fmt.Sprintf("RTP/AVP;unicast;client_port=%d-%d", c.RTCPPort(), c.RTCPPort())
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

	u.connections = make(map[int]*udpConnection)

	return errors.Join(errs...)
}
