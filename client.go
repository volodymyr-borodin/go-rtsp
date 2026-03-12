package rtsp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

var clientStateTransitions = map[ClientState]map[method]ClientState{
	ClientStateInit: {
		methodSetup:    ClientStateReady,
		methodTeardown: ClientStateInit,
	},
	ClientStateReady: {
		methodSetup:    ClientStateReady,
		methodPlay:     ClientStatePlaying,
		methodTeardown: ClientStateInit,
	},
	ClientStatePlaying: {
		methodTeardown: ClientStateInit,
	},
}

var (
	ErrUnauthorized       = errors.New("unauthorized")
	ErrMalformedResponse  = errors.New("malformed response")
	ErrMalformedRequest   = errors.New("malformed request")
	ErrRequestFailed      = errors.New("request failed")
	ErrInvalidClientState = errors.New("incorrect client state")
	ErrClientClosed       = errors.New("client closed")
	ErrClientTeardown     = errors.New("client teardown")
)

type ClientState string

const (
	ClientStateInit    ClientState = "INIT"
	ClientStateReady   ClientState = "READY"
	ClientStatePlaying ClientState = "PLAYING"
)

type TransportMode int

const (
	TransportModeAuto TransportMode = iota
	TransportModeTCP
	TransportModeUDP
)

type ClientOption func(*ClientConfig)

func WithTransport(t TransportMode) ClientOption {
	return func(c *ClientConfig) {
		c.Transport = t
	}
}

type ClientConfig struct {
	Transport TransportMode

	RtpChannelSize  int
	RtcpChannelSize int
	ErrChannelSize  int

	ControlMiddlewares []func(conn ControlConn) ControlConn
}

type Client struct {
	commandCh chan func()
	closed    bool

	path    string
	state   ClientState
	session string
	stateMu sync.RWMutex

	controlConn ControlConn
	mediaConn   MediaConn

	rtpPackets  chan *rtp.Packet
	rtcpPackets chan rtcp.Packet
	err         chan error
}

func NewClient(url *url.URL, opts ...ClientOption) (*Client, error) {
	cfg := ClientConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	return newClientWithConn(url, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt rtcp.Packet), onRTPError func(err error)) conn {
		return newTcpConnection(url.Host, onRTPPackage, onRTCPPackage, onRTPError)
	}, cfg), nil
}

func newClientWithConn(url *url.URL, connBuilder func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt rtcp.Packet), onRTPError func(err error)) conn, cfg ClientConfig) *Client {
	if cfg.RtpChannelSize <= 0 {
		cfg.RtpChannelSize = 1024
	}

	if cfg.RtcpChannelSize <= 0 {
		cfg.RtcpChannelSize = 64
	}

	if cfg.ErrChannelSize <= 0 {
		cfg.ErrChannelSize = 8
	}

	c := &Client{
		commandCh: make(chan func(), 3),

		state: ClientStateInit,
		path:  fmt.Sprintf("%s://%s%s", url.Scheme, url.Host, url.Path),

		rtpPackets:  make(chan *rtp.Packet, cfg.RtpChannelSize),
		rtcpPackets: make(chan rtcp.Packet, cfg.RtcpChannelSize),
		err:         make(chan error, cfg.ErrChannelSize),
	}

	conn := connBuilder(func(pkt *rtp.Packet) {
		c.rtpPackets <- pkt
	}, func(pkt rtcp.Packet) {
		c.rtcpPackets <- pkt
	}, func(err error) {
		c.err <- err
	})
	c.controlConn = newControlConn(conn, url.User, cfg.ControlMiddlewares)

	c.mediaConn = newMediaConn(url, conn, cfg, func(pkt *rtp.Packet) {
		c.rtpPackets <- pkt
	}, func(pkt rtcp.Packet) {
		c.rtcpPackets <- pkt
	}, func(err error) {
		c.err <- err
	})

	go func(c *Client) {
		for cmd := range c.commandCh {
			cmd()
		}
	}(c)

	return c
}

func (c *Client) Session() string {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()

	return c.session
}

func (c *Client) State() ClientState {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()

	return c.state
}

func (c *Client) RTPPackets() <-chan *rtp.Packet {
	return c.rtpPackets
}

func (c *Client) RTCPPackets() <-chan rtcp.Packet {
	return c.rtcpPackets
}

func (c *Client) Errors() <-chan error {
	return c.err
}

func (c *Client) Options(ctx context.Context) ([]string, error) {
	c.stateMu.RLock()
	if c.closed {
		return nil, ErrClientClosed
	}
	c.stateMu.RUnlock()

	resCh := make(chan optionsResponse, 1)
	c.commandCh <- func() {
		err := c.ensureControlConnReady(ctx)
		if err != nil {
			resCh <- optionsResponse{err: err}
			return
		}

		res, err := c.controlConn.DoCall(ctx, string(methodOptions), "*", make(map[string]string))
		if err != nil {
			resCh <- optionsResponse{err: err}
			return
		}

		if res.StatusCode != StatusOk {
			resCh <- optionsResponse{err: fmt.Errorf("%w: %d", ErrRequestFailed, res.StatusCode)}
			return
		}

		if _, ok := res.Headers[HeaderPublic]; !ok {
			resCh <- optionsResponse{err: fmt.Errorf("%w: missing %s header", ErrMalformedResponse, HeaderPublic)}
			return
		}

		methods := make([]string, 0)
		for _, method := range strings.Split(res.Headers[HeaderPublic], ",") {
			methods = append(methods, strings.TrimSpace(method))
		}

		resCh <- optionsResponse{methods: methods}
	}

	select {
	case res := <-resCh:
		return res.methods, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) Describe(ctx context.Context) (sdp.SessionDescription, error) {
	c.stateMu.RLock()
	if c.closed {
		return sdp.SessionDescription{}, ErrClientClosed
	}
	c.stateMu.RUnlock()

	resCh := make(chan describeResponse, 1)
	c.commandCh <- func() {
		err := c.ensureControlConnReady(ctx)
		if err != nil {
			resCh <- describeResponse{err: err}
			return
		}

		s := sdp.SessionDescription{}
		r, err := c.controlConn.DoCall(ctx, string(methodDescribe), c.path, map[string]string{
			HeaderAccept: ContentTypeSDP,
		})

		if err != nil {
			resCh <- describeResponse{err: err}
			return
		}

		if r.StatusCode != StatusOk {
			resCh <- describeResponse{err: fmt.Errorf("%w: %d", ErrRequestFailed, r.StatusCode)}
			return
		}

		r.Body = sanitizeSDPOrigin(r.Body)
		if err := s.Unmarshal(r.Body); err != nil {
			resCh <- describeResponse{err: fmt.Errorf("%w: %s", ErrMalformedResponse, err)}
			return
		}

		resCh <- describeResponse{sdp: s}
	}

	select {
	case res := <-resCh:
		return res.sdp, res.err
	case <-ctx.Done():
		return sdp.SessionDescription{}, ctx.Err()
	}
}

func (c *Client) Setup(ctx context.Context, media *sdp.MediaDescription) error {
	c.stateMu.RLock()
	if c.closed {
		return ErrClientClosed
	}
	c.stateMu.RUnlock()

	resCh := make(chan error, 1)
	c.commandCh <- func() {
		newState, ok := c.transitionAllowed(methodSetup)
		if !ok {
			resCh <- ErrInvalidClientState
			return
		}

		err := c.ensureControlConnReady(ctx)
		if err != nil {
			resCh <- err
			return
		}

		control, ok := media.Attribute("control")
		if !ok {
			resCh <- fmt.Errorf("%w: %s", ErrMalformedRequest, "control attribute missing")
			return
		}

		if !strings.HasPrefix(control, c.path) {
			control, _ = url.JoinPath(c.path, control)
		}

		transport, err := c.mediaConn.OpenMedia(ctx, media.MediaName.String())
		if err != nil {
			resCh <- err
			return
		}

		setupHeaders := map[string]string{
			HeaderTransport: transport,
		}

		if c.session != "" {
			setupHeaders[HeaderSession] = c.session
		}

		res, err := c.controlConn.DoCall(ctx, string(methodSetup), control, setupHeaders)
		if err != nil {
			resCh <- err
			return
		}

		if res.StatusCode != StatusOk {
			resCh <- fmt.Errorf("%w: %d", ErrRequestFailed, res.StatusCode)
			return
		}

		if sessionVal, ok := res.Headers[HeaderSession]; ok {
			if idx := strings.Index(sessionVal, ";"); idx >= 0 {
				c.session = strings.TrimSpace(sessionVal[:idx])
			} else {
				c.session = sessionVal
			}
		} else if c.session == "" {
			resCh <- fmt.Errorf("%w: %s", ErrMalformedResponse, "no session header")
			return
		}

		c.state = newState

		resCh <- nil
	}

	select {
	case res := <-resCh:
		return res
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) Play(ctx context.Context) error {
	c.stateMu.RLock()
	if c.closed {
		return ErrClientClosed
	}
	c.stateMu.RUnlock()

	errCh := make(chan error, 1)
	c.commandCh <- func() {
		newState, ok := c.transitionAllowed(methodPlay)
		if !ok {
			errCh <- ErrInvalidClientState
			return
		}

		err := c.ensureControlConnReady(ctx)
		if err != nil {
			errCh <- err
			return
		}

		res, err := c.controlConn.DoCall(ctx, string(methodPlay), c.path, map[string]string{
			HeaderSession: c.session,
		})

		if err != nil {
			errCh <- err
			return
		}

		if res.StatusCode != StatusOk {
			errCh <- fmt.Errorf("%w: %d", ErrRequestFailed, res.StatusCode)
			return
		}

		c.state = newState
		errCh <- nil
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) Teardown(ctx context.Context) error {
	c.stateMu.RLock()
	if c.closed {
		return ErrClientClosed
	}
	c.stateMu.RUnlock()

	errCh := make(chan error, 1)
	c.commandCh <- func() {
		newState, ok := c.transitionAllowed(methodTeardown)
		if !ok {
			errCh <- ErrInvalidClientState
			return
		}

		if c.state == ClientStateInit {
			errCh <- nil
			return
		}

		_, _ = c.controlConn.DoCall(ctx, string(methodTeardown), c.path, map[string]string{
			HeaderSession: c.session,
		})

		c.session = ""
		c.state = newState

		c.err <- ErrClientTeardown
		errCh <- nil
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) Close(ctx context.Context) error {
	c.stateMu.RLock()
	if c.closed {
		return nil
	}
	c.stateMu.RUnlock()

	errCh := make(chan error, 1)
	c.commandCh <- func() {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()

		if c.state != ClientStateInit {
			c.err <- ErrClientTeardown
		}

		c.closed = true
		c.session = ""
		c.state = ClientStateInit

		controlCloseErr := c.controlConn.Close()
		mediaCloseErr := c.mediaConn.Close()

		close(c.commandCh)
		close(c.rtpPackets)
		close(c.rtcpPackets)
		close(c.err)

		errCh <- errors.Join(controlCloseErr, mediaCloseErr)
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) SendRTCP(ctx context.Context, media *sdp.MediaDescription, pkt rtcp.Packet) error {
	c.stateMu.RLock()
	if c.closed {
		return ErrClientClosed
	}
	c.stateMu.RUnlock()

	errCh := make(chan error, 1)
	c.commandCh <- func() {
		err := c.ensureControlConnReady(ctx)
		if err != nil {
			errCh <- err
		}

		errCh <- c.mediaConn.SendRTCP(ctx, media.MediaName.String(), pkt)
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) ensureControlConnReady(ctx context.Context) error {
	return c.controlConn.Open(ctx)
}

func (c *Client) transitionAllowed(method method) (ClientState, bool) {
	if row, ok := clientStateTransitions[c.state]; ok {
		newState, ok := row[method]
		return newState, ok
	}

	return ClientStateInit, false
}

type describeResponse struct {
	sdp sdp.SessionDescription
	err error
}

type optionsResponse struct {
	methods []string
	err     error
}

// fixSDPOrigin removes extra value returned by tapo (tp-link) camera
func sanitizeSDPOrigin(sdp []byte) []byte {
	lines := bytes.Split(sdp, []byte("\n"))

	for i, line := range lines {
		if bytes.HasPrefix(line, []byte("o=")) {
			fields := strings.Fields(string(line[2:]))

			if len(fields) == 7 {
				fields = append(fields[:3], fields[4:]...)

				fixedLine := "o=" + strings.Join(fields, " ")
				lines[i] = []byte(fixedLine)
			}
		}
	}

	return bytes.Join(lines, []byte("\n"))
}

type conn interface {
	MediaConn
	ControlConn
}

type MediaConn interface {
	OpenMedia(ctx context.Context, mediaType string) (string, error)
	SendRTCP(ctx context.Context, mediaType string, pkt rtcp.Packet) error

	Close() error
}

func newMediaConn(
	url *url.URL,
	conn conn,
	cfg ClientConfig,
	onRTPPackage func(pkt *rtp.Packet),
	onRTCPPackage func(pkt rtcp.Packet),
	onRTPError func(err error)) MediaConn {

	switch cfg.Transport {
	case TransportModeTCP:

		return conn
	case TransportModeAuto, TransportModeUDP:
		fallthrough
	default:
		return newUdpPull(net.ParseIP(url.Host), onRTPPackage, onRTCPPackage, onRTPError)
	}
}

type ControlConn interface {
	Open(ctx context.Context) error
	DoCall(ctx context.Context, method string, url string, headers map[string]string) (response, error)
	Close() error
}

func newControlConn(conn conn, user *url.Userinfo, middlewares []func(conn ControlConn) ControlConn) ControlConn {
	controlConn := conn.(ControlConn)

	controlConn = newSeqMiddleware(controlConn)

	if user != nil {
		controlConn = newAuthMiddleware(user, controlConn)
	}

	for _, c := range middlewares {
		controlConn = c(controlConn)
	}

	return controlConn
}

type cseqMiddleware struct {
	cseq int

	next ControlConn
}

func newSeqMiddleware(next ControlConn) *cseqMiddleware {
	return &cseqMiddleware{
		next: next,
	}
}

func (m *cseqMiddleware) Open(ctx context.Context) error {
	return m.next.Open(ctx)
}

func (m *cseqMiddleware) DoCall(ctx context.Context, method string, url string, headers map[string]string) (response, error) {
	headers[HeaderCSeq] = strconv.Itoa(m.cseq)
	res, err := m.next.DoCall(ctx, method, url, headers)

	m.cseq++

	return res, err
}

func (m *cseqMiddleware) Close() error {
	return m.next.Close()
}
