package rtsp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"net"
	"net/url"
	"strconv"
	"strings"
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
	ErrMalformedResponse  = errors.New("malformed Response")
	ErrMalformedRequest   = errors.New("malformed request")
	ErrRequestFailed      = errors.New("request failed")
	ErrInvalidClientState = errors.New("incorrect client state")
)

type ClientState string

const (
	ClientStateInit    ClientState = "INIT"
	ClientStateReady   ClientState = "READY"
	ClientStatePlaying ClientState = "PLAYING"
)

type transportType int

const (
	transportTCP transportType = iota
	transportUDP
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

func WithControlMiddleware(m func(conn ControlConn) ControlConn) ClientOption {
	return func(c *ClientConfig) {
		c.ControlMiddlewares = append(c.ControlMiddlewares, m)
	}
}

type ClientConfig struct {
	Transport TransportMode

	ControlMiddlewares []func(conn ControlConn) ControlConn
}

type Client struct {
	commandCh chan func()

	state ClientState
	path  string

	controlConn ControlConn
	mediaConn   MediaConn

	session string
	cfg     ClientConfig

	onRTPPacket func(media *sdp.MediaDescription, pkt *rtp.Packet)
	onRTPError  func(err error)
}

func NewClient(url *url.URL) (*Client, error) {
	return NewClientWithOptions(url)
}

func NewClientWithOptions(url *url.URL, opts ...ClientOption) (*Client, error) {
	cfg := ClientConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	return newClientWithConn(url, newTcpConnection(url.Host), cfg), nil
}

func newClientWithConn(url *url.URL, conn conn, cfg ClientConfig) *Client {
	c := &Client{
		commandCh: make(chan func(), 3),

		state:       ClientStateInit,
		path:        fmt.Sprintf("%s://%s%s", url.Scheme, url.Host, url.Path),
		controlConn: newControlConn(conn, url.User, cfg.ControlMiddlewares),
		mediaConn:   newMediaConn(url, conn, cfg),

		onRTPPacket: func(media *sdp.MediaDescription, pkt *rtp.Packet) {},
		onRTPError:  func(err error) {},
	}

	go c.run()

	return c
}

func (c *Client) Session() string {
	return c.session
}

func (c *Client) State() ClientState {
	return c.state
}

func (c *Client) OnRTPPacket(f func(media *sdp.MediaDescription, pkt *rtp.Packet)) {
	c.onRTPPacket = f
}

func (c *Client) OnRTPError(f func(err error)) {
	c.onRTPError = f
}

func (c *Client) Options(ctx context.Context) ([]string, error) {
	resCh := make(chan optionsResponse)

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
	resCh := make(chan describeResponse)

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
	resCh := make(chan error)

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

		onRTPPacket := func(pkt *rtp.Packet) {
			c.onRTPPacket(media, pkt)
		}

		onRTPError := func(err error) {
			c.onRTPError(err)
		}

		transport, err := c.mediaConn.OpenMedia(ctx, media.MediaName.String(), onRTPPacket, onRTPError)
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
	resCh := make(chan error)
	c.commandCh <- func() {
		newState, ok := c.transitionAllowed(methodPlay)
		if !ok {
			resCh <- ErrInvalidClientState
			return
		}

		err := c.ensureControlConnReady(ctx)
		if err != nil {
			resCh <- err
			return
		}

		res, err := c.controlConn.DoCall(ctx, string(methodPlay), c.path, map[string]string{
			HeaderSession: c.session,
		})

		if err != nil {
			resCh <- err
			return
		}

		if res.StatusCode != StatusOk {
			resCh <- fmt.Errorf("%w: %d", ErrRequestFailed, res.StatusCode)
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

func (c *Client) Teardown(ctx context.Context) error {
	errCh := make(chan error)

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

		if err := c.controlConn.Close(); !errors.Is(err, ErrConnectionClosed) {
			errCh <- err
		} else {
			errCh <- nil
		}
	}

	select {
	case res := <-errCh:
		return res
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) ensureControlConnReady(ctx context.Context) error {
	if err := c.controlConn.Open(ctx); !errors.Is(err, ErrConnectionOpened) {
		return err
	}

	return nil
}

func (c *Client) transitionAllowed(method method) (ClientState, bool) {
	if row, ok := clientStateTransitions[c.state]; ok {
		newState, ok := row[method]
		return newState, ok
	}

	return ClientStateInit, false
}

func (c *Client) run() {
	for {
		select {
		case cmd := <-c.commandCh:
			cmd()
		}
	}
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
	OpenMedia(ctx context.Context, mediaType string, onRTPPackage func(pkt *rtp.Packet), onRTPError func(err error)) (transport string, err error)
	Close() error
}

func newMediaConn(url *url.URL, conn conn, cfg ClientConfig) MediaConn {
	if cfg.Transport == TransportModeTCP {
		return conn
	}

	return newUdpPull(net.ParseIP(url.Host))
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
