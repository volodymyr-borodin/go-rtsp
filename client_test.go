package rtsp

import (
	"context"
	"errors"
	"fmt"
	"github.com/pion/rtp"
	"net/url"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/sdp/v3"
)

var errTransport = errors.New("mock error")
var errCall = errors.New("mock error")

func TestClientOptions(t *testing.T) {
	tests := []struct {
		name string

		url     string
		conn    *mockTransport
		timeout time.Duration

		expectedMethods []string
		expectedError   error
	}{
		{
			name: "OK",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderPublic: "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN",
						},
					},
				},
			}),

			expectedMethods: []string{"OPTIONS", "DESCRIBE", "SETUP", "PLAY", "TEARDOWN"},
		},
		{
			name: "OK public header missing",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: response{
						StatusCode: 200,
						Headers:    map[string]string{},
					},
				},
			}),

			expectedError: ErrMalformedResponse,
		},
		{
			name: "BAD REQUEST",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: response{
						StatusCode: 400,
					},
				},
			}),

			expectedError: ErrRequestFailed,
		},
		{
			name: "connection failed",

			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newOpenErrorTransport(),

			expectedError: errTransport,
		},
		{
			name: "call timout",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderPublic: "OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN",
						},
					},

					delay: time.Second,
				},
			}),

			timeout:       100 * time.Millisecond,
			expectedError: context.DeadlineExceeded,
		},
		{
			name: "call failed",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					err: errCall,
				},
			}),

			expectedError: errCall,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
				return tt.conn
			}, ClientConfig{Transport: TransportModeTCP})

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			methods, err := c.Options(ctx)
			if tt.expectedError != nil {
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				if !slices.Equal(methods, tt.expectedMethods) {
					t.Errorf("expected %v, got %v", tt.expectedMethods, methods)
				}
			}
		})
	}
}

func TestClientOptions_OnClosedClient(t *testing.T) {
	u, _ := url.Parse("rtsp://u1:p1@127.0.0.1:554/stream1")
	c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
		return newMockTransport(nil)
	}, ClientConfig{Transport: TransportModeTCP})

	err := c.Close(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	_, err = c.Options(context.Background())

	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected error %v, got %v", ErrClientClosed, err)
	}
}

func TestClientDescribe(t *testing.T) {
	tests := []struct {
		name string

		url     string
		conn    *mockTransport
		timeout time.Duration

		expectedMedia []*sdp.MediaDescription
		expectedError error
	}{
		{
			name: "OK",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=rtpmap:96 H264/90000\n"),
					},
				},
			}),

			expectedMedia: []*sdp.MediaDescription{
				{
					MediaName: sdp.MediaName{
						Media:   "video",
						Formats: []string{"96"},
					},
				},
			},
		},
		{
			name: "BAD REQUEST",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 400,
						Headers:    make(map[string]string),
					},
				},
			}),

			expectedMedia: []*sdp.MediaDescription{},
			expectedError: ErrRequestFailed,
		},
		{
			name: "UNAUTHORIZED retried",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 401,
						Headers: map[string]string{
							HeaderWWWAuthenticate: `Digest realm="testrealm", nonce="abc123"`,
						},
					},
				},
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:          "1",
						HeaderAccept:        ContentTypeSDP,
						HeaderAuthorization: `Digest username="u1", realm="testrealm", nonce="abc123", uri="rtsp://127.0.0.1:554/stream1", response="06bf5e30d0c480cf51129e2a82462f86"`,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=rtpmap:96 H264/90000\n"),
					},
				},
			}),

			expectedMedia: []*sdp.MediaDescription{
				{
					MediaName: sdp.MediaName{
						Media:   "video",
						Formats: []string{"96"},
					},
				},
			},
		},
		{
			name: "SDP corrupted",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("NON_SDP RESPONSE\n"),
					},
				},
			}),

			expectedMedia: []*sdp.MediaDescription{},
			expectedError: ErrMalformedResponse,
		},
		{
			name: "connection failed",

			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newOpenErrorTransport(),

			expectedError: errTransport,
		},
		{
			name: "call timout",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=rtpmap:96 H264/90000\n"),
					},

					delay: time.Second,
				},
			}),

			timeout:       100 * time.Millisecond,
			expectedError: context.DeadlineExceeded,
		},
		{
			name: "call failed",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					err: errCall,
				},
			}),

			expectedError: errCall,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
				return tt.conn
			}, ClientConfig{Transport: TransportModeTCP})

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			s, err := c.Describe(ctx)
			if tt.expectedError != nil {
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				if s.MediaDescriptions[0].MediaName.Media != tt.expectedMedia[0].MediaName.Media {
					t.Errorf("expected media %s, got %s", tt.expectedMedia[0].MediaName.Media, s.MediaDescriptions[0].MediaName.Media)
				}

				if s.MediaDescriptions[0].MediaName.Formats[0] != tt.expectedMedia[0].MediaName.Formats[0] {
					t.Errorf("expected proto %s, got %s", tt.expectedMedia[0].MediaName.Formats[0], s.MediaDescriptions[0].MediaName.Formats[0])
				}

				if c.State() != ClientStateInit {
					t.Errorf("expected State %s, got %s", ClientStateInit, c.State())
				}
			}
		})
	}
}

func TestClientDescribe_OnClosedClient(t *testing.T) {
	u, _ := url.Parse("rtsp://u1:p1@127.0.0.1:554/stream1")
	c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
		return newMockTransport(nil)
	}, ClientConfig{Transport: TransportModeTCP})

	err := c.Close(context.Background())
	if err != nil {
		panic(err)
	}

	_, err = c.Describe(context.Background())

	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected error %v, got %v", ErrClientClosed, err)
	}
}

func TestClientSetup(t *testing.T) {
	tests := []struct {
		name string

		url     string
		conn    *mockTransport
		timeout time.Duration

		expectedSession string
		expectedError   error
	}{
		{
			name: "OK",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
					},
				},
			}),

			expectedSession: "Session1",
		},
		{
			name: "OK session header cleaned up",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1; timeout=15",
						},
					},
				},
			}),

			expectedSession: "Session1",
		},
		{
			name: "OK session header missing",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
					},
				},
			}),

			expectedError: ErrMalformedResponse,
		},
		{
			name: "BAD REQUEST",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 400,
						Headers:    map[string]string{},
						Body:       nil,
					},
				},
			}),

			expectedError: ErrRequestFailed,
		},
		{
			name: "call timeout",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
						Body: nil,
					},

					delay: time.Second,
				},
			}),

			timeout:       100 * time.Millisecond,
			expectedError: context.DeadlineExceeded,
		},
		{
			name: "call failed",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					err: errCall,
				},
			}),

			expectedError: errCall,
		},
		{
			name: "control attribute is missing",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
						Body: nil,
					},
				},
			}),

			expectedError: ErrMalformedRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
				return tt.conn
			}, ClientConfig{Transport: TransportModeTCP})
			s, err := c.Describe(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			err = c.Setup(ctx, s.MediaDescriptions[0])

			if tt.expectedError != nil {
				if !errors.Is(err, tt.expectedError) {
					t.Fatalf("expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				if c.Session() != tt.expectedSession {
					t.Errorf("expected session %s, got %s", tt.expectedSession, c.Session())
				}

				if c.State() != ClientStateReady {
					t.Errorf("expected state %s, got %s", ClientStateReady, c.State())
				}
			}
		})
	}
}

func TestClientSetup_OnClosedClient(t *testing.T) {
	u, _ := url.Parse("rtsp://127.0.0.1:554/stream1")
	c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
		return newMockTransport(nil)
	}, ClientConfig{Transport: TransportModeTCP})

	err := c.Close(context.Background())
	if err != nil {
		panic(err)
	}

	err = c.Setup(context.Background(), nil)

	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected error %v, got %v", ErrClientClosed, err)
	}
}

func TestClientPlay(t *testing.T) {
	tests := []struct {
		name string

		url     string
		conn    *mockTransport
		timeout time.Duration

		expectedState ClientState
		expectedError error
	}{
		{
			name: "OK",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
					},
				},
				{
					method: "PLAY",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					resp: response{
						StatusCode: 200,
					},
				},
			}),

			expectedState: ClientStateReady,
			expectedError: nil,
		},
		{
			name: "BAD REQUEST",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
					},
				},
				{
					method: "PLAY",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					resp: response{
						StatusCode: 400,
					},
				},
			}),

			expectedState: ClientStateReady,
			expectedError: ErrRequestFailed,
		},
		{
			name: "Play from Init state forbidden",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
			}),

			expectedState: ClientStateInit,
			expectedError: ErrInvalidClientState,
		},
		{
			name: "call failed",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
					},
				},
				{
					method: "PLAY",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					err: errCall,
				},
			}),

			expectedError: errCall,
		},
		{
			name: "call timeout",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
					},
				},
				{
					method: "PLAY",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					resp: response{
						StatusCode: 200,
					},

					delay: time.Second,
				},
			}),

			timeout:       100 * time.Millisecond,
			expectedError: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
				return tt.conn
			}, ClientConfig{Transport: TransportModeTCP})

			s, err := c.Describe(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(s.MediaDescriptions) != 0 {
				_ = c.Setup(context.Background(), s.MediaDescriptions[0])
			}

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			err = c.Play(ctx)
			if tt.expectedError != nil {
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				if c.State() != ClientStatePlaying {
					t.Errorf("expected state %s, got %s", ClientStatePlaying, c.State())
				}
			}
		})
	}
}

func TestClientPlay_OnClosedClient(t *testing.T) {
	u, _ := url.Parse("rtsp://127.0.0.1:554/stream1")
	c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
		return newMockTransport(nil)
	}, ClientConfig{Transport: TransportModeTCP})

	err := c.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	err = c.Play(context.Background())
	if !errors.Is(err, ErrClientClosed) {
		t.Errorf("expected error %v, got %v", ErrClientClosed, err)
	}
}

func TestClientTeardown(t *testing.T) {
	tests := []struct {
		name string

		url     string
		conn    *mockTransport
		timeout time.Duration

		expectedError error
	}{
		{
			name: "OK",
			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
					},
				},
				{
					method: "TEARDOWN",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					resp: response{
						StatusCode: 200,
					},
				},
			}),
		},
		{
			name: "OK from Init state",
			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "TEARDOWN",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq: "2",
					},
					resp: response{
						StatusCode: 200,
					},
				},
			}),
		},
		{
			name: "call timout",
			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP 96\na=control:track1\na=rtpmap:96 H264/90000\n"),
					},
				},
				{
					method: "SETUP",
					url:    "rtsp://127.0.0.1:554/stream1/track1",
					headers: map[string]string{
						HeaderCSeq:      "1",
						HeaderTransport: "RTP/AVP/TCP;unicast;interleaved=0-1",
					},
					resp: response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
					},
				},
				{
					method: "TEARDOWN",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					resp: response{
						StatusCode: 200,
						Headers:    map[string]string{},
						Body:       nil,
					},
					delay: time.Second,
				},
			}),

			timeout:       100 * time.Millisecond,
			expectedError: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, func(onRTPPackage func(pkt *rtp.Packet), onRTCPPackage func(pkt *rtcp.Packet), onRTPError func(err error)) conn {
				return tt.conn
			}, ClientConfig{Transport: TransportModeTCP})
			s, _ := c.Describe(context.Background())
			_ = c.Setup(context.Background(), s.MediaDescriptions[0])

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			err := c.Teardown(ctx)

			if tt.expectedError != nil {
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				if c.Session() != "" {
					t.Errorf("expected no session, got %s", c.Session())
				}

				if c.State() != ClientStateInit {
					t.Errorf("expected state %s, got %s", ClientStateInit, c.State())
				}
			}
		})
	}
}

type mockTransport struct {
	openError error

	sequence []transportSequence
	opened   bool
	openedMu sync.Mutex

	mediaChannel map[string]int
}

func newMockTransport(sequence []transportSequence) *mockTransport {
	return &mockTransport{
		sequence:     sequence,
		mediaChannel: make(map[string]int),
	}
}

func newOpenErrorTransport() *mockTransport {
	return &mockTransport{openError: errTransport}
}

type transportSequence struct {
	method  string
	url     string
	headers map[string]string

	resp response
	err  error

	delay time.Duration
}

func (m *mockTransport) Open(ctx context.Context) error {
	if m.openError != nil {
		return m.openError
	}

	m.openedMu.Lock()
	defer m.openedMu.Unlock()
	if m.opened {
		return nil
	}

	m.opened = true
	return nil
}

func (m *mockTransport) OpenMedia(ctx context.Context, media string) (string, error) {
	m.mediaChannel[media] = len(m.mediaChannel) * 2
	return fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", m.mediaChannel[media], m.mediaChannel[media]+1), nil
}

func (m *mockTransport) DoCall(ctx context.Context, method string, url string, headers map[string]string) (response, error) {
	for _, seq := range m.sequence {
		if seq.method != method {
			continue
		}

		if seq.url != url {
			continue
		}

		if !reflect.DeepEqual(seq.headers, headers) {
			continue
		}

		if seq.delay != 0 {
			select {
			case <-time.After(seq.delay):
			case <-ctx.Done():
				return response{}, ctx.Err()
			}
		}

		if seq.err != nil {
			return response{}, seq.err
		}

		return seq.resp, nil
	}

	return response{}, errors.New("not implemented")
}

func (m *mockTransport) SendRTCP(ctx context.Context, mediaType string, pkt rtcp.Packet) error {
	return nil
}

func (m *mockTransport) Close() error {
	m.openedMu.Lock()
	defer m.openedMu.Unlock()
	if !m.opened {
		return nil
	}

	m.opened = false
	return nil
}
