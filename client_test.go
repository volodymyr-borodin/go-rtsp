package rtsp

import (
	"context"
	"errors"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"

	"net/url"
	"reflect"
	"slices"
	"testing"
	"time"
)

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
			name: "passes successfully",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: Response{
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
			name: "status not ok",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: Response{
						StatusCode: 400,
					},
				},
			}),

			expectedError: ErrRequestFailed,
		},
		{
			name: "no public header",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: Response{
						StatusCode: 200,
						Headers:    map[string]string{},
					},
				},
			}),

			expectedError: ErrMalformedResponse,
		},
		{
			name: "request stuck",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "OPTIONS",
					url:    "*",
					headers: map[string]string{
						HeaderCSeq: "0",
					},
					resp: Response{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, tt.conn, ClientConfig{
				RtpTransportBuilder: func(t transportType) rtpTransport {
					return tt.conn
				},
			})

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
			name: "passes successfully",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
			name: "unauthorized retried successfully",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
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
			name: "returns bad request",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
						StatusCode: 400,
						Headers:    make(map[string]string),
					},
				},
			}),

			expectedMedia: []*sdp.MediaDescription{},
			expectedError: ErrRequestFailed,
		},
		{
			name: "no media formats provided",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP\n"),
					},
				},
			}),

			expectedMedia: []*sdp.MediaDescription{},
			expectedError: ErrMalformedResponse,
		},
		{
			name: "malformed format",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
						StatusCode: 200,
						Headers:    make(map[string]string),
						Body:       []byte("v=0\no=- 0 0 IN IP4 127.0.0.1\ns=-\nt=0 0\nm=video 0 RTP/AVP NOT_A_NUMBER\na=rtpmap:96 H264/90000\n"),
					},
				},
			}),

			expectedMedia: []*sdp.MediaDescription{},
			expectedError: ErrMalformedResponse,
		},
		{
			name: "non SDP response body",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
			name: "request stuck",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, tt.conn, ClientConfig{
				RtpTransportBuilder: func(t transportType) rtpTransport {
					return tt.conn
				},
			})

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
			name: "passes successfully",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
						Body: nil,
					},
				},
			}),

			expectedSession: "Session1",
			expectedError:   nil,
		},
		{
			name: "passes successfully, timeout removed from session",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1; timeout=15",
						},
						Body: nil,
					},
				},
			}),

			expectedSession: "Session1",
			expectedError:   nil,
		},
		{
			name: "bad request",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
						StatusCode: 400,
						Headers:    map[string]string{},
						Body:       nil,
					},
				},
			}),

			expectedError: ErrRequestFailed,
		},
		{
			name: "request stuck",

			url: "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.url)
			c := newClientWithConn(u, tt.conn, ClientConfig{
				RtpTransportBuilder: func(t transportType) rtpTransport {
					return tt.conn
				},
			})
			s, _ := c.Describe(context.Background())

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			err := c.Setup(ctx, s.MediaDescriptions[0])

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
			name: "ready -> playing",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
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
					resp: Response{
						StatusCode: 200,
					},
				},
			}),

			expectedState: ClientStateReady,
			expectedError: nil,
		},
		{
			name: "ready -> playing, bad request",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
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
					resp: Response{
						StatusCode: 400,
					},
				},
			}),

			expectedState: ClientStateReady,
			expectedError: ErrRequestFailed,
		},
		{
			name: "init -> playing",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "PLAY",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "0",
						HeaderSession: "Session1",
					},
					resp: Response{
						StatusCode: 400,
					},
				},
			}),

			expectedState: ClientStateInit,
			expectedError: ErrInvalidClientState,
		},
		{
			name: "request stuck",

			url: "rtsp://127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
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
					resp: Response{
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
			c := newClientWithConn(u, tt.conn, ClientConfig{
				RtpTransportBuilder: func(t transportType) rtpTransport {
					return tt.conn
				},
			})
			s, _ := c.Describe(context.Background())
			if len(s.MediaDescriptions) != 0 {
				_ = c.Setup(context.Background(), s.MediaDescriptions[0])
			}

			ctx := context.Background()
			if tt.timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			err := c.Play(ctx)
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

func TestClientTeardown(t *testing.T) {
	tests := []struct {
		name string

		url     string
		conn    *mockTransport
		timeout time.Duration

		expectedError error
	}{
		{
			name: "ready -> init",
			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
						Body: nil,
					},
				},
				{
					method: "TEARDOWN",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					resp: Response{
						StatusCode: 200,
						Headers:    map[string]string{},
						Body:       nil,
					},
				},
			}),

			expectedError: nil,
		},
		{
			name: "init -> init",
			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
						StatusCode: 200,
					},
				},
			}),

			expectedError: nil,
		},
		{
			name: "request stuck",
			url:  "rtsp://u1:p1@127.0.0.1:554/stream1",
			conn: newMockTransport([]transportSequence{
				{
					method: "DESCRIBE",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:   "0",
						HeaderAccept: ContentTypeSDP,
					},
					resp: Response{
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
					resp: Response{
						StatusCode: 200,
						Headers: map[string]string{
							HeaderSession: "Session1",
						},
						Body: nil,
					},
				},
				{
					method: "TEARDOWN",
					url:    "rtsp://127.0.0.1:554/stream1",
					headers: map[string]string{
						HeaderCSeq:    "2",
						HeaderSession: "Session1",
					},
					resp: Response{
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
			c := newClientWithConn(u, tt.conn, ClientConfig{
				RtpTransportBuilder: func(t transportType) rtpTransport {
					return tt.conn
				},
			})
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
	sequence []transportSequence
}

func newMockTransport(sequence []transportSequence) *mockTransport {
	return &mockTransport{
		sequence: sequence,
	}
}

type transportSequence struct {
	method  string
	url     string
	headers map[string]string

	resp Response

	delay time.Duration
}

func (m mockTransport) OnRTPPacket(f func(pkt *rtp.Packet)) {

}

func (m mockTransport) DoCall(ctx context.Context, method string, url string, headers map[string]string) (Response, error) {
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
			time.Sleep(seq.delay)
		}

		return seq.resp, nil
	}

	return Response{}, errors.New("not implemented")
}

func (m mockTransport) Close() error {
	return nil
}
