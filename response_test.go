package rtsp

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	"slices"
	"testing"
)

func TestReadRtspResponse(t *testing.T) {
	tests := []struct {
		name string

		buffer []byte

		expectedStatusCode int
		expectedHeaders    map[string]string
		expectedBody       []byte
		expectedError      error
	}{
		{
			name:   "single word status parsed, no body",
			buffer: []byte("RTSP/1.0 200 OK\r\nresh1: resv1\r\n\r\n"),

			expectedStatusCode: 200,
			expectedHeaders:    map[string]string{"reqh1": "resv1"},
			expectedBody:       make([]byte, 0),
		},
		{
			name:   "multiple words status parsed, no body",
			buffer: []byte("RTSP/1.0 400 Bad Request\r\nresh1: resv1\r\n\r\n"),

			expectedStatusCode: 400,
			expectedHeaders:    map[string]string{"reqh1": "resv1"},
			expectedBody:       make([]byte, 0),
		},
		{
			name:   "body parsed successfully",
			buffer: []byte("RTSP/1.0 200 OK\r\nresh1: resv1\r\nContent-Length: 13\r\n\r\nIt is not SDP"),

			expectedStatusCode: 200,
			expectedHeaders:    map[string]string{"reqh1": "resv1", "Content-Length": "13"},
			expectedBody:       []byte("It is not SDP"),
		},
		{
			name:   "status line malformed",
			buffer: []byte("RTSP/1.0200OK\r\nresh1: resv1\r\nContent-Length: 13\r\n\r\nIt is not SDP"),

			expectedStatusCode: 0,
			expectedHeaders:    map[string]string{},
			expectedBody:       make([]byte, 0),
			expectedError:      ErrMalformedStatusLine,
		},
		{
			name:   "unsupported protocol",
			buffer: []byte("HTTP/1.1 200 OK\r\nresh1: resv1\r\n\r\n"),

			expectedStatusCode: 0,
			expectedHeaders:    map[string]string{},
			expectedBody:       make([]byte, 0),
			expectedError:      ErrMalformedStatusLine,
		},
		{
			name:   "unable to parse status code protocol",
			buffer: []byte("RTSP/1.0 OK 200\r\nresh1: resv1\r\n\r\n"),

			expectedStatusCode: 0,
			expectedHeaders:    map[string]string{},
			expectedBody:       make([]byte, 0),
			expectedError:      ErrMalformedStatusLine,
		},
		{
			name:   "content length mismatch",
			buffer: []byte("RTSP/1.0 200 OK\nresh1: resv1\r\nContent-Length: 200\r\n\r\nIt is not SDP"),

			expectedStatusCode: 200,
			expectedHeaders:    map[string]string{"reqh1": "resv1"},
			expectedBody:       make([]byte, 0),
			expectedError:      ErrFailedToReadBody,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := readRtspResponse(bufio.NewReader(bytes.NewBuffer(tt.buffer)))

			if tt.expectedError != nil && !errors.Is(err, tt.expectedError) {
				t.Fatalf("expected error: %v, got: %v", tt.expectedError, err)
			}
			if tt.expectedStatusCode != resp.StatusCode {
				t.Errorf("expected status code: %v, got: %v", tt.expectedStatusCode, resp.StatusCode)
			}

			if reflect.DeepEqual(resp.Headers, tt.expectedHeaders) {
				t.Errorf("expected headers: %v, got: %v", tt.expectedHeaders, resp.Headers)
			}

			if !slices.Equal(resp.Body, tt.expectedBody) {
				t.Errorf("expected body: %v, got: %v", tt.expectedBody, resp.Body)
			}
		})
	}
}
