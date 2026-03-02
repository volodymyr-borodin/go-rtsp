package rtsp

import (
	"errors"
	"fmt"
	"testing"
)

func TestParseDigest(t *testing.T) {
	tests := []struct {
		name   string
		header string

		expected      Digest
		expectedError error
	}{
		{
			name:          "valid",
			header:        `Digest realm="testrealm", nonce="abc123"`,
			expected:      Digest{Realm: "testrealm", Nonce: "abc123"},
			expectedError: nil,
		},
		{
			name:          "missing nonce",
			header:        `Digest realm="testrealm"`,
			expected:      Digest{Realm: "testrealm"},
			expectedError: nil,
		},
		{
			name:          "invalid format",
			header:        `Digest realm="bad`,
			expected:      Digest{},
			expectedError: MalformedDigestHeader,
		},
		{
			name:          "no prefix",
			header:        `realm="testrealm", nonce="abc123"`,
			expected:      Digest{},
			expectedError: MalformedDigestHeader,
		},
		{
			name:   "extra spaces",
			header: `Digest   realm="testrealm"  ,   nonce="abc123"   `,
			expected: Digest{
				Realm: "testrealm",
				Nonce: "abc123",
			},
		},
		{
			name:   "token values",
			header: `Digest realm=testrealm, nonce=abc123`,
			expected: Digest{
				Realm: "testrealm",
				Nonce: "abc123",
			},
		},
		{
			name:   "escaped quote",
			header: `Digest realm="test\"realm", nonce="abc123"`,
			expected: Digest{
				Realm: `test"realm`,
				Nonce: "abc123",
			},
		},
		{
			name:          "duplicate key",
			header:        `Digest realm="a", realm="b", nonce="abc"`,
			expectedError: MalformedDigestHeader,
		},
		{
			name:          "missing value",
			header:        `Digest realm=, nonce="abc"`,
			expectedError: MalformedDigestHeader,
		},
		{
			name:          "missing equals",
			header:        `Digest realm "abc"`,
			expectedError: MalformedDigestHeader,
		},
		{
			name:          "trailing garbage",
			header:        `Digest realm="a", nonce="b" garbage`,
			expectedError: MalformedDigestHeader,
		},
		{
			name:          "empty key",
			header:        `Digest ="value"`,
			expectedError: MalformedDigestHeader,
		},
		{
			name:          "empty",
			header:        "",
			expectedError: MalformedDigestHeader,
		},
		{
			name:          "only scheme",
			header:        "Digest",
			expectedError: MalformedDigestHeader,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ParseDigest(test.header)

			if test.expectedError != nil {
				if err == nil {
					t.Fatal("expected error but got nil")
				}

				if !errors.Is(err, test.expectedError) {
					t.Fatalf("expected: %v, got: %v", test.expectedError, err)
				}
			} else if err != nil {
				t.Fatalf("no errors expected but got %v", err)
			}

			if test.expected != result {
				t.Fatalf("expected: %v, got: %v", test.expected, result)
			}
		})
	}
}

func TestBuildHeader(t *testing.T) {
	tests := []struct {
		name     string
		digest   Digest
		username string
		password string
		method   string
		uri      string
		expected string
	}{
		{
			name:     "basic test",
			digest:   Digest{Realm: "testrealm@host.com", Nonce: "dcd98b7102dd2f0e8b11d0f600bfb0c093"},
			username: "Mufasa",
			password: "CircleOfLife",
			method:   "GET",
			uri:      "/dir/index.html",
			expected: `Digest username="Mufasa", realm="testrealm@host.com", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", uri="/dir/index.html", response="1949323746fe6a43ef61f9606e7febea"`,
		},
		{
			name:     "different user",
			digest:   Digest{Realm: "example", Nonce: "abc123"},
			username: "Alice",
			password: "secret",
			method:   "POST",
			uri:      "/api/data",
			expected: func() string {
				ha1 := md5Hex("Alice:example:secret")
				ha2 := md5Hex("POST:/api/data")
				resp := md5Hex(ha1 + ":abc123:" + ha2)
				return fmt.Sprintf(`Digest username="Alice", realm="example", nonce="abc123", uri="/api/data", response="%s"`, resp)
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.digest.BuildHeader(tt.username, tt.password, tt.method, tt.uri)
			if got != tt.expected {
				t.Errorf("expected: %s, got: %s", tt.expected, got)
			}
		})
	}
}
