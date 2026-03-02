package rtsp

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var (
	MalformedDigestHeader = errors.New("malformed digest header")
)

// Digest RFC 2069 implementation
type Digest struct {
	Realm string
	Nonce string
}

func ParseDigest(header string) (Digest, error) {
	if !strings.HasPrefix(header, "Digest ") {
		return Digest{}, fmt.Errorf("%w: %s", MalformedDigestHeader, header)
	}

	trimmed := strings.TrimPrefix(header, "Digest ")
	params, err := parseKeyValue(trimmed)
	if err != nil {
		return Digest{}, fmt.Errorf("%w: %w", MalformedDigestHeader, err)
	}

	return Digest{
		Realm: params["realm"],
		Nonce: params["nonce"],
	}, nil
}

func (d Digest) BuildHeader(username string, password string, method string, uri string) string {
	ha1 := md5Hex(username + ":" + d.Realm + ":" + password)
	ha2 := md5Hex(method + ":" + uri)

	response := md5Hex(ha1 + ":" + d.Nonce + ":" + ha2)

	return fmt.Sprintf(
		`Digest username="%s", realm="%s", nonce="%s", uri="%s", response="%s"`,
		username,
		d.Realm,
		d.Nonce,
		uri,
		response,
	)
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func unescapeQuoted(s string) string {
	s = strings.ReplaceAll(s, `\"`, `"`)
	s = strings.ReplaceAll(s, `\\`, `\`)
	return s
}

func parseKeyValue(input string) (map[string]string, error) {
	result := make(map[string]string)

	for {
		input = strings.TrimSpace(input)
		if input == "" {
			return result, nil
		}

		eq := strings.IndexByte(input, '=')
		if eq <= 0 {
			return nil, errors.New("missing or invalid '='")
		}

		key := strings.TrimSpace(input[:eq])
		if !isValidToken(key) {
			return nil, fmt.Errorf("invalid token key %q", key)
		}

		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate key %q", key)
		}

		input = input[eq+1:]
		if input == "" {
			return nil, errors.New("missing value")
		}

		// --- Parse value ---
		var value string

		if input[0] == '"' {
			// Quoted string
			input = input[1:]
			end := -1
			escaped := false

			for i := 0; i < len(input); i++ {
				if input[i] == '\\' && !escaped {
					escaped = true
					continue
				}
				if input[i] == '"' && !escaped {
					end = i
					break
				}
				escaped = false
			}

			if end == -1 {
				return nil, errors.New("unterminated quoted string")
			}

			value = unescapeQuoted(input[:end])
			input = input[end+1:]
		} else {
			// Token value
			comma := strings.IndexByte(input, ',')
			if comma == -1 {
				value = strings.TrimSpace(input)
				input = ""
			} else {
				value = strings.TrimSpace(input[:comma])
				input = input[comma:]
			}

			if !isValidToken(value) {
				return nil, fmt.Errorf("invalid token value %q", value)
			}
		}

		result[key] = value

		input = strings.TrimSpace(input)

		if input == "" {
			return result, nil
		}

		if input[0] != ',' {
			return nil, errors.New("expected ',' separator")
		}

		input = input[1:]
	}
}

func isValidToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) &&
			!unicode.IsDigit(r) &&
			!strings.ContainsRune("!#$%&'*+-.^_`|~", r) {
			return false
		}
	}
	return true
}

func parseDigest(headers map[string]string) (Digest, error) {
	if authorizeHeader, ok := headers[HeaderWWWAuthenticate]; ok {
		return ParseDigest(authorizeHeader)
	}

	return Digest{}, ErrUnauthorized
}
