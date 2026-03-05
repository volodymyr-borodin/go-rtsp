package rtsp

import (
	"context"
	"fmt"
	"net/url"
)

type authMiddleware struct {
	userInfo *url.Userinfo
	digest   *Digest

	next ControlConn
}

func newAuthMiddleware(userInfo *url.Userinfo, next ControlConn) *authMiddleware {
	return &authMiddleware{
		userInfo: userInfo,
		next:     next,
	}
}

func (m *authMiddleware) Open(ctx context.Context) error {
	return m.next.Open(ctx)
}

func (m *authMiddleware) DoCall(ctx context.Context, method string, url string, headers map[string]string) (response, error) {
	if m.digest != nil && m.userInfo != nil {
		username := m.userInfo.Username()
		password, _ := m.userInfo.Password()

		headers[HeaderAuthorization] = m.digest.BuildHeader(username, password, method, url)
	}

	res, err := m.next.DoCall(ctx, method, url, headers)
	if err != nil {
		return response{}, err
	}

	if res.StatusCode == StatusUnauthorized && m.userInfo != nil && m.digest == nil {
		digest, parseErr := parseDigest(res.Headers)
		if parseErr != nil {
			return response{}, fmt.Errorf("%w: %w", err, parseErr)
		}

		m.digest = &digest
		return m.DoCall(ctx, method, url, headers)
	}

	return res, err
}

func (m *authMiddleware) Close() error {
	return m.next.Close()
}
