package rtsp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	MalformedStatusLineError = errors.New("malformed status line")
	FailedToReadBodyError    = errors.New("failed to read body")
)

type response struct {
	StatusCode int
	Headers    headers
	Body       []byte
}

func readRtspResponse(r *bufio.Reader) (response, error) {
	statusLine, err := readStatusLine(r)
	if err != nil {
		return response{}, err
	}

	statusSplit := strings.Split(statusLine, " ")
	if len(statusSplit) < 2 {
		return response{}, fmt.Errorf("%w: %s", MalformedStatusLineError, statusLine)
	}

	if statusSplit[0] != "RTSP/1.0" {
		return response{}, fmt.Errorf("%w: %s", MalformedStatusLineError, statusLine)
	}

	statusCode, err := strconv.Atoi(statusSplit[1])
	if err != nil {
		return response{}, fmt.Errorf("%w: %s", MalformedStatusLineError, statusLine)
	}

	h, err := readHeaders(r)
	if err != nil {
		return response{}, err
	}

	var body []byte
	if cl, ok := h.ContentLength(); ok {
		body, err = readBody(r, cl)
		if cl != len(body) {
			return response{
				StatusCode: statusCode,
				Headers:    h,
			}, fmt.Errorf("%w: expected %d bytes but got %d bytes", FailedToReadBodyError, cl, len(body))
		}

		if err != nil {
			return response{
				StatusCode: statusCode,
				Headers:    h,
			}, fmt.Errorf("%w: %w", FailedToReadBodyError, err)
		}
	}

	return response{
		StatusCode: statusCode,
		Headers:    h,
		Body:       body,
	}, nil
}

func readStatusLine(r *bufio.Reader) (string, error) {
	statusLine, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(statusLine), nil
}

func readHeaders(r *bufio.Reader) (headers, error) {
	h := make(map[string]string)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return h, err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			break
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			h[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	return h, nil
}

func readBody(r *bufio.Reader, length int) ([]byte, error) {
	body := make([]byte, length)
	_, err := io.ReadFull(r, body)
	if err != nil && !errors.Is(err, io.EOF) {
		return body, err
	}

	return body, nil
}

type headers map[string]string

func (h headers) ContentLength() (int, bool) {
	if _, ok := h[HeaderContentLength]; !ok {
		return 0, false
	}

	length, err := strconv.Atoi(h[HeaderContentLength])
	if err != nil {
		return 0, false
	}

	return length, true
}
