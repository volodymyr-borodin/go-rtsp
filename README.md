## go-rtsp

High-level RTSP library for Go with TCP and UDP transport support.

This library implements a focused subset of the RTSP protocol needed to read RTSP streams, with additional capabilities planned. It is intended for experimentation and simpler integrations; if you need a production-ready, fully featured RTSP solution, consider mature projects such as `gortsplib`.

### Features

- **Stateful RTSP client**: `OPTIONS`, `DESCRIBE`, `SETUP`, `PLAY`, `TEARDOWN` with a simple API.
- **Transport selection**: RTSP control over TCP with media over TCP interleaving or UDP.
- **Safe concurrency model**: per-client command loop; public methods are safe for concurrent use.
- **Explicit lifecycle**: `Teardown` ensures connections and goroutines are cleaned up.
- **RTP callbacks**: register handlers for incoming RTP packets and transport-level RTP errors.

### Examples

- **RTSP over TCP**: `examples/client-rtsp-over-tcp`
- **RTSP over UDP**: `examples/client-rtsp-over-udp`

### Support matrix

|                  | Supported                                          | Not supported                                                               |
|------------------|----------------------------------------------------|-----------------------------------------------------------------------------|
| **RTSP methods** | `OPTIONS`, `DESCRIBE`, `SETUP`, `PLAY`, `TEARDOWN` | `PAUSE`, `ANNOUNCE`, `RECORD`, `GET_PARAMETER`, `SET_PARAMETER`, `REDIRECT` |
| **RTSP client**  | TCP, UDP, digest authentication                    | RTCP handling and stats callbacks, automatic reconnection/keep-lives        |
| **RTSP server**  |                                                    | RTSP server implementation                                                  |
