package rtsp

type method string

const (
	MethodDescribe method = "DESCRIBE"
	MethodOptions  method = "OPTIONS"
	MethodPlay     method = "PLAY"
	MethodSetup    method = "SETUP"
	MethodTeardown method = "TEARDOWN"
)

const (
	ContentTypeSDP = "application/sdp"

	HeaderAccept          = "Accept"
	HeaderAuthorization   = "Authorization"
	HeaderCSeq            = "CSeq"
	HeaderContentLength   = "Content-Length"
	HeaderPublic          = "Public"
	HeaderSession         = "Session"
	HeaderTransport       = "Transport"
	HeaderWWWAuthenticate = "WWW-Authenticate"

	StatusOk           = 200
	StatusUnauthorized = 401
)
