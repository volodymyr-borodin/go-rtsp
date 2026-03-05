package rtsp

type method string

const (
	methodDescribe method = "DESCRIBE"
	methodOptions  method = "OPTIONS"
	methodPlay     method = "PLAY"
	methodSetup    method = "SETUP"
	methodTeardown method = "TEARDOWN"
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
