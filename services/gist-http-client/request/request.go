package request

type Request struct {
	Method              string
	Url                 string
	Headers             map[string]string
	QueryParams         map[string]string
	Body                []byte
	OmitResponseHeaders bool
}
