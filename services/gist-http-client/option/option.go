package option

import "github.com/wieoapps/gist/services/gist-http-client/request"

type Option func(*request.Request)

func WithHeader(key string, value string) Option {
	return func(r *request.Request) {
		if r.Headers == nil {
			r.Headers = make(map[string]string)
		}
		r.Headers[key] = value
	}
}

func WithQueryParams(key string, value string) Option {
	return func(r *request.Request) {
		if r.QueryParams == nil {
			r.QueryParams = make(map[string]string)
		}
		r.QueryParams[key] = value
	}
}

func WithBody(body []byte) Option {
	return func(r *request.Request) {
		r.Body = body
	}
}

func WithOmitResponseHeaders() Option {
	return func(r *request.Request) {
		r.OmitResponseHeaders = true
	}
}
