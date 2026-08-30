package delete

import "github.com/wieoapps/gist/services/gist-http-client/option"

type Option option.Option

func WithHeader(key string, value string) Option {
	return Option(option.WithHeader(key, value))
}

func WithQueryParams(key string, value string) Option {
	return Option(option.WithQueryParams(key, value))
}

func WithOmitResponseHeaders() Option {
	return Option(option.WithOmitResponseHeaders())
}
