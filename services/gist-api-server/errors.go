package gistapiserver

import (
	"fmt"

	"github.com/wieoapps/gist/services/gist-api-server/status"
)

type ExpectedError struct {
	status.Code `json:"code"`
	Message     string `json:"message,omitempty"`
}

func (e ExpectedError) WithMessage(message string, values ...any) ExpectedError {
	if len(values) > 0 {
		message = fmt.Sprintf(message, values...)
	}
	return ExpectedError{Code: e.Code, Message: message}
}

func (e ExpectedError) WithoutMessage() ExpectedError {
	return ExpectedError{Code: e.Code}
}

func (e ExpectedError) Error() string {
	return e.Message
}

func (e ExpectedError) StatusCode() int32 {
	return int32(e.Code)
}

func NewExpectedError(code status.Code, message string) ExpectedError {
	return ExpectedError{Code: code, Message: message}
}

var (
	FailedPrecondition = NewExpectedError(status.FailedPrecondition, "bad request (failed precondition)")
	InvalidArgument    = NewExpectedError(status.InvalidArgument, "bad request (invalid argument)")
	OutOfRange         = NewExpectedError(status.OutOfRange, "bad request (out of range)")
	Unauthenticated    = NewExpectedError(status.Unauthenticated, "unauthenticated")
	PermissionDenied   = NewExpectedError(status.PermissionDenied, "forbidden")
	NotFound           = NewExpectedError(status.NotFound, "not found")
	Aborted            = NewExpectedError(status.Aborted, "conflict (aborted)")
	AlreadyExists      = NewExpectedError(status.AlreadyExists, "conflict (already exists)")
	ResourceExhausted  = NewExpectedError(status.ResourceExhausted, "too many requests")
	Canceled           = NewExpectedError(status.Canceled, "client closed request (canceled)")
	DataLoss           = NewExpectedError(status.DataLoss, "internal server error (data loss)")
	Internal           = NewExpectedError(status.Internal, "internal server error (internal)")
	Unknown            = NewExpectedError(status.Unknown, "internal server error (unknown)")
	Unimplemented      = NewExpectedError(status.Unimplemented, "not implemented")
	Unavailable        = NewExpectedError(status.Unavailable, "service unavailable")
	DeadlineExceeded   = NewExpectedError(status.DeadlineExceeded, "gateway timeout")
)
