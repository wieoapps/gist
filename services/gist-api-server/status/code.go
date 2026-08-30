package status

import "strings"

type Code int

const (
	OK                 Code = 0
	Canceled           Code = 1
	Unknown            Code = 2
	InvalidArgument    Code = 3
	DeadlineExceeded   Code = 4
	NotFound           Code = 5
	AlreadyExists      Code = 6
	PermissionDenied   Code = 7
	ResourceExhausted  Code = 8
	FailedPrecondition Code = 9
	Aborted            Code = 10
	OutOfRange         Code = 11
	Unimplemented      Code = 12
	Internal           Code = 13
	Unavailable        Code = 14
	DataLoss           Code = 15
	Unauthenticated    Code = 16
)

var strToCode = map[string]Code{
	"OK":                  OK,
	"CANCELLED":           Canceled,
	"UNKNOWN":             Unknown,
	"INVALID_ARGUMENT":    InvalidArgument,
	"DEADLINE_EXCEEDED":   DeadlineExceeded,
	"NOT_FOUND":           NotFound,
	"ALREADY_EXISTS":      AlreadyExists,
	"PERMISSION_DENIED":   PermissionDenied,
	"RESOURCE_EXHAUSTED":  ResourceExhausted,
	"FAILED_PRECONDITION": FailedPrecondition,
	"ABORTED":             Aborted,
	"OUT_OF_RANGE":        OutOfRange,
	"UNIMPLEMENTED":       Unimplemented,
	"INTERNAL":            Internal,
	"UNAVAILABLE":         Unavailable,
	"DATA_LOSS":           DataLoss,
	"UNAUTHENTICATED":     Unauthenticated,
}

var codeToStr = func() map[Code]string {
	m := make(map[Code]string, len(strToCode))
	for str, code := range strToCode {
		m[code] = str
	}
	return m
}()

var codeToMsg = func() map[Code]string {
	m := make(map[Code]string, len(strToCode))
	for str, code := range strToCode {
		m[code] = strings.ToLower(strings.ReplaceAll(str, "_", " "))
	}
	return m
}()

func (c Code) String() string {
	return codeToStr[c]
}

func (c Code) Error() string {
	return codeToMsg[c]
}

func (c Code) Is(target error) bool {
	return target == c
}

func (c Code) Status() Code {
	return c
}
