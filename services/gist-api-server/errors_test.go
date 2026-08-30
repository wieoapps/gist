package gistapiserver

import (
	"testing"

	"github.com/wieoapps/gist/services/gist-api-server/status"
)

func TestExpectedError_WithMessage_Formats(t *testing.T) {
	e := NotFound.WithMessage("order %d not found", 42)
	if e.Message != "order 42 not found" {
		t.Errorf("expected formatted message, got %q", e.Message)
	}
	if e.Code != status.NotFound {
		t.Errorf("expected Code to be preserved from the base error, got %v", e.Code)
	}
}

func TestExpectedError_WithMessage_NoValues_LeavesMessageAsIs(t *testing.T) {
	e := NotFound.WithMessage("literal message, no %s placeholders consumed")
	if e.Message != "literal message, no %s placeholders consumed" {
		t.Errorf("expected message left untouched when no values are given, got %q", e.Message)
	}
}

func TestExpectedError_WithoutMessage_ClearsMessage(t *testing.T) {
	e := NotFound.WithMessage("something").WithoutMessage()
	if e.Message != "" {
		t.Errorf("expected WithoutMessage to clear the message, got %q", e.Message)
	}
	if e.Code != status.NotFound {
		t.Errorf("expected Code to survive WithoutMessage, got %v", e.Code)
	}
}

func TestExpectedError_Error_ReturnsMessage(t *testing.T) {
	e := NewExpectedError(status.Internal, "boom")
	if e.Error() != "boom" {
		t.Errorf("expected Error() to return the message, got %q", e.Error())
	}
}

func TestExpectedError_StatusCode_MatchesCode(t *testing.T) {
	e := NewExpectedError(status.PermissionDenied, "nope")
	if e.StatusCode() != int32(status.PermissionDenied) {
		t.Errorf("expected StatusCode() == %d, got %d", status.PermissionDenied, e.StatusCode())
	}
}

func TestExpectedError_Catalog_CodesAreDistinct(t *testing.T) {
	catalog := []ExpectedError{
		FailedPrecondition, InvalidArgument, OutOfRange, Unauthenticated, PermissionDenied,
		NotFound, Aborted, AlreadyExists, ResourceExhausted, Canceled, DataLoss, Internal,
		Unknown, Unimplemented, Unavailable, DeadlineExceeded,
	}
	seen := map[status.Code]bool{}
	for _, e := range catalog {
		if seen[e.Code] {
			t.Errorf("duplicate status code %[1]v (%[1]s) in the predefined catalog", e.Code)
		}
		seen[e.Code] = true
		if e.Message == "" {
			t.Errorf("catalog entry with code %v has an empty default message", e.Code)
		}
	}
}
