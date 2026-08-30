package status

import "testing"

func TestCode_String_RoundTripsWithStrToCode(t *testing.T) {
	for str, code := range strToCode {
		if got := code.String(); got != str {
			t.Errorf("code %d: String() = %q, want %q", code, got, str)
		}
	}
}

func TestCode_Error_IsLowercaseHumanReadable(t *testing.T) {
	if got := NotFound.Error(); got != "not found" {
		t.Errorf("expected NotFound.Error() == %q, got %q", "not found", got)
	}
	if got := InvalidArgument.Error(); got != "invalid argument" {
		t.Errorf("expected InvalidArgument.Error() == %q, got %q", "invalid argument", got)
	}
}

func TestCode_Is_MatchesOnlyItself(t *testing.T) {
	if !NotFound.Is(NotFound) {
		t.Error("expected NotFound.Is(NotFound) to be true")
	}
	if NotFound.Is(Internal) {
		t.Error("expected NotFound.Is(Internal) to be false")
	}
}

func TestCode_Status_IsSelfAccessor(t *testing.T) {
	if NotFound.Status() != NotFound {
		t.Errorf("expected Status() to return the receiver unchanged, got %v", NotFound.Status())
	}
}

func TestCode_Values_MatchGRPCConventions(t *testing.T) {
	// These specific integer values are load-bearing (they cross the
	// wire as int32 in gistproto.ExpectedError.Code) - a future edit
	// reordering the const block would silently break every already-
	// deployed customer's error codes without this failing.
	cases := map[Code]int{
		OK: 0, Canceled: 1, Unknown: 2, InvalidArgument: 3, DeadlineExceeded: 4,
		NotFound: 5, AlreadyExists: 6, PermissionDenied: 7, ResourceExhausted: 8,
		FailedPrecondition: 9, Aborted: 10, OutOfRange: 11, Unimplemented: 12,
		Internal: 13, Unavailable: 14, DataLoss: 15, Unauthenticated: 16,
	}
	for code, want := range cases {
		if int(code) != want {
			t.Errorf("expected %s == %d, got %d", code.String(), want, int(code))
		}
	}
}
