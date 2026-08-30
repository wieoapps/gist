package gist

import (
	"regexp"
	"testing"
)

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// TestNewTraceID_LooksLikeAV4UUID proves the hand-rolled generator
// produces a well-formed RFC 4122 v4 UUID string (correct version
// nibble, correct variant bits, correct grouping/length) - the format
// callers searching a log line for this value will expect.
func TestNewTraceID_LooksLikeAV4UUID(t *testing.T) {
	id := newTraceID()
	if !uuidV4Pattern.MatchString(id) {
		t.Errorf("newTraceID() = %q, does not look like a v4 UUID", id)
	}
}

// TestNewTraceID_IsActuallyRandom proves distinct calls don't collide -
// the whole point is one value per error, not a constant.
func TestNewTraceID_IsActuallyRandom(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for range 1000 {
		id := newTraceID()
		if seen[id] {
			t.Fatalf("newTraceID() produced a duplicate: %q", id)
		}
		seen[id] = true
	}
}
