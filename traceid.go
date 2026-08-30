package gist

import (
	"crypto/rand"
	"fmt"
)

// newTraceID returns a random RFC 4122 v4 UUID string - hand-rolled with
// only crypto/rand rather than pulling in a UUID library, since gist-sdk
// deliberately keeps its dependency footprint minimal (the same
// reasoning that dropped logrus/zap entirely once logging moved
// server-side). Used by callbackServer.Invoke to tag an error response
// whose real message was swallowed (see BubbleUpErrors) and the
// matching server-side log line with the identical value, so a
// customer's own support team can find the exact internal log entry for
// a user's error report by searching for it - nothing else about this
// value is meaningful (not a security token, not unique-per-anything
// else).
func newTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
