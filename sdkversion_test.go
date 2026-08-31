package gist

import "testing"

// sdkVersion reads runtime/debug.ReadBuildInfo(), which reports the
// calling *binary's* own dependency graph - when the test binary being
// built and run is this SDK's own test suite, gist is the main module
// under test, not a dependency of itself, so it won't appear in
// bi.Deps at all. This just confirms the "can't find myself" path
// returns "" cleanly instead of panicking - the "found a real
// dependency version" path is exercised for real by a downstream
// consumer (any project that actually requires github.com/wieoapps/gist),
// not by this module's own test suite.
func TestSdkVersion_DoesNotPanic(t *testing.T) {
	v := sdkVersion()
	t.Logf("sdkVersion() from this module's own test binary: %q", v)
}
