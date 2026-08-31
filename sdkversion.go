// sdkversion.go reports this SDK's own resolved version, sent to
// gist-server as part of the connect-time handshake (see dialAdmin in
// app.go) so a version mismatch is caught over the wire - including in
// production, where the customer's go.mod normally isn't shipped at all
// (gist-server's own go.mod-based check, checkSDKVersion, only ever sees
// that in a dev/build environment).
package gist

import "runtime/debug"

// sdkModulePath is this SDK's own module path (see gist/go.mod).
const sdkModulePath = "github.com/wieoapps/gist"

// sdkVersion reads the actual version of github.com/wieoapps/gist the
// calling binary was built against from its own embedded build info
// (debug.ReadBuildInfo) rather than a compile-time constant, so it
// reflects reality even across a replace-driven local build. Returns ""
// - "unknown, don't guess" - whenever that can't be said with
// confidence: build info unavailable (not a module-mode build), this
// module missing from the dependency list (shouldn't happen in
// practice, since the calling binary must import this package to reach
// this code at all, but handled), a "(devel)" pseudo-version, or a local
// replace directive.
func sdkVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, dep := range bi.Deps {
		if dep.Path != sdkModulePath {
			continue
		}
		if dep.Replace != nil {
			return ""
		}
		if dep.Version == "" || dep.Version == "(devel)" {
			return ""
		}
		return dep.Version
	}
	return ""
}
