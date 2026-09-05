// sdkversion.go reports this SDK's own resolved version, sent to
// app.go) so a version mismatch is caught over the wire - including in
// (gist-server's own go.mod-based check, checkSDKVersion, only ever sees
package gist

import "runtime/debug"

const sdkModulePath = "github.com/wieoapps/gist"

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
