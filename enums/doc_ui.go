// Package enums holds small string-enum types shared across gist
// services' config structs - ported from plugins/services/*/enums so
// gist-server can use real enum types without depending on the plugins
// monorepo at build time (gist-server's release build runs with
// GOWORK=off precisely to avoid that dependency).
package enums

import (
	"fmt"
	"slices"
	"strings"
)

// DocUi is gist-api-server's Docs.UI value - ported from
// plugins/services/gist_api/enums/doc_ui.go.
type DocUi string

const (
	Swagger   DocUi = "swagger"
	Redoc     DocUi = "redoc"
	Scalar    DocUi = "scalar"
	OpenapiUI DocUi = "openapi-ui"
	None      DocUi = "none"
)

//go:generate generate-enums

// Auto generated code below, any changes will be lost when the file is regenerated.

var _DocUiKeys = []string{"swagger", "redoc", "scalar", "openapi-ui", "none"}

func (e DocUi) String() string {
	return string(e)
}

func (e *DocUi) JSONSchemaBytes() ([]byte, error) {
	list := `"` + strings.Join(_DocUiKeys, `","`) + `"`
	return fmt.Appendf(nil, `{"type":"string","enum":[%s]}`, list), nil
}

func (e *DocUi) IsValidDocUi() bool {
	return slices.Contains(_DocUiKeys, e.String())
}
