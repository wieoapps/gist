package enums

import (
	"fmt"
	"slices"
	"strings"
)

// Provider is gist-auth's Config.Provider value - ported from
// plugins/services/gist_auth/enums/provider.go.
type Provider string

const (
	Apple  Provider = "apple"
	Gitea  Provider = "gitea"
	Google Provider = "google"
)

//go:generate generate-enums

// Auto generated code below, any changes will be lost when the file is regenerated.

var _ProviderKeys = []string{"apple", "gitea", "google"}

func (e Provider) String() string {
	return string(e)
}

func (e *Provider) JSONSchemaBytes() ([]byte, error) {
	list := `"` + strings.Join(_ProviderKeys, `","`) + `"`
	return fmt.Appendf(nil, `{"type":"string","enum":[%s]}`, list), nil
}

func (e *Provider) IsValidProvider() bool {
	return slices.Contains(_ProviderKeys, e.String())
}
