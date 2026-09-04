package enums

import (
	"fmt"
	"slices"
	"strings"
)

// IssueAs is gist-auth's Config.IssueAs value - how the JWT minted on
// successful OAuth completion is returned to the client.
type IssueAs string

const (
	IssueAsCookie IssueAs = "cookie"
	IssueAsBody   IssueAs = "body"
	IssueAsBoth   IssueAs = "both"
)

//go:generate generate-enums

// Auto generated code below, any changes will be lost when the file is regenerated.

var _IssueAsKeys = []string{"cookie", "body", "both"}

func (e IssueAs) String() string {
	return string(e)
}

func (e *IssueAs) JSONSchemaBytes() ([]byte, error) {
	list := `"` + strings.Join(_IssueAsKeys, `","`) + `"`
	return fmt.Appendf(nil, `{"type":"string","enum":[%s]}`, list), nil
}

func (e *IssueAs) IsValidIssueAs() bool {
	return slices.Contains(_IssueAsKeys, e.String())
}
