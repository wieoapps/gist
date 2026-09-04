package enums

import (
	"fmt"
	"slices"
	"strings"
)

// Subject is gist-auth's Config.Subject value - which goth.User field
// becomes the JWT subject minted on successful OAuth completion.
type Subject string

const (
	SubjectUserID Subject = "user_id"
	SubjectEmail  Subject = "email"
)

//go:generate generate-enums

// Auto generated code below, any changes will be lost when the file is regenerated.

var _SubjectKeys = []string{"user_id", "email"}

func (e Subject) String() string {
	return string(e)
}

func (e *Subject) JSONSchemaBytes() ([]byte, error) {
	list := `"` + strings.Join(_SubjectKeys, `","`) + `"`
	return fmt.Appendf(nil, `{"type":"string","enum":[%s]}`, list), nil
}

func (e *Subject) IsValidSubject() bool {
	return slices.Contains(_SubjectKeys, e.String())
}
