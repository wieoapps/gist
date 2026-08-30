package gist

import (
	"github.com/wieoapps/gist/logging"
)

// Logger is gist-sdk's client-side logging interface - see
// gist-sdk/logging's package doc for the full picture (a servicesGroup
// field of this type is populated automatically by BuildServiceGroup;
// gist-sdk/logging's own package-level Debug/Info/Warn/Error/Panic send
// through the identical path without needing a field at all). An alias
// for logging.Logger.
type Logger = logging.Logger
