// Package logging is gist-sdk's logging entry point: a customer can
// import this package alone - `import "github.com/wieoapps/gist/logging"`
// - and call logging.Info/Debug/Warn/Error/Panic directly from their own
// code (a trigger function, a background goroutine, anywhere), without
// needing a *gistsdk.Server or any other gist-server-adjacent access.
//
// Every call is forwarded to gist-server over the same admin gRPC
// connection every other built-in service uses - gist-server does the
// actual writing (which backend, how fields get rendered, ...), so
// none of that is implemented, or even knowable, from this package.
// gistsdk.Server.Logger (populated automatically into any servicesGroup
// field of type Logger, see BuildServiceGroup) sends through the exact
// same path - a customer using sg.Logger and a customer calling
// logging.Info directly both end up making the identical RPC.
package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
)

// Logger is gist-sdk's client-side logging interface - see
// gistsdk.Logger, which aliases this exact type; a servicesGroup field
// of this type is populated automatically by BuildServiceGroup.
type Logger interface {
	Debug(msg string, fields map[string]any)
	Info(msg string, fields map[string]any)
	Warn(msg string, fields map[string]any)
	Error(msg string, fields map[string]any)
	Panic(msg string, fields map[string]any)
}

// rpcLogger implements Logger by sending every call to owner's
// gist-server over the admin connection - owner is `any` (in practice
// always a *gistsdk.Server) specifically so this package never needs to
// import gistsdk, which would cycle back (gistsdk.Start is the one
// real caller of both NewLogger and SetOwner below).
type rpcLogger struct{ owner any }

// NewLogger returns a Logger that sends through owner's dialed admin
// connection - gistsdk.Start builds Server.Logger from this; a customer
// never calls it themselves.
func NewLogger(owner any) Logger { return rpcLogger{owner: owner} }

func (l rpcLogger) Debug(msg string, fields map[string]any) { send(l.owner, "debug", msg, fields) }
func (l rpcLogger) Info(msg string, fields map[string]any)  { send(l.owner, "info", msg, fields) }
func (l rpcLogger) Warn(msg string, fields map[string]any)  { send(l.owner, "warn", msg, fields) }
func (l rpcLogger) Error(msg string, fields map[string]any) { send(l.owner, "error", msg, fields) }
func (l rpcLogger) Panic(msg string, fields map[string]any) {
	send(l.owner, "panic", msg, fields)
	panic(msg)
}

// defaultOwner is the *gistsdk.Server the package-level Debug/Info/
// Warn/Error/Panic functions below send through - set once by
// gistsdk.Start via SetOwner, as soon as the admin connection is
// dialed.
var defaultOwner any

// SetOwner records which *gistsdk.Server's dialed connection the
// package-level Debug/Info/Warn/Error/Panic functions should send
// through - called once by gistsdk.Start; a customer never calls this
// themselves.
func SetOwner(owner any) { defaultOwner = owner }

// Debug/Info/Warn/Error/Panic log directly through gist-server - call
// these from anywhere in your own code, e.g.
// logging.Info("order approved", map[string]any{"order_id": id}).
// fields may be nil.
func Debug(msg string, fields map[string]any) { send(defaultOwner, "debug", msg, fields) }
func Info(msg string, fields map[string]any)  { send(defaultOwner, "info", msg, fields) }
func Warn(msg string, fields map[string]any)  { send(defaultOwner, "warn", msg, fields) }
func Error(msg string, fields map[string]any) { send(defaultOwner, "error", msg, fields) }
func Panic(msg string, fields map[string]any) {
	send(defaultOwner, "panic", msg, fields)
	panic(msg)
}

// send makes the actual Log RPC. Never returns an error - a logging
// call failing (gist-server not reachable yet, a transient RPC error)
// must never be something the customer's own code has to handle, so
// any failure - including "owner is nil", the state before
// gistsdk.Start has run, e.g. a package-level var initializer logging
// before main() even calls NewApp - falls back to a plain stderr line
// instead of losing the message.
func send(owner any, level, msg string, fields map[string]any) {
	if owner != nil {
		if clients := rpcconn.For(owner); clients != nil && clients.Logging != nil {
			if fieldsJSON, err := json.Marshal(fields); err == nil {
				if _, err := clients.Logging.Log(context.Background(), &proto.LogRequest{
					Level: level, Msg: msg, FieldsJson: fieldsJSON,
				}); err == nil {
					return
				}
			}
		}
	}
	fmt.Fprintf(os.Stderr, "%s: %s %v\n", level, msg, fields)
}
