// Package rpcconn holds the raw gistproto-typed gRPC clients gist-sdk's
// built-in service packages (gist-mysql, gist-http-client, ...) call to
// reach gist-server. Its import path puts it off-limits to anything
// outside github.com/wieoapps/gist - a customer's own module can never
// import it - so gistproto never has to appear in gistsdk.Server's exported
// fields/methods for these sibling packages to still reach it.
package rpcconn

import (
	"fmt"
	"sync"

	"github.com/wieoapps/gist-proto"
)

// Clients is the bundle of raw service clients dialed once a *gistsdk.Server
// is up. owner (below) is always that *gistsdk.Server, kept as `any` so this
// package never needs to import gistsdk - that would cycle back, since
// gistsdk itself is what calls Register.
type Clients struct {
	Admin              gistproto.BootstrapServiceClient
	DB                 gistproto.MySQLServiceClient
	PG                 gistproto.PostgresServiceClient
	Elasticsearch      gistproto.ElasticsearchServiceClient
	GoogleCloudStorage gistproto.GoogleCloudStorageServiceClient
	StateMachine       gistproto.StateMachineServiceClient
	HTTPClient         gistproto.HTTPClientServiceClient
	Logging            gistproto.LoggingServiceClient
	RabbitMQ           gistproto.RabbitMQServiceClient
}

var (
	mu       sync.RWMutex
	registry = map[any]*Clients{}
)

// Register associates owner (a *gistsdk.Server) with its dialed clients.
// Also the fake-injection point for unit tests that build a bare
// &gistsdk.Server{} and want it to answer with fakes instead of dialing a
// real gist-server.
func Register(owner any, c *Clients) {
	mu.Lock()
	defer mu.Unlock()
	registry[owner] = c
}

// Unregister removes owner's entry - called from Server.Stop so a
// long-running process that creates many short-lived Servers (mainly
// tests) doesn't leak registry entries for the life of the process.
func Unregister(owner any) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, owner)
}

// For looks up the clients registered for owner, or nil if none were.
func For(owner any) *Clients {
	mu.RLock()
	defer mu.RUnlock()
	return registry[owner]
}

// MustFor is For, but panics with a clear message instead of returning nil -
// for the common call sites where a nil result only ever means a
// programming error (a Server used before Start finished, or a test's
// &gistsdk.Server{} that never called Register).
func MustFor(owner any) *Clients {
	c := For(owner)
	if c == nil {
		panic(fmt.Sprintf("rpcconn: no clients registered for %v - Server not started, or its fake clients not registered via rpcconn.Register", owner))
	}
	return c
}
