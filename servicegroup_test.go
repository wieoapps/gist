package gist_test

import (
	"testing"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/services/gist-elasticsearch"
	"github.com/wieoapps/gist/services/gist-mysql-client"
)

// These tests exercise the REAL registrations gist-mysql/gist-elasticsearch
// make from their own init() - not a fake registered type - since that's
// exactly what a customer's ServicesGroup relies on once those packages
// are imported.

type oneFieldGroup struct {
	TestDb *gistmysqlclient.Service `name:"test-db"`
}

func TestBuildServiceGroup_ResolvesTaggedField(t *testing.T) {
	server := &gist.Server{}
	sg, err := gist.BuildServiceGroup[oneFieldGroup](server)
	if err != nil {
		t.Fatalf("BuildServiceGroup failed: %v", err)
	}
	if sg.TestDb == nil {
		t.Fatal("expected TestDb to be resolved, got nil")
	}
}

type mixedFieldGroup struct {
	TestDb     *gistmysqlclient.Service   `name:"test-db"`
	TestSearch *gistelasticsearch.Service `name:"test-search"`
	Untagged   *gistmysqlclient.Service
}

func TestBuildServiceGroup_MultipleDistinctTypes_AllResolved(t *testing.T) {
	server := &gist.Server{}
	sg, err := gist.BuildServiceGroup[mixedFieldGroup](server)
	if err != nil {
		t.Fatalf("BuildServiceGroup failed: %v", err)
	}
	if sg.TestDb == nil {
		t.Fatal("expected TestDb to be resolved, got nil")
	}
	if sg.TestSearch == nil {
		t.Fatal("expected Search to be resolved, got nil")
	}
}

func TestBuildServiceGroup_UntaggedField_LeftZeroValue(t *testing.T) {
	server := &gist.Server{}
	sg, err := gist.BuildServiceGroup[mixedFieldGroup](server)
	if err != nil {
		t.Fatalf("BuildServiceGroup failed: %v", err)
	}
	if sg.Untagged != nil {
		t.Fatalf("expected an untagged field to stay nil, got %v", sg.Untagged)
	}
}

type unregisteredTypeGroup struct {
	Thing *struct{} `name:"some-id"`
}

func TestBuildServiceGroup_UnregisteredType_ReturnsDescriptiveError(t *testing.T) {
	server := &gist.Server{}
	_, err := gist.BuildServiceGroup[unregisteredTypeGroup](server)
	if err == nil {
		t.Fatal("expected an error for a name-tagged field with no registered service type")
	}
}

func TestBuildServiceGroup_NonStruct_ReturnsError(t *testing.T) {
	server := &gist.Server{}
	if _, err := gist.BuildServiceGroup[int](server); err == nil {
		t.Fatal("expected an error when T is not a struct")
	}
}

// loggerGroup has no `name` tag on Logger - there's exactly one per
// server, unlike DB/Search/etc., which are per-config-id instances a tag
// picks out. This is the split-architecture's replacement for the old
// monolith's g.ServiceGroup embed, which gave every servicesGroup a
// populated Logger field automatically via Fx.
type loggerGroup struct {
	Logger gist.Logger
	TestDb *gistmysqlclient.Service `name:"test-db"`
}

// fakeLogger is a minimal gistsdk.Logger for proving BuildServiceGroup
// copies whatever Server.Logger holds by interface value - what it
// actually does with a call doesn't matter here (see gist-sdk/logging
// for that).
type fakeLogger struct{}

func (fakeLogger) Debug(msg string, fields map[string]any) {}
func (fakeLogger) Info(msg string, fields map[string]any)  {}
func (fakeLogger) Warn(msg string, fields map[string]any)  {}
func (fakeLogger) Error(msg string, fields map[string]any) {}
func (fakeLogger) Panic(msg string, fields map[string]any) {}

func TestBuildServiceGroup_LoggerField_PopulatedWithoutTag(t *testing.T) {
	wantLogger := fakeLogger{}
	server := &gist.Server{Logger: wantLogger}

	sg, err := gist.BuildServiceGroup[loggerGroup](server)
	if err != nil {
		t.Fatalf("BuildServiceGroup failed: %v", err)
	}
	if sg.Logger != wantLogger {
		t.Fatalf("expected Logger to be populated from the server, got %v", sg.Logger)
	}
	if sg.TestDb == nil {
		t.Fatal("expected the tagged field alongside it to still resolve normally")
	}
}
