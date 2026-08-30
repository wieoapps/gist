package gistpostgresclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/wieoapps/gist-proto"
)

// fakePGClient implements gistproto.PostgresServiceClient entirely in memory - no
// gRPC, no gist-server, no database. It records the last request each
// method received (so tests can assert the client built the right wire
// message) and returns whatever response/error the test pre-loads,
// letting Find/Save/Update/Delete/etc.'s real reflection/encoding/
// decoding logic be exercised end to end with no external dependency.
type fakePGClient struct {
	beginReq  *gistproto.BeginTransactionRequest
	beginResp *gistproto.BeginTransactionResponse
	beginErr  error

	commitReq  *gistproto.TransactionHandle
	commitResp *gistproto.Ack
	commitErr  error

	rollbackReq  *gistproto.TransactionHandle
	rollbackResp *gistproto.Ack
	rollbackErr  error

	repoReq  *gistproto.RepoRequest   // most recent Repo request
	repoReqs []*gistproto.RepoRequest // every Repo request, in call order - for tests asserting across multiple calls in one transaction
	repoResp *gistproto.RepoResponse
	repoErr  error
}

func (f *fakePGClient) BeginTransaction(_ context.Context, in *gistproto.BeginTransactionRequest, _ ...grpc.CallOption) (*gistproto.BeginTransactionResponse, error) {
	f.beginReq = in
	if f.beginErr != nil {
		return nil, f.beginErr
	}
	if f.beginResp != nil {
		return f.beginResp, nil
	}
	return &gistproto.BeginTransactionResponse{TransactionHandle: "fake-handle"}, nil
}

func (f *fakePGClient) Commit(_ context.Context, in *gistproto.TransactionHandle, _ ...grpc.CallOption) (*gistproto.Ack, error) {
	f.commitReq = in
	if f.commitErr != nil {
		return nil, f.commitErr
	}
	if f.commitResp != nil {
		return f.commitResp, nil
	}
	return &gistproto.Ack{}, nil
}

func (f *fakePGClient) Rollback(_ context.Context, in *gistproto.TransactionHandle, _ ...grpc.CallOption) (*gistproto.Ack, error) {
	f.rollbackReq = in
	if f.rollbackErr != nil {
		return nil, f.rollbackErr
	}
	if f.rollbackResp != nil {
		return f.rollbackResp, nil
	}
	return &gistproto.Ack{}, nil
}

// Repo simulates the real AdminServer.Repo's "fold BeginTransaction into
// the first op" behavior: when in carries Begin (and no End - see below),
// the response gets a TransactionHandle (defaulting to "fake-handle", same
// as BeginTransaction above) unless the test already scripted one - so a
// test that pre-loads repoResp for its own reasons (an error code,
// scripted rows) doesn't also have to remember to set TransactionHandle
// for a fresh transaction's first call to look realistic. When in also
// carries End, no handle is assigned at all - mirroring the real server's
// rule that a transaction closed by the same call that opened it never
// gets a handle worth remembering.
func (f *fakePGClient) Repo(_ context.Context, in *gistproto.RepoRequest, _ ...grpc.CallOption) (*gistproto.RepoResponse, error) {
	f.repoReq = in
	f.repoReqs = append(f.repoReqs, in)
	if f.repoErr != nil {
		return nil, f.repoErr
	}
	resp := f.repoResp
	if resp == nil {
		resp = &gistproto.RepoResponse{}
	}
	if in.GetBegin() != nil && in.GetEnd() == nil && resp.GetTransactionHandle() == "" {
		resp.TransactionHandle = "fake-handle"
	}
	return resp, nil
}
