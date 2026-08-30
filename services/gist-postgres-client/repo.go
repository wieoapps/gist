package gistpostgresclient

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/wieoapps/gist-proto"
)

func Find[model any](tr *Transaction, opts ...Option) ([]model, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeOf(*new(model)))

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_FIND
	req.Schema = schema
	req.Options = &gistproto.QueryOptions{
		Conditions:         toWireConditions(r.conditions),
		Sorts:              toWireSorts(r.sorts),
		Limit:              r.limit,
		Offset:             r.offset,
		Lock:               r.lock,
		RelationConditions: toWireRelationConditions(r.relationConditions),
	}

	resp, err := tr.pg().Repo(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("gistpostgres: find: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return nil, fmt.Errorf("gistpostgres: find: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}

	models := make([]model, len(resp.GetRowsJson()))
	for i, rowJSON := range resp.GetRowsJson() {
		if err := json.Unmarshal(rowJSON, &models[i]); err != nil {
			return nil, fmt.Errorf("gistpostgres: find: could not decode row: %w", err)
		}
	}
	return models, nil
}

func FindOne[model any](tr *Transaction, opts ...Option) (*model, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeOf(*new(model)))

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_FIND_ONE
	req.Schema = schema
	req.Options = &gistproto.QueryOptions{
		Conditions:         toWireConditions(r.conditions),
		Sorts:              toWireSorts(r.sorts),
		Lock:               r.lock,
		RelationConditions: toWireRelationConditions(r.relationConditions),
	}

	resp, err := tr.pg().Repo(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("gistpostgres: find one: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return nil, fmt.Errorf("gistpostgres: find one: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	if len(resp.GetRowsJson()) == 0 {
		return nil, nil
	}

	var m model
	if err := json.Unmarshal(resp.GetRowsJson()[0], &m); err != nil {
		return nil, fmt.Errorf("gistpostgres: find one: could not decode row: %w", err)
	}
	return &m, nil
}

func Count[model any](tr *Transaction, opts ...Option) (int64, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeOf(*new(model)))

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_COUNT
	req.Schema = schema
	req.Options = &gistproto.QueryOptions{Conditions: toWireConditions(r.conditions)}

	resp, err := tr.pg().Repo(context.Background(), req)
	if err != nil {
		return 0, fmt.Errorf("gistpostgres: count: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistpostgres: count: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetCount(), nil
}

func Exists[model any](tr *Transaction, opts ...Option) (bool, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeOf(*new(model)))

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_EXISTS
	req.Schema = schema
	req.Options = &gistproto.QueryOptions{Conditions: toWireConditions(r.conditions)}

	resp, err := tr.pg().Repo(context.Background(), req)
	if err != nil {
		return false, fmt.Errorf("gistpostgres: exists: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return false, fmt.Errorf("gistpostgres: exists: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetExists(), nil
}

// FindAutoClose, FindOneAutoClose, CountAutoClose and ExistsAutoClose are
// one-shot equivalents of Find/FindOne/Count/Exists: they run entirely
// against s, folding BeginTransaction and Rollback into the single Repo
// call (via Service.oneShot) instead of the 2-3 RPCs a NewReadTransaction
// + op + Commit/Rollback call sequence costs. Rollback (not Commit) is used
// to close, since a read has nothing to persist either way. Prefer these
// over the Transaction-based API whenever a caller needs exactly one op and
// has no reason to keep a transaction open across several.

func FindAutoClose[model any](s *Service, opts ...Option) ([]model, error) {
	tr := s.oneShot(true, gistproto.EndAction_END_ACTION_ROLLBACK)
	models, err := Find[model](tr, opts...)
	if err == nil && tr.endErr != nil {
		return nil, fmt.Errorf("gistpostgres: find: %w", tr.endErr)
	}
	return models, err
}

func FindOneAutoClose[model any](s *Service, opts ...Option) (*model, error) {
	tr := s.oneShot(true, gistproto.EndAction_END_ACTION_ROLLBACK)
	m, err := FindOne[model](tr, opts...)
	if err == nil && tr.endErr != nil {
		return nil, fmt.Errorf("gistpostgres: find one: %w", tr.endErr)
	}
	return m, err
}

func CountAutoClose[model any](s *Service, opts ...Option) (int64, error) {
	tr := s.oneShot(true, gistproto.EndAction_END_ACTION_ROLLBACK)
	count, err := Count[model](tr, opts...)
	if err == nil && tr.endErr != nil {
		return 0, fmt.Errorf("gistpostgres: count: %w", tr.endErr)
	}
	return count, err
}

func ExistsAutoClose[model any](s *Service, opts ...Option) (bool, error) {
	tr := s.oneShot(true, gistproto.EndAction_END_ACTION_ROLLBACK)
	exists, err := Exists[model](tr, opts...)
	if err == nil && tr.endErr != nil {
		return false, fmt.Errorf("gistpostgres: exists: %w", tr.endErr)
	}
	return exists, err
}
