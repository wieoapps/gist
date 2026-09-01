package gistmysqlclient

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	gistproto "github.com/wieoapps/gist/proto"
)

func Save(tr *Transaction, models ...any) (int64, error) {
	if len(models) == 0 {
		return 0, nil
	}
	schema := buildModelSchema(reflect.TypeOf(models[0]))
	rows := make([]*gistproto.ModelRow, len(models))
	for i, m := range models {
		rowJSON, err := json.Marshal(buildModelRow(reflect.ValueOf(m)))
		if err != nil {
			return 0, fmt.Errorf("gistmysql: save: could not encode row %d: %w", i, err)
		}
		rows[i] = &gistproto.ModelRow{RowJson: rowJSON}
	}

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_SAVE
	req.Schema = schema
	req.Rows = rows

	resp, err := tr.db().Repo(context.Background(), req)
	if err != nil {
		return 0, fmt.Errorf("gistmysql: save: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistmysql: save: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetAffected(), nil
}

func SaveWithReturning[model any](tr *Transaction, models ...any) ([]model, int64, error) {
	if len(models) == 0 {
		return nil, 0, nil
	}
	schema := buildModelSchema(reflect.TypeFor[model]())
	rows := make([]*gistproto.ModelRow, len(models))
	for i, m := range models {
		rowJSON, err := json.Marshal(buildModelRow(reflect.ValueOf(m)))
		if err != nil {
			return nil, 0, fmt.Errorf("gistmysql: save with returning: could not encode row %d: %w", i, err)
		}
		rows[i] = &gistproto.ModelRow{RowJson: rowJSON}
	}

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_SAVE_WITH_RETURNING
	req.Schema = schema
	req.Rows = rows

	resp, err := tr.db().Repo(context.Background(), req)
	if err != nil {
		return nil, 0, fmt.Errorf("gistmysql: save with returning: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return nil, 0, fmt.Errorf("gistmysql: save with returning: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	results, err := decodeRows[model](resp.GetRowsJson())
	return results, resp.GetAffected(), err
}

func Update(tr *Transaction, newValues any, opts ...Option) (int64, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeOf(newValues))
	rowJSON, err := json.Marshal(buildModelRow(reflect.ValueOf(newValues)))
	if err != nil {
		return 0, fmt.Errorf("gistmysql: update: could not encode row: %w", err)
	}

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_UPDATE
	req.Schema = schema
	req.Rows = []*gistproto.ModelRow{{RowJson: rowJSON}}
	req.Options = &gistproto.QueryOptions{Conditions: toWireConditions(r.conditions)}

	resp, err := tr.db().Repo(context.Background(), req)
	if err != nil {
		return 0, fmt.Errorf("gistmysql: update: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistmysql: update: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetAffected(), nil
}

func UpdateWithReturning[model any](tr *Transaction, newValues any, opts ...Option) ([]model, int64, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeFor[model]())
	rowJSON, err := json.Marshal(buildModelRow(reflect.ValueOf(newValues)))
	if err != nil {
		return nil, 0, fmt.Errorf("gistmysql: update with returning: could not encode row: %w", err)
	}

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_UPDATE_WITH_RETURNING
	req.Schema = schema
	req.Rows = []*gistproto.ModelRow{{RowJson: rowJSON}}
	req.Options = &gistproto.QueryOptions{Conditions: toWireConditions(r.conditions)}

	resp, err := tr.db().Repo(context.Background(), req)
	if err != nil {
		return nil, 0, fmt.Errorf("gistmysql: update with returning: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return nil, 0, fmt.Errorf("gistmysql: update with returning: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	models, err := decodeRows[model](resp.GetRowsJson())
	return models, resp.GetAffected(), err
}

func Delete[model any](tr *Transaction, opts ...Option) (int64, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeFor[model]())

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_DELETE
	req.Schema = schema
	req.Options = &gistproto.QueryOptions{Conditions: toWireConditions(r.conditions), Limit: r.limit}

	resp, err := tr.db().Repo(context.Background(), req)
	if err != nil {
		return 0, fmt.Errorf("gistmysql: delete: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return 0, fmt.Errorf("gistmysql: delete: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetAffected(), nil
}

func DeleteWithReturning[model any](tr *Transaction, opts ...Option) ([]model, int64, error) {
	r := newRepo(opts...)
	schema := buildModelSchema(reflect.TypeFor[model]())

	req := tr.repoRequest()
	req.Op = gistproto.RepoOp_DELETE_WITH_RETURNING
	req.Schema = schema
	req.Options = &gistproto.QueryOptions{Conditions: toWireConditions(r.conditions), Limit: r.limit}

	resp, err := tr.db().Repo(context.Background(), req)
	if err != nil {
		return nil, 0, fmt.Errorf("gistmysql: delete with returning: %w", err)
	}
	tr.captureHandle(resp)
	if resp.GetErrorCode() != "" {
		return nil, 0, fmt.Errorf("gistmysql: delete with returning: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	models, err := decodeRows[model](resp.GetRowsJson())
	return models, resp.GetAffected(), err
}

// SaveAutoClose, SaveWithReturningAutoClose, UpdateAutoClose,
// UpdateWithReturningAutoClose, DeleteAutoClose and DeleteWithReturningAutoClose
// are one-shot equivalents of their non-suffixed counterparts: they run
// entirely against s, folding BeginTransaction and Commit/Rollback into the
// single Repo call (via Service.oneShot) instead of the 2-3 RPCs a
// NewTransaction + op + Commit call sequence costs. END_ACTION_COMMIT_IF_OK
// commits when the op itself succeeded and rolls back otherwise, decided
// server-side in the same call. Prefer these over the Transaction-based API
// whenever a caller needs exactly one op and has no reason to keep a
// transaction open across several.

func SaveAutoClose(s *Service, models ...any) (int64, error) {
	tr := s.oneShot(false, gistproto.EndAction_END_ACTION_COMMIT_IF_OK)
	affected, err := Save(tr, models...)
	if err == nil && tr.endErr != nil {
		return 0, fmt.Errorf("gistmysql: save: %w", tr.endErr)
	}
	return affected, err
}

func SaveWithReturningAutoClose[model any](s *Service, models ...any) ([]model, int64, error) {
	tr := s.oneShot(false, gistproto.EndAction_END_ACTION_COMMIT_IF_OK)
	results, affected, err := SaveWithReturning[model](tr, models...)
	if err == nil && tr.endErr != nil {
		return nil, 0, fmt.Errorf("gistmysql: save with returning: %w", tr.endErr)
	}
	return results, affected, err
}

func UpdateAutoClose(s *Service, newValues any, opts ...Option) (int64, error) {
	tr := s.oneShot(false, gistproto.EndAction_END_ACTION_COMMIT_IF_OK)
	affected, err := Update(tr, newValues, opts...)
	if err == nil && tr.endErr != nil {
		return 0, fmt.Errorf("gistmysql: update: %w", tr.endErr)
	}
	return affected, err
}

func UpdateWithReturningAutoClose[model any](s *Service, newValues any, opts ...Option) ([]model, int64, error) {
	tr := s.oneShot(false, gistproto.EndAction_END_ACTION_COMMIT_IF_OK)
	models, affected, err := UpdateWithReturning[model](tr, newValues, opts...)
	if err == nil && tr.endErr != nil {
		return nil, 0, fmt.Errorf("gistmysql: update with returning: %w", tr.endErr)
	}
	return models, affected, err
}

func DeleteAutoClose[model any](s *Service, opts ...Option) (int64, error) {
	tr := s.oneShot(false, gistproto.EndAction_END_ACTION_COMMIT_IF_OK)
	affected, err := Delete[model](tr, opts...)
	if err == nil && tr.endErr != nil {
		return 0, fmt.Errorf("gistmysql: delete: %w", tr.endErr)
	}
	return affected, err
}

func DeleteWithReturningAutoClose[model any](s *Service, opts ...Option) ([]model, int64, error) {
	tr := s.oneShot(false, gistproto.EndAction_END_ACTION_COMMIT_IF_OK)
	models, affected, err := DeleteWithReturning[model](tr, opts...)
	if err == nil && tr.endErr != nil {
		return nil, 0, fmt.Errorf("gistmysql: delete with returning: %w", tr.endErr)
	}
	return models, affected, err
}

func decodeRows[model any](rowsJSON [][]byte) ([]model, error) {
	models := make([]model, len(rowsJSON))
	for i, rowJSON := range rowsJSON {
		if err := json.Unmarshal(rowJSON, &models[i]); err != nil {
			return nil, fmt.Errorf("gistmysql: could not decode row: %w", err)
		}
	}
	return models, nil
}
