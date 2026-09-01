package gistfixtures

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/wieoapps/gist"
	gistproto "github.com/wieoapps/gist/proto"
	"github.com/wieoapps/gist/internal/rpcconn"
)

func GenerateFixtures(id string, rows ...any) gist.Option {
	return func(server *gist.Server) error {
		out := tableRows{}
		kinds := columnKinds{}
		for _, row := range rows {
			collect(out, kinds, reflect.ValueOf(row))
		}

		rowsJSON, err := json.Marshal(out)
		if err != nil {
			return fmt.Errorf("gistfixtures: could not encode fixture rows: %w", err)
		}

		wireKinds := make(map[string]*gistproto.ColumnKinds, len(kinds))
		for table, cols := range kinds {
			wireKinds[table] = &gistproto.ColumnKinds{Kinds: cols}
		}

		if _, err := rpcconn.MustFor(server).Admin.GenerateFixtures(context.Background(), &gistproto.GenerateFixturesRequest{
			ServiceId:   id,
			RowsJson:    rowsJSON,
			ColumnKinds: wireKinds,
		}); err != nil {
			return fmt.Errorf("gistfixtures: could not send fixture rows for %q: %w", id, err)
		}
		return nil
	}
}
