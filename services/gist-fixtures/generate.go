package gistfixtures

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/wieoapps/gist"
	"github.com/wieoapps/gist/internal/rpcconn"
	"github.com/wieoapps/gist/proto"
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

		wireKinds := make(map[string]*proto.ColumnKinds, len(kinds))
		for table, cols := range kinds {
			wireKinds[table] = &proto.ColumnKinds{Kinds: cols}
		}

		if _, err := rpcconn.MustFor(server).Admin.GenerateFixtures(context.Background(), &proto.GenerateFixturesRequest{
			ServiceId:   id,
			RowsJson:    rowsJSON,
			ColumnKinds: wireKinds,
		}); err != nil {
			return fmt.Errorf("gistfixtures: could not send fixture rows for %q: %w", id, err)
		}
		return nil
	}
}
