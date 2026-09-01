package gistmysqlclient

import gistproto "github.com/wieoapps/gist/proto"

type Scend interface {
	toWire() *gistproto.Sort
}

type Asc []string

func (a Asc) toWire() *gistproto.Sort { return &gistproto.Sort{Columns: a} }

type Desc []string

func (d Desc) toWire() *gistproto.Sort { return &gistproto.Sort{Columns: d, Descending: true} }

func toWireSorts(scends []Scend) []*gistproto.Sort {
	out := make([]*gistproto.Sort, len(scends))
	for i, s := range scends {
		out[i] = s.toWire()
	}
	return out
}
