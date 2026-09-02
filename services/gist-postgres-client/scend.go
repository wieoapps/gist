package gistpostgresclient

import "github.com/wieoapps/gist/proto"

type Scend interface {
	toWire() *proto.Sort
}

type Asc []string

func (a Asc) toWire() *proto.Sort { return &proto.Sort{Columns: a} }

type Desc []string

func (d Desc) toWire() *proto.Sort { return &proto.Sort{Columns: d, Descending: true} }

func toWireSorts(scends []Scend) []*proto.Sort {
	out := make([]*proto.Sort, len(scends))
	for i, s := range scends {
		out[i] = s.toWire()
	}
	return out
}
