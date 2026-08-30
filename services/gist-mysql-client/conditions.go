package gistmysqlclient

import (
	"encoding/json"

	"github.com/wieoapps/gist-proto"
)

type Operator string

const (
	Equal              Operator = "eq"
	NotEqual           Operator = "neq"
	GreaterThan        Operator = "gt"
	GreaterThanOrEqual Operator = "gte"
	LessThan           Operator = "lt"
	LessThanOrEqual    Operator = "lte"
	IsNullOperator     Operator = "is_null"
	IsNotNullOperator  Operator = "is_not_null"
	InOperator         Operator = "in"
	NotInOperator      Operator = "not_in"
	BeginsWith         Operator = "begins_with"
	EndsWith           Operator = "ends_with"
	Contains           Operator = "contains"

	BeginsWithInsensitive Operator = "begins_with_ci"
	EndsWithInsensitive   Operator = "ends_with_ci"
	ContainsInsensitive   Operator = "contains_ci"

	Between    Operator = "between"
	NotBetween Operator = "not_between"
)

type Conditioner interface {
	toWire() *gistproto.Condition
}

type condition struct {
	field    string
	operator Operator
	value    any
}

func (c condition) toWire() *gistproto.Condition {
	valueJSON, _ := json.Marshal(c.value)
	return &gistproto.Condition{Leaf: &gistproto.ConditionLeaf{Field: c.field, Operator: string(c.operator), ValueJson: valueJSON}}
}

func NewCondition(field string, operator Operator, value any) Conditioner {
	return condition{field: field, operator: operator, value: value}
}

func NewBetweenCondition(field string, low, high any) Conditioner {
	return condition{field: field, operator: Between, value: [2]any{low, high}}
}

func NewNotBetweenCondition(field string, low, high any) Conditioner {
	return condition{field: field, operator: NotBetween, value: [2]any{low, high}}
}

type conditionGroup struct {
	conds []Conditioner
	or    bool
}

func (g conditionGroup) toWire() *gistproto.Condition {
	children := make([]*gistproto.Condition, len(g.conds))
	for i, c := range g.conds {
		children[i] = c.toWire()
	}
	if g.or {
		return &gistproto.Condition{OrGroup: &gistproto.ConditionGroup{Conditions: children}}
	}
	return &gistproto.Condition{AndGroup: &gistproto.ConditionGroup{Conditions: children}}
}

func AndConditions(cs ...Conditioner) Conditioner { return conditionGroup{conds: cs} }
func OrConditions(cs ...Conditioner) Conditioner  { return conditionGroup{conds: cs, or: true} }

func toWireConditions(cs []Conditioner) []*gistproto.Condition {
	out := make([]*gistproto.Condition, len(cs))
	for i, c := range cs {
		out[i] = c.toWire()
	}
	return out
}

func toWireRelationConditions(m map[string][]Conditioner) map[string]*gistproto.ConditionGroup {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*gistproto.ConditionGroup, len(m))
	for relation, cs := range m {
		out[relation] = &gistproto.ConditionGroup{Conditions: toWireConditions(cs)}
	}
	return out
}
