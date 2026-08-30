package gistpostgresclient

type repo struct {
	conditions         []Conditioner
	relationConditions map[string][]Conditioner
	sorts              []Scend
	limit              int64
	offset             int64
	lock               bool
}

type Option func(r *repo)

func WithLimit(limit int64) Option {
	return func(r *repo) {
		if limit >= 1 {
			r.limit = limit
		}
	}
}

func WithOffset(offset int64) Option {
	return func(r *repo) {
		if offset >= 1 {
			r.offset = offset
		}
	}
}

func WithRange(start, end int64) Option {
	return func(r *repo) {
		if start >= 1 && end >= start {
			r.limit = start
			r.offset = end
		}
	}
}

func WithConditions(conds ...Conditioner) Option {
	return func(r *repo) { r.conditions = append(r.conditions, conds...) }
}

func WithSorting(scends ...Scend) Option {
	return func(r *repo) { r.sorts = append(r.sorts, scends...) }
}

func WithRelationConditions(relation string, conds ...Conditioner) Option {
	return func(r *repo) {
		if r.relationConditions == nil {
			r.relationConditions = map[string][]Conditioner{}
		}
		r.relationConditions[relation] = append(r.relationConditions[relation], conds...)
	}
}

func WithLock() Option {
	return func(r *repo) { r.lock = true }
}

func newRepo(opts ...Option) repo {
	var r repo
	for _, o := range opts {
		o(&r)
	}
	return r
}
