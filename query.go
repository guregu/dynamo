package dynamo

import (
	"errors"
)

type Query struct {
	table    Table
	hashKey  interface{}
	rangeKey interface{}

	err error
}

func (table Table) Get(keys ...interface{}) *Query {
	q := &Query{table: table}
	switch len(keys) {
	case 0:
		q.err = errors.New("you must specify at least one key")
	case 1:
		q.hashKey = keys[0]
	case 2:
		q.hashKey = keys[0]
		q.rangeKey = keys[1]
	default:
		q.err = errors.New("too many keys, specify 1 or 2")
	}
	return q
}

func (q *Query) One(out interface{}) error {

}

func (q *Query) All(out interface{}) error {

}

func (q *Query) Count() (int64, error) {

}
