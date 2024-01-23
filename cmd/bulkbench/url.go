package main

import (
	"net/http"
	"net/url"
	"strconv"
)

const (
	urlQuerySequential = "sequential"
	urlQueryBatchCount = "batchcount"
	urlQueryBatchSize  = "batchsize"
	urlQueryCommand    = "command"
)

type urlQuery struct {
	values url.Values
}

func newURLQuery(r *http.Request) *urlQuery {
	return &urlQuery{values: r.URL.Query()}
}

func (q *urlQuery) get(name string) (string, bool) {
	v := q.values.Get(name)
	if v == "" {
		return v, false
	}
	return v, true
}

func (q *urlQuery) getString(name string, defValue string) string {
	s, ok := q.get(name)
	if !ok {
		return defValue
	}
	return s
}

func (q *urlQuery) getBool(name string, defValue bool) bool {
	s, ok := q.get(name)
	if !ok {
		return defValue
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return defValue
	}
	return b
}

func (q *urlQuery) getInt(name string, defValue int) int {
	s, ok := q.get(name)
	if !ok {
		return defValue
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return defValue
	}
	return i
}
