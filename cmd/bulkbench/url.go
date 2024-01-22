package main

import (
	"fmt"
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

func (q *urlQuery) get(name string) (string, error) {
	v := q.values.Get(name)
	if v == "" {
		return "", fmt.Errorf("url query value %s missing", name)
	}
	return v, nil
}

func (q *urlQuery) getString(name string, defValue string) string {
	s, err := q.get(name)
	if err != nil {
		return defValue
	}
	return s
}

func (q *urlQuery) getBool(name string, defValue bool) bool {
	s, err := q.get(name)
	if err != nil {
		return defValue
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return defValue
	}
	return b
}

func (q *urlQuery) getInt(name string, defValue int) int {
	s, err := q.get(name)
	if err != nil {
		return defValue
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return defValue
	}
	return i
}
