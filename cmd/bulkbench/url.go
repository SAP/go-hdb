package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

const (
	urlQueryBatchCount = "batchcount"
	urlQueryBatchSize  = "batchsize"
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
