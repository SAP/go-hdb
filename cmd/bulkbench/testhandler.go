package main

import (
	"html/template"
	"log"
	"net/http"
)

// testHandler implements the http.Handler interface for the tests.
type testHandler struct {
	tmpl *template.Template
	lt   *loadTest
}

// newTestHandler returns a new TestHandler instance.
func newTestHandler(dba *dba) (*testHandler, error) {
	tmpl, err := template.ParseFS(templateFS, "templates/testresult.gohtml")
	if err != nil {
		return nil, err
	}
	return &testHandler{tmpl: tmpl, lt: newLoadTest(dba)}, nil
}

func (h *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := newURLQuery(r)

	sequential := q.getBool(urlQuerySequential, defSequential)
	batchCount := q.getInt(urlQueryBatchCount, defBatchCount)
	batchSize := q.getInt(urlQueryBatchSize, defBatchSize)

	result := h.lt.test(sequential, batchCount, batchSize, drop)

	log.Printf("%s", result)
	h.tmpl.Execute(w, result) //nolint: errcheck
}
