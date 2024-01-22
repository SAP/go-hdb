package main

import (
	"html/template"
	"log"
	"net/http"
)

// Database operation URL paths.
const (
	cmdCountRows    = "countRows"
	cmdDeleteRows   = "deleteRows"
	cmdCreateTable  = "createTable"
	cmdDropTable    = "dropTable"
	cmdCreateSchema = "createSchema"
	cmdDropSchema   = "dropSchema"
)

// dbHandler implements the http.Handler interface for database operations.
type dbHandler struct {
	tmpl *template.Template
	dba  *dba
}

// newDBHandler returns a new DBHandler instance.
func newDBHandler(dba *dba) (*dbHandler, error) {
	tmpl, err := template.ParseFS(templateFS, "templates/dbresult.gohtml")
	if err != nil {
		return nil, err
	}
	return &dbHandler{tmpl: tmpl, dba: dba}, nil
}

func (h *dbHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := newURLQuery(r)

	command := q.getString(urlQueryCommand, "")

	result := h.dba.executeCommand(command)

	log.Printf("%s", result)
	h.tmpl.Execute(w, result) //nolint: errcheck
}
