package main

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"runtime"

	"github.com/SAP/go-hdb/driver"
)

type indexTestDef struct {
	Descr          string
	SequentialLink string
	ConcurrentLink string
}

type indexCommandDef struct {
	Command string
	Link    string
}

type indexData struct {
	Flags          []*flag.Flag
	TestDefs       []*indexTestDef
	SchemaName     string
	TableName      string
	SchemaCommands []*indexCommandDef
	TableCommands  []*indexCommandDef
}

// indexHandler implements the http.Handler interface for the html index page.
type indexHandler struct {
	tmpl *template.Template
	data *indexData
}

func newIndexTestDef(batchCount, batchSize int) *indexTestDef {
	return &indexTestDef{
		Descr:          fmt.Sprintf("%d x %d", batchCount, batchSize),
		SequentialLink: fmt.Sprintf("test?sequential=t&batchcount=%d&batchsize=%d", batchCount, batchSize),
		ConcurrentLink: fmt.Sprintf("test?sequential=f&batchcount=%d&batchsize=%d", batchCount, batchSize),
	}
}

// newIndexHandler returns a new IndexHandler instance.
func newIndexHandler(dba *dba) (*indexHandler, error) {
	funcMap := template.FuncMap{
		"gomaxprocs":    func() int { return runtime.GOMAXPROCS(0) },
		"numcpu":        runtime.NumCPU,
		"driverversion": func() string { return driver.DriverVersion },
		"hdbversion":    dba.hdbVersion,
		"goos":          func() string { return runtime.GOOS },
		"goarch":        func() string { return runtime.GOARCH },
	}

	tmpl, err := template.New(tmplIndexName).Funcs(funcMap).ParseFS(templateFS, tmplIndexFile)
	if err != nil {
		return nil, err
	}

	indexTestDefs := []*indexTestDef{}
	for _, prm := range parameters {
		indexTestDefs = append(indexTestDefs, newIndexTestDef(prm.BatchCount, prm.BatchSize))
	}

	tableCommands := []*indexCommandDef{
		{Command: "create", Link: fmt.Sprintf("db?command=%s", cmdCreateTable)},
		{Command: "drop", Link: fmt.Sprintf("db?command=%s", cmdDropTable)},
		{Command: "deleteRows", Link: fmt.Sprintf("db?command=%s", cmdDeleteRows)},
		{Command: "countRows", Link: fmt.Sprintf("db?command=%s", cmdCountRows)},
	}

	schemaCommands := []*indexCommandDef{
		{Command: "create", Link: fmt.Sprintf("db?command=%s", cmdCreateSchema)},
		{Command: "drop", Link: fmt.Sprintf("db?command=%s", cmdDropSchema)},
	}

	data := &indexData{
		Flags:          flags(),
		TestDefs:       indexTestDefs,
		SchemaName:     string(dba.schemaName),
		TableName:      string(dba.tableName),
		SchemaCommands: schemaCommands,
		TableCommands:  tableCommands,
	}
	return &indexHandler{tmpl: tmpl, data: data}, nil
}

func (h *indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.tmpl.Execute(w, h.data) //nolint: errcheck
}
