package main

import (
	"flag"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
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
func newIndexHandler(dba *dba, templateFS fs.FS) (*indexHandler, error) {
	funcMap := template.FuncMap{
		"gomaxprocs":    func() int { return runtime.GOMAXPROCS(0) },
		"numcpu":        runtime.NumCPU,
		"driverversion": func() string { return driver.DriverVersion },
		"hdbversion":    dba.hdbVersion,
		"goos":          func() string { return runtime.GOOS },
		"goarch":        func() string { return runtime.GOARCH },
	}

	tmpl, err := template.New(tmplIndex).Funcs(funcMap).ParseFS(templateFS, tmplIndex)
	if err != nil {
		return nil, err
	}

	indexTestDefs := []*indexTestDef{}
	for _, prm := range parameters {
		indexTestDefs = append(indexTestDefs, newIndexTestDef(prm.BatchCount, prm.BatchSize))
	}

	const dbCommand = "db?command="

	tableCommands := []*indexCommandDef{
		{Command: "create", Link: dbCommand + cmdCreateTable},
		{Command: "drop", Link: dbCommand + cmdDropTable},
		{Command: "deleteRows", Link: dbCommand + cmdDeleteRows},
		{Command: "countRows", Link: dbCommand + cmdCountRows},
	}

	schemaCommands := []*indexCommandDef{
		{Command: "create", Link: dbCommand + cmdCreateSchema},
		{Command: "drop", Link: dbCommand + cmdDropSchema},
	}

	data := &indexData{
		Flags:          flags(),
		TestDefs:       indexTestDefs,
		SchemaName:     string(dba.schemaName),
		TableName:      string(dba.tableName),
		SchemaCommands: schemaCommands,
		TableCommands:  tableCommands,
	}

	// test if data and template definition does match
	if err := tmpl.Execute(io.Discard, data); err != nil {
		return nil, err
	}

	return &indexHandler{tmpl: tmpl, data: data}, nil
}

func (h *indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := h.tmpl.Execute(w, h.data)
	if err != nil {
		log.Printf("template execute error: %s", err)
	}
}
