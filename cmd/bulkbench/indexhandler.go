package main

import (
	"bytes"
	"flag"
	"html/template"
	"net/http"
	"runtime"
)

// indexHandler implements the http.Handler interface for the html index page.
type indexHandler struct {
	b *bytes.Buffer
}

// newIndexHandler returns a new IndexHandler instance.
func newIndexHandler(testHandler *testHandler, dbHandler *dbHandler) (*indexHandler, error) {
	return (&indexHandler{b: new(bytes.Buffer)}).init(testHandler, dbHandler)
}

func (h *indexHandler) init(testHandler *testHandler, dbHandler *dbHandler) (*indexHandler, error) {
	type page struct {
		GOMAXPROCS    int
		NumCPU        int
		DriverVersion string
		HDBVersion    string
		Flags         []*flag.Flag
		Prms          [][]prm
		Tests         []string
		SchemaName    string
		TableName     string
		SchemaFuncs   []*dbFunc
		TableFuncs    []*dbFunc
		GOOS          string
		GOARCH        string
	}

	indexPage := page{
		GOMAXPROCS:    runtime.GOMAXPROCS(0),
		NumCPU:        runtime.NumCPU(),
		DriverVersion: dbHandler.driverVersion(),
		HDBVersion:    dbHandler.hdbVersion(),
		Flags:         flags(),
		Prms:          parameters.toNumRecordList(),
		Tests:         testHandler.tests(),
		SchemaName:    schemaName,
		TableName:     tableName,
		SchemaFuncs:   dbHandler.schemaFuncs(),
		TableFuncs:    dbHandler.tableFuncs(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
	}
	return h, indexTmpl.Execute(h.b, indexPage)
}

func (h *indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) { w.Write(h.b.Bytes()) }

var indexTmpl = template.Must(template.New("index").Parse(`
{{define "root"}}
<html>
	<head>
		<title>index</title>
	</head>
	<style>
		thead, tbody {
			border: 2px solid black;
			border-collapse: collapse;
		}
		
		table, th, td {
			border: 1px solid black;
			border-collapse: collapse;
		}
	</style>
	
	<body>
	
		<table border="1">
			<tr><th colspan="100%">Runtime information</td></tr>
			<tr><td>GOMAXPROCS</td><td>{{.GOMAXPROCS}}</td></tr>
			<tr><td>NumCPU</td><td>{{.NumCPU}}</td></tr>
			<tr><td>Driver Version</td><td>{{.DriverVersion}}</td></tr>
			<tr><td>HANA Version</td><td>{{.HDBVersion}}</td></tr>
			<tr><td>goos/goarch</td><td>{{.GOOS}}/{{.GOARCH}}</td></tr>
		</table>

		<br/>
		
		<table border="1">
			<tr><th colspan="100%">Test parameter</td></tr>
			<tr>
				<th>Command line flag</td>
				<th>Value</td>
				<th>Usage</td>
			</tr>
			{{range .Flags}}
			<tr>
				<td>{{.Name}}</td>
				<td>{{.Value}}</td>
				<td>{{.Usage}}</td>
			</tr>
			{{end}}
		</table>

		<br/>
		
		<table border="1">
			<thead>
				<tr>
					<th rowspan="2">BatchCount x BatchSize</th>
					<th colspan="1">Sequential</th>
					<th colspan="1">Parallel</th>
				</tr>
			</thead>	
			{{$Tests := .Tests}}
			{{$Prms := .Prms}}
			{{range $PrmSet := $Prms}}
			</tbody>
			{{range $Prm := $PrmSet}}
			<tr>
				<td>{{$Prm.BatchCount}} x {{$Prm.BatchSize}}</td>
				{{range $Test := $Tests}}
				<td>{{with $x := printf "%s?batchcount=%d&batchsize=%d" $Test $Prm.BatchCount $Prm.BatchSize }}<a href={{$x}}>start</a>{{end}}</td>
				{{end}}
			</tr>
			{{end}}
			</tbody>
			{{end}}
		</table>

		<br/>
			
		<table border="1">
			{{$SchemaName := .SchemaName}}
			{{$TableName := .TableName}}
			<tr>
				<th colspan="100%">Database operations</td>
			</tr>
			<tr>
				<td>Table {{$TableName}}</td>
				{{range .TableFuncs}}
				{{$Op := .Op.String}}
				<td>{{with $x := printf "%s?schemaname=%s&tablename=%s" .Command $SchemaName $TableName }}<a href={{$x}}>{{$Op}}</a>{{end}}</td>
				{{end}}
			</tr>
			<tr>
				<td>Schema {{$SchemaName}}</td>
				{{range .SchemaFuncs}}
				{{$Op := .Op.String}}
				<td>{{with $x := printf "%s?schemaname=%s" .Command $SchemaName }}<a href={{$x}}>{{$Op}}</a>{{end}}</td>
				{{end}}
			</tr>
		</table>

	</body>
</html>
{{end}}

{{template "root" .}}
`))
