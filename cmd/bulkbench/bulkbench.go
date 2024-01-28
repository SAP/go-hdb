package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint: gosec
	"os"
	"os/signal"
	"runtime"
	"time"
)

//go:embed templates/*
var rootFS embed.FS

const (
	tmplIndex      = "index.html"
	tmplDBResult   = "dbresult.html"
	tmplTestResult = "testresult.html"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	// Print runtime info.
	log.Printf("Runtime Info - GOMAXPROCS: %d NumCPU: %d GOOS/GOARCH: %s/%s", runtime.GOMAXPROCS(0), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)

	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dba, err := newDBA(dsn)
	if err != nil {
		return err
	}
	defer dba.close()

	templateFS, err := fs.Sub(rootFS, "templates")
	if err != nil {
		return err
	}
	testHandler, err := newTestHandler(dba, templateFS)
	if err != nil {
		return err
	}
	dbHandler, err := newDBHandler(dba, templateFS)
	if err != nil {
		return err
	}
	indexHandler, err := newIndexHandler(dba, templateFS)
	if err != nil {
		return err
	}

	http.Handle("/test/", testHandler)
	http.Handle("/db/", dbHandler)
	http.Handle("/", indexHandler)
	http.HandleFunc("/favicon.ico", func(http.ResponseWriter, *http.Request) {}) // Avoid "/" handler call for browser favicon request.
	addr := net.JoinHostPort(host, port)
	srv := http.Server{Addr: addr, ReadHeaderTimeout: 30 * time.Second}

	done := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint

		// shutdown server.
		log.Println("shutting down...")
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Print(err)
		}
		close(done)
	}()

	log.Printf("listening on %s ...", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Print(err)
	}
	<-done
	return nil
}
