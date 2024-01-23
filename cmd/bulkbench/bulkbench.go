package main

import (
	"context"
	"embed"
	"errors"
	"flag"
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
var templateFS embed.FS

const (
	tmplIndexName      = "index.html"
	tmplIndexFile      = "templates/index.html"
	tmplDBResultFile   = "templates/dbresult.html"
	tmplTestResultFile = "templates/testresult.html"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	// Print runtime info.
	log.Printf("Runtime Info - GOMAXPROCS: %d NumCPU: %d GOOS/GOARCH: %s/%s", runtime.GOMAXPROCS(0), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)

	dba, err := newDBA(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer dba.close()

	// Create handlers.
	dbHandler, err := newDBHandler(dba)
	if err != nil {
		dba.close()
		log.Fatal(err) //nolint: gocritic
	}
	testHandler, err := newTestHandler(dba)
	if err != nil {
		log.Fatal(err)
	}
	indexHandler, err := newIndexHandler(dba)
	if err != nil {
		log.Fatal(err)
	}

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)

	http.Handle("/test/", testHandler)
	http.Handle("/db/", dbHandler)
	http.Handle("/", indexHandler)
	http.HandleFunc("/favicon.ico", func(http.ResponseWriter, *http.Request) {}) // Avoid "/" handler call for browser favicon request.

	addr := net.JoinHostPort(host, port)
	svr := http.Server{Addr: addr, ReadHeaderTimeout: 30 * time.Second}
	log.Printf("listening on %s ...", addr)

	go func() {
		if err := svr.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-sigint
	// shutdown server
	log.Println("shutting down...")
	if err := svr.Shutdown(context.Background()); err != nil {
		log.Fatalf("HTTP server Shutdown: %v", err)
	}
}
