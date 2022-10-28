package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	checkErr := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}

	// Print runtime info.
	log.Printf("Runtime Info - GOMAXPROCS: %d NumCPU: %d GOOS/GOARCH: %s/%s", runtime.GOMAXPROCS(0), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)

	s := make([]string, 0)
	visit(func(f *flag.Flag) {
		s = append(s, fmt.Sprintf("%s:%s", f.Name, f.Value))
	})
	log.Printf("Command line flags: %s", strings.Join(s, " "))

	// Create handlers.
	dbHandler, err := newDBHandler(log.Printf)
	checkErr(err)
	testHandler, err := newTestHandler(log.Printf)
	checkErr(err)
	indexHandler, err := newIndexHandler(testHandler, dbHandler)
	checkErr(err)

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)

	mux := http.NewServeMux()

	mux.Handle("/test/", testHandler)
	mux.Handle("/db/", dbHandler)
	mux.Handle("/", indexHandler)
	mux.HandleFunc("/favicon.ico", func(http.ResponseWriter, *http.Request) {}) // Avoid "/" handler call for browser favicon request.

	// pprof
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	svr := http.Server{Addr: net.JoinHostPort(host, port), Handler: mux}
	log.Println("listening...")

	go func() {
		if err := svr.ListenAndServe(); err != http.ErrServerClosed {
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
