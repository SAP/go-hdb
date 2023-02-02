package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/SAP/go-hdb/driver"
)

// Flag name constants.
const (
	fnDSN        = "dsn"
	fnHost       = "host"
	fnPort       = "port"
	fnBufferSize = "bufferSize"
	fnParameters = "parameters"
	fnDrop       = "drop"
	fnWait       = "wait"
)

var flagNames = []string{fnDSN, fnHost, fnPort, fnBufferSize, fnParameters, fnDrop, fnWait}

// Environment constants.
const (
	envDSN        = "GOHDBDSN"
	envHost       = "HOST"
	envPort       = "PORT"
	envBufferSize = "BUFFERSIZE"
	envParameters = "PARAMETERS"
	envDrop       = "DROP"
	envWait       = "WAIT"
	envBlur       = "BLUR"
)

var (
	dsn, host, port string
	bufferSize      int
	parameters      = &prmValue{prms: []prm{{1, 100000}, {10, 10000}, {100, 1000}, {1, 1000000}, {10, 100000}, {100, 10000}, {1000, 1000}}}
	drop            bool
	wait            int
)

var initOnce sync.Once

const (
	schemaPrefix = "goHdbTest_"
	tablePrefix  = "table_"
)

// blurFlagSet is used to 'blur' information on the web ui (currently DSN not to expose user, password and ip address)
var blurFlagSet = flag.NewFlagSet("blur", flag.PanicOnError)

func init() {
	initOnce.Do(func() {
		defaultBufferSize := driver.NewConnector().BufferSize()

		flag.StringVar(&dsn, fnDSN, getStringEnv(envDSN, "hdb://MyUser:MyPassword@localhost:39013"), fmt.Sprintf("DNS (environment variable: %s)", envDSN))
		flag.StringVar(&host, fnHost, getStringEnv(envHost, "localhost"), fmt.Sprintf("HTTP host (environment variable: %s)", envHost))
		flag.StringVar(&port, fnPort, getStringEnv(envPort, "8080"), fmt.Sprintf("HTTP port (environment variable: %s)", envPort))
		flag.IntVar(&bufferSize, fnBufferSize, getIntEnv(envBufferSize, defaultBufferSize), fmt.Sprintf("Buffer size in bytes (environment variable: %s)", envBufferSize))
		flag.Var(parameters, fnParameters, fmt.Sprintf("Parameters (environment variable: %s)", envParameters))
		flag.BoolVar(&drop, fnDrop, getBoolEnv(envDrop, true), fmt.Sprintf("Drop table before test (environment variable: %s)", envDrop))
		flag.IntVar(&wait, fnWait, getIntEnv(envWait, 0), fmt.Sprintf("Wait time before starting test in seconds (environment variable: %s)", envWait))

		if _, ok := os.LookupEnv(envBlur); ok {
			blurFlagSet.String(fnDSN, "hdb://MyUser:MyPassword@localhost:39013", fmt.Sprintf("DNS (environment variable: %s)", envDSN))
		}
	})
}

func lookupFlag(name string) (*flag.Flag, bool) {
	if fl := blurFlagSet.Lookup(name); fl != nil {
		return fl, true
	}
	if fl := flag.Lookup(name); fl != nil {
		return fl, true
	}
	return nil, false
}

// flags returns a slice containing all command-line flags defined in this package.
func flags() []*flag.Flag {
	flags := make([]*flag.Flag, 0)
	for _, name := range flagNames {
		if fl, ok := lookupFlag(name); ok {
			flags = append(flags, fl)
		}
	}
	return flags
}

// visit visits the command-line flags defined in this package.
func visit(f func(f *flag.Flag)) {
	for _, fl := range flags() {
		f(fl)
	}
}

// getStringEnv retrieves the string value of the environment variable named by the key.
// If the variable is present in the environment the value is returned.
// Otherwise the default value  defValue is retuned.
func getStringEnv(key, defValue string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return defValue
	}
	return value
}

// getIntEnv retrieves the int value of the environment variable named by the key.
// If the variable is present in the environment the value is returned.
// Otherwise the default value defValue is retuned.
func getIntEnv(key string, defValue int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return defValue
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return defValue
	}
	return i
}

// getBoolEnv retrieves the bool value of the environment variable named by the key.
// If the variable is present in the environment the value is returned.
// Otherwise the default value defValue is retuned.
func getBoolEnv(key string, defValue bool) bool {
	value, ok := os.LookupEnv(key)
	if !ok {
		return defValue
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return defValue
	}
	return b
}
