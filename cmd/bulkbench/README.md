# bulkbench

bulkbench was created in the context of a performance / throughput analysis of different hdb client implementations.

## Test object

Test object is a column table consisting of 2 columns, one of type integer and one of type double:

```
create column table <TableName> (id integer, field double)
```

## Test variants

The basic idea is to insert data in chunks (batchCount) of a fixed amount of records (batchSize) whether sequentially or 'in parallel'.
The actual 'grade of parallelization' is heavily depending on the test environment (CPU cores, TCP/IP stack). bulkbench 'enables' potential parallelism 'idiomatically' via Goroutines. Each Goroutine is using a database connection for the tests being dependent of the Go sql.DB connection pool handling and configuration.
As the test performance results are heavily 'I/O bound' the implementation mainly tries to reduce client server round-trips. Therefore the go-hdb driver bulk insert capabilities are used (please refer to the [go-hdb driver documentation and examples](https://github.com/SAP/go-hdb)
for details).

## In a real world example...

... one might consider

* to implement a worker pool with the number of concurrent workers set in relation to GOMAXPROCS
* optimizing the number of records per chunk (batchSize)
* optimizing the go-hdb driver TCP/IP buffer size.
	* all writes to the TCP/IP connection are buffered by the go-hdb client
	* the buffer size can be configured via the driver.Connector object (BufferSize)
	* when reaching the buffer size, the go-hdb driver writes the buffered data to the TCP/IP connection

## Execute tests

**Caution: please do NOT use a productive HANA instance for testing as bulkbench does create schemas and database tables.**

Executing bulkbench starts a HTTP server on 'localhost:8080'.

After starting a browser pointing to the server address the following HTML page, powered by [htmx](https://htmx.org/) and [Pico.css](https://picocss.com/),
should be visible in the browser window:

![cannot display bulkbench.png](./bulkbench.png)

* the first section displays some runtime information like GOMAXPROCS and the driver and database version
* the second section lists all test relevant parameters which can be set as environment variables or commandline parameters
* the third sections allows to execute tests with predefined BatchCount and BatchSize parameters (see parameters command-line flag)
* the last section provides some database commands for the selected test database schema and table

Clicking on one of the predefined test will execute it and display the result consisting of test parameters and the duration.

## Benchmark

Parallel to the single execution using the browser or any other HTTP client (like wget, curl, ...), the tests can be executed automatically as Go benchmark. The benchmark can be executed whether by 
```
go test -bench .
```
or compiling the benchmark with 
```
go test -c 
```
and executing it via
```
./bulkbench.test -test.bench .
```

In addition to the standard Go benchmarks four additional metrics are reported:
* avgsec/op: the average time (*) 
* maxsec/op: the maximum time (*)
* medsec/op: the median  time (*)
* minsec/op: the minimal time (*)

(*) inserting BatchCount x BatchSize records into the database table when executing one test several times.

For details about Go benchmarks please see the [Golang testing documentation](https://golang.org/pkg/testing).

### Benchmark examples

Finally let's see some examples executing the benchmark.

```
export GOHDBDSN="hdb://MyUser:MyPassword@host:port"
go test -c 
```

* set the data source name (dsn) via environment variable
* and compile the benchmark


```
./bulkbench.test -test.bench . -test.benchtime 10x
```

* -test.bench . (run all benchmarks)
* -test.benchtime 10x (run each benchmark ten times)
* run benchmarks for all BatchCount / BatchSize combinations defined as parameters 
* the test database table is dropped and re-created before each benchmark execution (command-line parameter drop defaults to true)

```
./bulkbench.test -test.bench . -test.benchtime 10x -parameters "10x10000"
```
* same like before but
* execute benchmarks only for 10x10000 as BatchCount / BatchSize combination

```
./bulkbench.test -test.bench . -test.benchtime 10x -wait 5 
```

* same like first example and
* -wait 5 (wait 5 seconds before starting a benchmark run to reduce database pressure)

### Benchmark example output

```
./bulkbench.test -test.bench . -test.benchtime 10x -wait 5

2024/01/21 19:52:15 Runtime Info - GOMAXPROCS: 32 NumCPU: 32 DriverVersion 1.7.5 HDBVersion: 2.00.072.00.1690304772
goos: linux
goarch: amd64
pkg: github.com/SAP/go-hdb/cmd/bulkbench
cpu: AMD Ryzen 9 7950X 16-Core Processor            
Benchmark/sequential-1x100000-32         	      10	5285818357 ns/op	         0.09177 avgsec/op	         0.09784 maxsec/op	         0.09167 medsec/op	         0.08464 minsec/op
Benchmark/sequential-10x10000-32         	      10	5277063412 ns/op	         0.08814 avgsec/op	         0.09130 maxsec/op	         0.08851 medsec/op	         0.08482 minsec/op
Benchmark/sequential-100x1000-32         	      10	5411144659 ns/op	         0.2250 avgsec/op	         0.2429 maxsec/op	         0.2240 medsec/op	         0.2078 minsec/op
Benchmark/sequential-1x1000000-32        	      10	6060119948 ns/op	         0.8404 avgsec/op	         0.9543 maxsec/op	         0.8295 medsec/op	         0.7983 minsec/op
Benchmark/sequential-10x100000-32        	      10	5829117992 ns/op	         0.6361 avgsec/op	         0.6495 maxsec/op	         0.6376 medsec/op	         0.6168 minsec/op
Benchmark/sequential-100x10000-32        	      10	6008048710 ns/op	         0.8170 avgsec/op	         0.8402 maxsec/op	         0.8165 medsec/op	         0.7955 minsec/op
Benchmark/concurrent-1x100000-32         	      10	5273034242 ns/op	         0.08714 avgsec/op	         0.09937 maxsec/op	         0.08703 medsec/op	         0.07726 minsec/op
Benchmark/concurrent-10x10000-32         	      10	5262679752 ns/op	         0.06510 avgsec/op	         0.07544 maxsec/op	         0.06520 medsec/op	         0.05459 minsec/op
Benchmark/concurrent-100x1000-32         	      10	5347686066 ns/op	         0.09944 avgsec/op	         0.1087 maxsec/op	         0.09856 medsec/op	         0.09213 minsec/op
Benchmark/concurrent-1x1000000-32        	      10	6039928922 ns/op	         0.8263 avgsec/op	         0.8493 maxsec/op	         0.8222 medsec/op	         0.7998 minsec/op
Benchmark/concurrent-10x100000-32        	      10	5702794921 ns/op	         0.5025 avgsec/op	         0.5368 maxsec/op	         0.5168 medsec/op	         0.4340 minsec/op
Benchmark/concurrent-100x10000-32        	      10	5663472687 ns/op	         0.4123 avgsec/op	         0.4419 maxsec/op	         0.4131 medsec/op	         0.3869 minsec/op
PASS
```
