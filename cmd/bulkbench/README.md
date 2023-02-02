# bulkbench

bulkbench was created in the context of a performance / throughput analysis of different hdb client implementations.

## Test object

Test object is a column table consisting of 2 columns, one of type integer and one of type double:

```
create column table <TableName> (id integer, field double)
```

## Test variants

The basic idea is to insert data in chunks (batchCount) of a fixed amount of records (batchSize) whether sequentially or 'in parallel'.
The actual 'grade of parallelization' is heavily depending on the test environment (CPU cores, TCP/IP stack). bulkbench 'enables'
potential parallelism 'idiomatically' via Goroutines. Each Goroutine is using an own dedicated database connection for the tests
being independent of the Go sql.DB connection pool handling and configuration.
As the test performance results are heavily 'I/O bound' the implementation mainly tries to reduce client server round-trips. Therefore
the go-hdb driver bulk insert capabilities are used (please refer to the [go-hdb driver documentation and examples](https://github.com/SAP/go-hdb)
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

After starting a browser pointing to the server address the following HTML page should be visible in the browser window:

![cannot display bulkbench.png](./bulkbench.png)
 
* the first section displays some runtime information like GOMAXPROCS and the driver and database version
* the second section lists all test relevant parameters which can be set as environment variables or commandline parameters
* the third sections allows to execute tests with predefined BatchCount and BatchSize parameters (see parameters command-line flag)
* the last section provides some database operations for the selected test database schema and table

Clicking on one of the predefined test will execute it and display the result consisting of test parameters and the duration in seconds.
The result is a JSON payload, which provides an easy way to be interpreted by a program.

## URL format 

Running bulkbench as HTTP server a test can be executed via a HTTP GET using the following URL format:

```
http://<host>:<port>/test/<TestType>?batchcount=<number>&batchsize=<number>
```
with 
```
<TestType> =:= Seq | Par
```

## Benchmark

Parallel to the single execution using the browser or any other HTTP client (like wget, curl, ...), the tests can be executed automatically
as Go benchmark. The benchmark can be executed whether by 
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

The benchmark is 'self-contained', meaning it includes its own http server (for details please see [httptest](https://golang.org/pkg/net/http/httptest/).

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

GOMAXPROCS: 8
NumCPU: 8
Driver Version: 0.110.0
HANA Version: 2.00.045.00.1575639312
goos: darwin
goarch: arm64
pkg: github.com/SAP/go-hdb/cmd/bulkbench
Benchmark/seq-1x100000-8         	      10	5633601117 ns/op	         0.2156 avgsec/op	         0.2486 maxsec/op	         0.2155 medsec/op	         0.1947 minsec/op
Benchmark/seq-10x10000-8         	      10	5699490917 ns/op	         0.2786 avgsec/op	         0.3108 maxsec/op	         0.2803 medsec/op	         0.2559 minsec/op
Benchmark/seq-100x1000-8         	      10	6165608862 ns/op	         0.7506 avgsec/op	         0.8077 maxsec/op	         0.7440 medsec/op	         0.6990 minsec/op
Benchmark/seq-1x1000000-8        	      10	6938632171 ns/op	         1.514 avgsec/op	         1.629 maxsec/op	         1.515 medsec/op	         1.407 minsec/op
Benchmark/seq-10x100000-8        	      10	7054997538 ns/op	         1.636 avgsec/op	         1.725 maxsec/op	         1.622 medsec/op	         1.536 minsec/op
Benchmark/seq-100x10000-8        	      10	7810094800 ns/op	         2.397 avgsec/op	         2.557 maxsec/op	         2.411 medsec/op	         2.262 minsec/op
Benchmark/seq-1000x1000-8        	      10	12483512412 ns/op	         7.064 avgsec/op	         7.356 maxsec/op	         7.038 medsec/op	         6.812 minsec/op
Benchmark/par-1x100000-8         	      10	5599063146 ns/op	         0.2224 avgsec/op	         0.2483 maxsec/op	         0.2207 medsec/op	         0.2002 minsec/op
Benchmark/par-10x10000-8         	      10	5897479183 ns/op	         0.1346 avgsec/op	         0.1571 maxsec/op	         0.1305 medsec/op	         0.1186 minsec/op
Benchmark/par-100x1000-8         	      10	9334510842 ns/op	         0.2375 avgsec/op	         0.2488 maxsec/op	         0.2389 medsec/op	         0.2175 minsec/op
Benchmark/par-1x1000000-8        	      10	6892064029 ns/op	         1.451 avgsec/op	         1.581 maxsec/op	         1.434 medsec/op	         1.361 minsec/op
Benchmark/par-10x100000-8        	      10	6699703246 ns/op	         0.9531 avgsec/op	         1.022 maxsec/op	         0.9434 medsec/op	         0.9004 minsec/op
Benchmark/par-100x10000-8        	      10	10699403688 ns/op	         0.9221 avgsec/op	         0.9838 maxsec/op	         0.9181 medsec/op	         0.8722 minsec/op
Benchmark/par-1000x1000-8        	      10	50726314071 ns/op	         2.204 avgsec/op	         2.301 maxsec/op	         2.216 medsec/op	         2.096 minsec/op
PASS
```
