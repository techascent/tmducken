# tech.ml.dataset Integration with DuckDB

## THIS REPO HAS MOVED ##

 * [techascent/tmducken](https://github.com/techascent/tmducken)
 
 

[![Clojars Project](https://clojars.org/com.cnuernber/tmducken/latest-version.svg)](https://clojars.org/com.cnuernber/tmducken)


[DuckDB](https://duckdb.org/) is a high performance in-process database system.  It is a 
natural pairing for [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset) which is 
a high performance column-major in-memory dataframe system similar to pandas or R's data table.
DuckDB provides perhaps [cleaner pathways](https://duckdb.org/docs/data/overview) to load/save 
parquet files as you don't need to navigate the 
[minefield of dependencies](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html) 
required to use parquet from Clojure.


* [API Documentation](https://cnuernber.github.io/tmducken/)


This system requires the user to install/manage the duckdb binary dependency.  DuckDB also supplies a
[JDBC driver](https://search.maven.org/artifact/org.duckdb/duckdb_jdbc) ([documentation](https://duckdb.org/docs/api/java))
so this library is entirely optional as you can use the jdbc driver with tmd's existing
[jdbc integration](https://github.com/techascent/tech.ml.dataset.sql).

 
What this library provides beyond the JDBC pathway is very fast transfer from the
duckdb query result, which is stored already in column-major form, to a dataset.
The JDBC driver forces you to go row by row and essentially use the
sequence-of-maps->dataset pathway.  This library is also another example of how to
use [dtype-next's ffi system to bind to a C library](src/tmducken/duckdb/ffi.clj).


One thing to note is that because duckdb already supplies the query result columns to the
user in data form there is no advantage to using apache arrow as a query result.


## Usage

First, download binaries and set either install then into your
system path or set the DUCKDB_HOME environment variable to where
the shared library is installed - see [enable-duckdb](scripts/enable-duckdb)
script.  Linux users can simply type:

```console
source scripts/enable-duckdb
```

Next, you should be able to call [initialize!](https://cnuernber.github.io/tmducken/tmducken.duckdb.html#var-initialize.21)
in the duckdb namespace.  Be sure to read the [namespace documentation](https://cnuernber.github.io/tmducken/tmducken.duckdb.html)
and perhaps peruse the [unit tests](test/tmducken/duckdb_test.clj).


## License

 * [MIT License](LICENSE)
