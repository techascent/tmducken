# tech.ml.dataset Integration with DuckDB

[![Clojars Project](https://clojars.org/com.techascent/tmducken/latest-version.svg)](https://clojars.org/com.techascent/tmducken)


[DuckDB](https://duckdb.org/) is a high performance in-process database system.  It is a
natural pairing for [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset) which is
a high performance column-major in-memory dataframe system similar to pandas or R's data table.
DuckDB provides perhaps [cleaner pathways](https://duckdb.org/docs/data/overview) to load/save
parquet files as you don't need to navigate the
[minefield of dependencies](https://techascent.github.io/tech.ml.dataset/tech.v3.libs.parquet.html)
required to use parquet from Clojure.

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> (def stocks
        (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                      {:key-fn keyword
                       :dataset-name :stocks}))
#'user/stocks
user> (ds/head stocks)
:stocks [5 3]:

| :symbol |      :date | :price |
|---------|------------|-------:|
|    MSFT | 2000-01-01 |  39.81 |
|    MSFT | 2000-02-01 |  36.35 |
|    MSFT | 2000-03-01 |  43.22 |
|    MSFT | 2000-04-01 |  28.37 |
|    MSFT | 2000-05-01 |  25.45 |
user> (require '[tmducken.duckdb :as duckdb])
nil
user> (duckdb/initialize!)
Aug 31, 2023 8:50:21 AM clojure.tools.logging$eval5800$fn__5803 invoke
INFO: Attempting to load duckdb from "./binaries/libduckdb.so"
true
user> (def db (duckdb/open-db))
#'user/db
user> (def conn (duckdb/connect db))
#'user/conn
user> (duckdb/create-table! conn stocks)
"stocks"
user> (duckdb/insert-dataset! conn stocks)
560
user> (ds/head (duckdb/sql->dataset conn \"select * from stocks\"))

_unnamed [5 3]:

| symbol |       date | price |
|--------|------------|------:|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
user> (def stmt (duckdb/prepare conn \"select * from stocks\"))
Aug 31, 2023 8:52:25 AM clojure.tools.logging$eval5800$fn__5803 invoke
INFO: Reference thread starting
#'user/stmt
user> stmt
#duckdb-prepared-statement-0[\"select * from stocks\"]
user> (stmt)
#duckdb-streaming-result[\"select * from stocks\"]
user> (ds/head (first *1))
_unnamed [5 3]:

| symbol |       date | price |
|--------|------------|------:|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
```


* [API Documentation](https://techascent.github.io/tmducken/)


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

### Preferred method (Nix)

You can get a jar for your own OS (Linux/MacOS) with duckdb included by [installing Nix](https://github.com/DeterminateSystems/nix-installer),
then running:

``` console
$ nix build github:schemamap/tmducken

$ uname -s -p
Darwin arm

$ jar tf result/tmducken.jar | grep libduckdb.dylib
darwin-aarch64/libduckdb.dylib
```

You can include this in your deps.edn via: `{:local/root "result/tmducken.jar"}`
With this method, you can invoke `(duckdb/initialize!)` normally and it will load the shared library automatically.

### Manual method

First, download binaries and set either install then into your
system path or set the DUCKDB_HOME environment variable to where
the shared library is installed - see [enable-duckdb](scripts/enable-duckdb)
script.  Linux users can simply type:

```console
source scripts/enable-duckdb
```

Next, you should be able to call [initialize!](https://techascent.github.io/tmducken/tmducken.duckdb.html#var-initialize.21)
in the duckdb namespace.  Be sure to read the [namespace documentation](https://techascent.github.io/tmducken/tmducken.duckdb.html)
and perhaps peruse the [unit tests](test/tmducken/duckdb_test.clj).


## License

 * [MIT License](LICENSE)
