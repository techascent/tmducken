## 1.5.5-01
 * Support for the 1.X series of duckdb - tested against 1.5.5.  **duckdb 0.X is no longer supported.**
 * Migrated off the deprecated `duckdb_stream_fetch_chunk` (-> `duckdb_fetch_chunk`),
   `duckdb_appender_error` (-> `duckdb_appender_error_data`) and the deprecated
   `error_message` field of `duckdb_result` (-> `duckdb_result_error`).
 * `duckdb_result_is_streaming` is no longer consulted - the requested `:result-type`
   already determines how the result was executed, so it is threaded through directly.
 * Dropped the unused `duckdb_row_count` binding.
 * `open-db`, `run-query!` and `insert-dataset!` now throw `RuntimeException` rather than
   `Exception`, matching the rest of the namespace.  Existing `(catch Exception ...)` handlers
   are unaffected.
 * Added the type constants introduced since 0.10 - `ARRAY`, `ANY`, `BIGNUM`, `SQLNULL`,
   `STRING_LITERAL`, `INTEGER_LITERAL`, `TIME_NS`, `GEOMETRY` and `VARIANT`.  Reading columns
   of these types is not implemented yet, but they now report a named type rather than a
   bare id.

## 0.10.1-01
 * Support for 0.10.X series of duckdb - 0.10.0 was a bad release however - do not use!!
 * Support for list datatype
 * Support for schema tables during insert-dataset!
 
## 0.8.1-13
 * Errors are [correctly reported for prepared statements](https://github.com/techascent/tmducken/pull/16).
 * Support for [packaging the binary in the jar using nix](https://github.com/techascent/tmducken/pull/15).
 
## 0.8.1-12
 * Two insert issues fixed and uuid support.

## 0.8.1-11
 * Optimization for very large (> 128 chars) strings.

## 0.8.1-10
 * Fix for large strings

## 0.8.1-09
 * Many serious perf improvements benchmarking loading a very large dataset.  System appears to
   be running beautifully.

## 0.8.1-06
 * Initial prepared statement support.

## 0.8.1-05
 * small perf upgrades.

## 0.8.1-04
 * parallelized string conversion on insert.
 * small perf upgrades to dtype-next.

## 0.8.1-02
 - read/write of datasets now uses the data chunk api.  This is major upgrade
   and requires dtype 10.003 for pass/return by value support.
