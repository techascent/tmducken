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
