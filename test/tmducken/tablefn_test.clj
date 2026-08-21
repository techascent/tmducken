(ns tmducken.tablefn-test
  (:require [clojure.test :refer [deftest is]]
            [tmducken.duckdb :as duckdb]
            [tmducken.duckdb.tablefn :as tablefn]
            [tech.v3.dataset :as ds]))


(duckdb/initialize!)

(def db* (delay (duckdb/initialize!)
                (duckdb/open-db)))

(def conn* (delay (duckdb/connect @db*)))


(deftest register-42
  (tablefn/register-42! @conn* "forty_two_fn")
  (let [ds (duckdb/sql->dataset @conn* "select * from forty_two_fn()")]
    (is (= [1 1] (ds/shape ds)))
    (is (= ["forty-two"] (vec (ds/column-names ds))))
    (is (= [42] (vec (ds "forty-two")))))
  ;;init allocates fresh scan state each time, so a second scan must produce the same
  ;;row rather than an empty result
  (is (= [42] (vec ((duckdb/sql->dataset @conn* "select * from forty_two_fn()")
                    "forty-two"))))
  ;;two independent scans in one query
  (let [ds (duckdb/sql->dataset
            @conn*
            "select a.\"forty-two\" as l, b.\"forty-two\" as r
             from forty_two_fn() a cross join forty_two_fn() b")]
    (is (= [42] (vec (ds "l"))))
    (is (= [42] (vec (ds "r")))))
  (is (= [1] (vec ((duckdb/sql->dataset @conn* "select count(*) as n from forty_two_fn()")
                   "n")))))
