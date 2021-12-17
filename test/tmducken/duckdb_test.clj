(ns tmducken.duckdb-test
  (:require [clojure.test :refer [deftest is]]
            [tmducken.duckdb :as duckdb]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]))



(duckdb/initialize!)

(def db* (delay (duckdb/initialize!)
                (duckdb/open-db)))

(def conn* (delay (duckdb/connect @db*)))


(deftest basic-stocks-test
  (let [stocks (-> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv" {:key-fn keyword})
                   (vary-meta assoc :name :stocks))
        _ (do (duckdb/create-table! @conn* stocks)
              (duckdb/append-dataset! @conn* stocks))
        sql-stocks (duckdb/execute-query! @conn* "select * from stocks")]
    (is (= (ds/row-count stocks)
           (ds/row-count sql-stocks)))
    (is (= (vec (stocks :symbol))
           (vec (sql-stocks "symbol"))))
        (is (= (vec (stocks :date))
           (vec (sql-stocks "date"))))
    (is (dfn/equals (stocks :price)
                    (sql-stocks "price")))))


(deftest missing-instant-test
  (let [ds (-> (ds/->dataset {:a [1 2 nil 4 nil 6]
                              :b [(dtype-dt/instant) nil nil (dtype-dt/instant) nil (dtype-dt/instant)]})
               (vary-meta assoc :name "testdb"))

        _ (do (duckdb/create-table! @conn* ds)
              (duckdb/append-dataset! @conn* ds))
        sql-ds (duckdb/execute-query! @conn* "select * from testdb" {:key-fn keyword})]
    (is (= (ds/missing ds)
           (ds/missing sql-ds)))
    (is (= (vec (ds :a))
           (vec (sql-ds :a))))
    (is (= (vec (ds :b))
           (vec (sql-ds :b))))))
