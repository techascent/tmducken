(ns tmducken.duckdb-test
  (:require [clojure.test :refer [deftest is]]
            [tmducken.duckdb :as duckdb]
            [tmducken.duckdb.ffi :as duckdb-ffi]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.resource :as resource])
  (:import [java.util UUID]
           [java.time LocalTime]
           [tech.v3.dataset Text]))


(duckdb/initialize!)

(def db* (delay (duckdb/initialize!)
                (duckdb/open-db)))

(def conn* (delay (duckdb/connect @db*)))


(deftest trivial
  (try
    (duckdb/drop-table! @conn* "trivial")
    (catch Throwable e nil))
  (let [ds (-> (ds/->dataset [{:a 1}])
               (ds/set-dataset-name "trivial"))]
    (duckdb/create-table! @conn* ds)
    (duckdb/insert-dataset! @conn* ds)
    (is (= 1 (-> (duckdb/sql->dataset @conn* "select * from trivial;")
                 (ds/row-count))))))

(defn supported-datatype-ds
  []
  (-> (ds/->dataset {:boolean [true false true true false false true false false true]
                     :bytes (byte-array (range 10))
                     :shorts (short-array (range 10))
                     :ints (int-array (range 10))
                     :longs (long-array (range 10))
                     :floats (float-array (range 10))
                     :doubles (double-array (range 10))
                     :strings (map str (range 10))
                     ;; :text (map (comp #(Text. %) str) (range 10))
                     ;; :uuids (repeatedly 10 #(UUID/randomUUID))
                     :instants (repeatedly 10 dtype-dt/instant)
                     ;;sql doesn't support dash-case
                     :local_dates (repeatedly 10 dtype-dt/local-date)
                     ;;some sql engines (or the jdbc api) don't support more than second
                     ;;resolution for sql time objects
                     :local_times (->> (repeatedly 10 dtype-dt/local-time))})
      (vary-meta assoc
                 :primary-key :longs
                 :name :testtable)))


(deftest basic-datatype-test
  (try
    (let [ds (supported-datatype-ds)]
      (duckdb/create-table! @conn* ds)
      (duckdb/insert-dataset! @conn* ds)
      (let [sql-ds (duckdb/sql->dataset @conn* "select * from testtable"
                                        {:key-fn keyword})]
        (doseq [column (vals ds)]
          (is (= (vec column)
                 (vec (sql-ds (:name (meta column)))))))))
    (finally
      (try (duckdb/drop-table! @conn* "testtable")
           (catch Throwable e nil)))))

(defonce stocks-src* (delay
                       (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                                     {:key-fn keyword
                                      :dataset-name :stocks})))


(deftest basic-stocks-test
  (try
    (let [stocks @stocks-src*
          _ (do (duckdb/create-table! @conn* stocks)
                (duckdb/insert-dataset! @conn* stocks))
          sql-stocks (duckdb/sql->dataset @conn* "select * from stocks")]
      (is (= (ds/row-count stocks)
             (ds/row-count sql-stocks)))
      (is (= (vec (stocks :symbol))
             (vec (sql-stocks "symbol"))))
      (is (= (vec (stocks :date))
             (vec (sql-stocks "date"))))
      (is (dfn/equals (stocks :price)
                      (sql-stocks "price"))))
    (finally
      (try
        (duckdb/drop-table! @conn* "stocks")
        (catch Throwable e nil)))))


(deftest prepared-statements-test
  (try
    (let [stocks @stocks-src*
          _ (do (duckdb/create-table! @conn* stocks)
                (duckdb/insert-dataset! @conn* stocks))]
      (resource/stack-resource-context
       (let [prep-stmt (duckdb/prepare @conn* "select * from stocks" {:result-type :single})]
         (is (== 560 (ds/row-count (prep-stmt))) "single")))
      (resource/stack-resource-context
       (let [prep-stmt (duckdb/prepare @conn* "select * from stocks" {:result-type :streaming})]
         (is (== 560 (ds/row-count (first (prep-stmt)))) "streaming")))
      (resource/stack-resource-context
       (let [prep-stmt (duckdb/prepare @conn* "select * from stocks where symbol = $1")]
         (is (== (ds/row-count (ds/filter-column @stocks-src* :symbol "AAPL"))
                 (ds/row-count (first (prep-stmt "AAPL")))) "single arg"))))

    (finally
      (try
        (duckdb/drop-table! @conn* "stocks")
        (catch Throwable e nil)))))


(deftest missing-instant-test
  (try
    (let [ds (-> (ds/->dataset {:a [1 2 nil 4 nil 6]
                                :b [(dtype-dt/instant) nil nil (dtype-dt/instant) nil (dtype-dt/instant)]})
                 (vary-meta assoc :name "testdb"))

          _ (do (duckdb/create-table! @conn* ds)
                (duckdb/insert-dataset! @conn* ds))
          sql-ds (duckdb/sql->dataset @conn* "select * from testdb" {:key-fn keyword})]
      (is (= (ds/missing ds)
             (ds/missing sql-ds)))
      (is (= (vec (ds :a))
             (vec (sql-ds :a))))
      (is (= (vec (ds :b))
             (vec (sql-ds :b)))))
    (finally
      (try
        (duckdb/drop-table! @conn* "testdb")
        (catch Throwable e nil)))))

(deftest insert-test
  (let [cn 4
        rn 1024
        ds-fn #(-> (into {} (for [i (range cn)] [(str "c" i)
                                                 (for [_ (range rn)] (str (random-uuid)))]))
                   (ds/->dataset {:dataset-name "t"})
                   (ds/select-columns (for [i (range cn)] (str "c" i))))]
    (try
      (duckdb/drop-table! @conn* "t")
      (catch Throwable e nil))
    (duckdb/create-table! @conn* (ds-fn))
    (duckdb/insert-dataset! @conn* (ds-fn))
    (duckdb/insert-dataset! @conn* (ds-fn))
    (is (= (* 2 rn) (-> (duckdb/sql->dataset @conn* "from t")
                        (ds/row-count))))))

(deftest insert-chunk-size-test
  (let [cn 4
        rn (duckdb-ffi/duckdb_vector_size)
        ds-fn #(-> (into {} (for [i (range cn)] [(str "c" i)
                                                 (for [_ (range rn)] (str (random-uuid)))]))
                   (ds/->dataset {:dataset-name "t"})
                   (ds/select-columns (for [i (range cn)] (str "c" i))))]
    (try
      (duckdb/drop-table! @conn* "t")
      (catch Throwable e nil))
    (duckdb/create-table! @conn* (ds-fn))
    (duckdb/insert-dataset! @conn* (ds-fn))
    (duckdb/insert-dataset! @conn* (ds-fn))
    (is (= (* 2 rn) (-> (duckdb/sql->dataset @conn* "from t")
                        (ds/row-count))))))
