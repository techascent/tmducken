(ns tmducken.duckdb-test
  (:require [clojure.test :refer [deftest is]]
            [tmducken.duckdb :as duckdb]
            [tmducken.duckdb.ffi :as duckdb-ffi]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.resource :as resource])
  (:import [java.util UUID]
           [java.time LocalTime]
           [tech.v3.dataset Text]))


(duckdb/initialize!)

(def db* (delay (duckdb/initialize!)
                (duckdb/open-db)))

(def conn* (delay (duckdb/connect @db*)))


(defn drop-table-nf ;;drop-table-no-fail
  [table-name]
  (try
    (duckdb/drop-table! @conn* table-name)
    (catch Throwable _e)))

(defmacro with-temp-table
  [tablename & code]
  `(try
     (drop-table-nf ~tablename)
     ~@code
     (finally
       (drop-table-nf ~tablename))))


(deftest trivial
  (with-temp-table "trivial"
    (let [ds (-> (ds/->dataset [{:a 1}])
                 (ds/set-dataset-name "trivial"))]
      (duckdb/create-table! @conn* ds)
      (duckdb/insert-dataset! @conn* ds)
      (is (= 1 (-> (duckdb/sql->dataset @conn* "select * from trivial;")
                   (ds/row-count)))))))

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
                     :uuids (repeatedly 10 #(UUID/randomUUID))
                     :instants (repeatedly 10 dtype-dt/instant)
                     ;;sql doesn't support dash-case
                     :local_dates (repeatedly 10 dtype-dt/local-date)
                     ;;some sql engines (or the jdbc api) don't support more than second
                     ;;resolution for sql time objects
                     :local_times (->> (repeatedly 10 dtype-dt/local-time))
                     })
      (vary-meta assoc
                 :primary-key :longs
                 :name :testtable)))


(deftest basic-datatype-test
  (with-temp-table "testtable"
    (let [ds (supported-datatype-ds)]
      (duckdb/create-table! @conn* ds)
      (duckdb/insert-dataset! @conn* ds)
      (let [sql-ds (duckdb/sql->dataset @conn* "select * from testtable"
                                        {:key-fn keyword})]
        (doseq [column (vals ds)]
          (is (= (vec column)
                 (vec (sql-ds (:name (meta column)))))))))))

(defonce stocks-src* (delay
                       (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                                     {:key-fn keyword
                                      :dataset-name :stocks})))


(deftest basic-stocks-test
  (with-temp-table "stocks"
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
                      (sql-stocks "price"))))))


(deftest filter-stonks-test
  (with-temp-table "stonks"
    (let [stonks (-> (apply ds/concat (repeat 10 @stocks-src*))
                     (ds/row-map (fn [m]
                                   (cond
                                     (< (:price m) 37.) (update m :price (constantly nil))
                                     :else m)))
                     (vary-meta assoc :name :stonks))]
      (do (duckdb/create-table! @conn* stonks)
          (duckdb/insert-dataset! @conn* stonks))
      (let [sql-stocks (duckdb/sql->dataset @conn* "select * from stonks")]
        (is (= (ds/row-count stonks)
               (ds/row-count sql-stocks)))
        (is (= (vec (stonks :symbol))
               (vec (sql-stocks "symbol"))))
        (is (= (vec (stonks :date))
               (vec (sql-stocks "date"))))
        (is (= (vec (bitmap/->random-access (ds/missing stonks)))
               (vec (bitmap/->random-access (ds/missing sql-stocks)))))
        (is (dfn/equals (stonks :price)
                        (sql-stocks "price")))))))


(deftest prepared-statements-test
  (with-temp-table "stocks"
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
                 (ds/row-count (first (prep-stmt "AAPL")))) "single arg"))))))


(deftest missing-instant-test
  (with-temp-table "testdb"
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
             (vec (sql-ds :b)))))))

(deftest insert-test
  (with-temp-table "t"
    (let [cn 4
          rn 1024
          ds-fn #(-> (into {} (for [i (range cn)] [(str "c" i)
                                                   (for [_ (range rn)] (str (random-uuid)))]))
                     (ds/->dataset {:dataset-name "t"})
                     (ds/select-columns (for [i (range cn)] (str "c" i))))]
      (duckdb/create-table! @conn* (ds-fn))
      (duckdb/insert-dataset! @conn* (ds-fn))
      (duckdb/insert-dataset! @conn* (ds-fn))
      (is (= (* 2 rn) (-> (duckdb/sql->dataset @conn* "from t")
                          (ds/row-count)))))))

(deftest insert-chunk-size-test
  (with-temp-table "t"
    (let [cn 4
          rn (duckdb-ffi/duckdb_vector_size)
          ds-fn #(-> (into {} (for [i (range cn)] [(str "c" i)
                                                   (for [_ (range rn)] (random-uuid))]))
                     (ds/->dataset {:dataset-name "t"})
                     (ds/select-columns (for [i (range cn)] (str "c" i))))]
      (duckdb/create-table! @conn* (ds-fn))
      (duckdb/insert-dataset! @conn* (ds-fn))
      (duckdb/insert-dataset! @conn* (ds-fn))
      (is (= (* 2 rn) (-> (duckdb/sql->dataset @conn* "from t")
                          (ds/row-count)))))))


(deftest pivot-test
  (with-temp-table "cities"
    (let [cities
          [{:country "NL" :name "Amsterdam" :year 2000 :population 1005}
           {:country "NL" :name "Amsterdam" :year 2010 :population 1065}
           {:country "NL" :name "Amsterdam" :year 2020 :population 1158}
           {:country "US" :name "Seattle" :year 2000 :population 564}
           {:country "US" :name "Seattle" :year 2010 :population 608}
           {:country "US" :name "Seattle" :year 2020 :population 738}
           {:country "US" :name "New York" :year 2000 :population 8015}
           {:country "US" :name "New York" :year 2010 :population 8175}
           {:country "US" :name "New York" :year 2020 :population 8772}]
          cities-ds (ds/->dataset cities {:dataset-name "cities"})]
      (duckdb/create-table! @conn* cities-ds)
      (duckdb/insert-dataset! @conn* cities-ds)
      (let [result-ds (duckdb/sql->dataset
                       @conn*
                       "SELECT country, name, \"2010\"::Integer as '2010' FROM (PIVOT cities ON year USING sum(population::SmallInt) order by country, name);")]
        (is (= [{"country" "NL", "name" "Amsterdam", "2010" 1065}
                {"country" "US", "name" "New York", "2010" 8175}
                {"country" "US", "name" "Seattle", "2010" 608}]
               (ds/rows result-ds)))))))

(deftest uuid-test
  (with-temp-table "uuid_test"
    (let [id (random-uuid)
          data (ds/->dataset [{:id id}] {:dataset-name "uuid_test"})]
      (duckdb/create-table! @conn* data)
      (duckdb/insert-dataset! @conn* data)
      (let [result-ds (duckdb/sql->dataset
                       @conn*
                       (format "SELECT id from uuid_test where id = '%s'" id))]
        (is (some? result-ds))
        (is (= 1 (count (ds/rows result-ds))))
        (is (= [{"id" id}] (ds/rows result-ds)))))))

(deftest show-all-tables-test
  (with-temp-table "foo"
    (let [ds (-> (ds/->dataset [{:a 1}])
                 (ds/set-dataset-name "foo"))
          _ (do (duckdb/create-table! @conn* ds)
                (duckdb/insert-dataset! @conn* ds))
          alltables (duckdb/sql->dataset @conn* "show all tables;")
          _ (do (is (= [["a"]] (alltables "column_names")))
                (is (= [["BIGINT"]] (alltables "column_types")))
                (drop-table-nf "foo"))
          ds (-> (supported-datatype-ds)
                 (ds/set-dataset-name "foo"))
          _ (do (duckdb/create-table! @conn* ds)
                (duckdb/insert-dataset! @conn* ds))
          alltables (duckdb/sql->dataset @conn* "show all tables;")
          _ (is (= #{"boolean"
                     "shorts"
                     "uuids"
                     "strings"
                     "longs"
                     "instants"
                     "doubles"
                     "ints"
                     "bytes"
                     "floats"
                     "local_dates"
                     "local_times"}
                   (set (first (alltables "column_names")))))])))
