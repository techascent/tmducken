(ns tmducken.duckdb
  "DuckDB C-level bindings for tech.ml.dataset.

  Current datatype support:

  * boolean, all numeric types int8->int64, uint8->uint64, float32, float64.
  * string
  * LocalDate, Instant column types.


  Example:

  ```clojure

user> (require '[tech.v3.dataset :as ds])
nil
user> (require '[tmducken.duckdb :as duckdb])
nil
user> (duckdb/initialize!)
10:04:14.814 [nREPL-session-635e9bc8-2923-442b-9fad-da547210617b] INFO tmducken.duckdb - Attempting to load duckdb from \"/home/chrisn/dev/cnuernber/tmducken/binaries/libduckdb.so\"
true
user> (def stocks
        (-> (ds/->dataset \"https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv\" {:key-fn keyword})
            (vary-meta assoc :name :stocks)))
#'user/stocks
user> (def db (duckdb/open-db))
#'user/db
user> (def conn (duckdb/connect db))
#'user/conn
user> (duckdb/create-table! conn stocks)
\"stocks\"
  user> (duckdb/insert-dataset! conn stocks)
nil
  user> (ds/head (duckdb/sql->dataset conn \"select * from stocks\"))

_unnamed [5 3]:

| symbol |       date | price |
|--------|------------|------:|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
```"
  (:require [tmducken.duckdb.ffi :as duckdb-ffi]
            [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.struct :as dt-struct]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype :as dt]
            [tech.v3.datatype.datetime :as dt-datetime]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.resource :as resource]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.sql :as sql]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc]
            [clojure.tools.logging :as log])
  (:import [java.nio.file Paths]
           [java.util Map Iterator ArrayList]
           [java.time LocalDate LocalTime]
           [tech.v3.datatype.ffi Pointer]
           [ham_fisted ITypedReduce]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang Seqable IReduceInit]))


(set! *warn-on-reflection* true)


(defonce ^:private initialize* (atom false))

(defn initialized?
  []
  @initialize*)


(defn initialize!
  "Initialize the duckdb ffi system.  This must be called first should be called only once.
  It is safe, however, to call this multiple times.

  Options:

  * `:duckdb-home` - Directory in which to find the duckdb shared library.  Users can pass
  this in.  If not passed in, then the environment variable `DUCKDB_HOME` is checked.  If
  neither is passed in then the library will be searched in the normal system library
  paths."
  ([{:keys [duckdb-home]}]
   (swap! initialize*
          (fn [is-init?]
            (when-not is-init?
              (let [duckdb-home (or duckdb-home
                                    (System/getenv "DUCKDB_HOME")
                                    "./binaries")
                    libpath (if-not (empty? duckdb-home)
                              (str (Paths/get duckdb-home
                                              (into-array String [(System/mapLibraryName "duckdb")])))
                              "duckdb")]
                (if libpath
                  (log/infof "Attempting to load duckdb from \"%s\"" libpath)
                  (log/infof "Attempting to load in-process duckdb" libpath))
                (duckdb-ffi/define-datatypes!)
                (dt-ffi/library-singleton-set! duckdb-ffi/lib libpath)))
            true)))
  ([] (initialize! nil)))


(defn get-config-options
  "Returns a sequence of maps of {:name :desc} describing valid valid configuration
  options to the open-db function."
  []
  (resource/stack-resource-context
   (->> (range (duckdb-ffi/duckdb_config_count))
        (mapv (fn [^long idx]
                (let [msg-ptr (dt-ffi/make-ptr :pointer 0)
                      desc-ptr (dt-ffi/make-ptr :pointer 0)]
                  (duckdb-ffi/duckdb_get_config_flag idx msg-ptr desc-ptr)
                  {:name (dt-ffi/c->string (Pointer. (msg-ptr 0)))
                   :desc (dt-ffi/c->string (Pointer. (desc-ptr 0)))}))))))


(defn open-db
  "Open a database.  `path` may be nil in which case database is opened in-memory.
  For valid config options call [[get-config-options]].  Options must be
  passed as a map of string->string.  As duckdb is dynamically linked configuration options
  may change but with `linux-amd64-0.3.1` current options are:

```clojure
tmducken.duckdb> (get-config-options)
[{:name \"access_mode\",
  :desc \"Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)\"}
 {:name \"default_order\",
  :desc \"The order type used when none is specified ([ASC] or DESC)\"}
 {:name \"default_null_order\",
  :desc \"Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)\"}
 {:name \"enable_external_access\",
  :desc
  \"Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)\"}
 {:name \"enable_object_cache\",
  :desc \"Whether or not object cache is used to cache e.g. Parquet metadata\"}
 {:name \"max_memory\", :desc \"The maximum memory of the system (e.g. 1GB)\"}
 {:name \"threads\", :desc \"The number of total threads used by the system\"}]
```"
  (^Pointer [^String path config-options]
   (resource/stack-resource-context
    (let [path (or path "")
          config-ptr (when-not (empty? config-options)
                       (let [config-ptr (dt-ffi/make-ptr :pointer 0)
                             _ (duckdb-ffi/duckdb_create_config config-ptr)
                             cfg (Pointer. (config-ptr 0))]
                         (doseq [[k v] config-options]
                           (duckdb-ffi/duckdb_set_config cfg (str k) (str v)))
                         config-ptr))
          config (when config-ptr
                   (Pointer. (config-ptr 0)))
          db-ptr (dt-ffi/make-ptr :pointer 0)
          err (dt-ffi/make-ptr :pointer 0)
          open-retval (duckdb-ffi/duckdb_open_ext path db-ptr config err)]
      (when config-ptr
        (duckdb-ffi/duckdb_destroy_config config-ptr))
      (when-not (= open-retval duckdb-ffi/DuckDBSuccess)
        (let [err-ptr (Pointer. (err 0))
              err-str (dt-ffi/c->string err-ptr)]
          (duckdb-ffi/duckdb_free err-ptr)
          (throw (Exception. (format "Error opening database: %s" err-str)))))
      (Pointer. (db-ptr 0)))))
  (^Pointer [^String path]
   (open-db path nil))
  (^Pointer []
   (open-db "")))


(defn close-db
  "Close the database."
  [^Pointer db]
  (resource/stack-resource-context
   (let [db-ptr (dt-ffi/make-ptr :pointer (.address db))]
     (duckdb-ffi/duckdb_close db-ptr))))


(defn connect
  "Create a new database connection from an opened database.
  Users should call disconnect to close this connection."
  ^Pointer [^Pointer db]
  (resource/stack-resource-context
   (let [ctx-ptr (dt-ffi/make-ptr :pointer 0)]
     (duckdb-ffi/duckdb_connect db ctx-ptr)
     (Pointer. (ctx-ptr 0)))))


(defn disconnect
  "Disconnect a connection."
  [^Pointer conn]
  (resource/stack-resource-context
   (let [conn-ptr (dt-ffi/make-ptr :pointer (.address conn))]
     (duckdb-ffi/duckdb_disconnect conn-ptr))))


(defn- run-query!
  ([conn sql options]
   (let [query-res (dt-struct/new-struct :duckdb-result {:container-type :native-heap
                                                         :resource-type nil})
         success? (= (duckdb-ffi/duckdb_query conn (str sql) query-res)
                     duckdb-ffi/DuckDBSuccess)
         query-ptr (dt-ffi/->pointer query-res)
         ;;destructor must only be called once and cannot reference query-res as that will
         ;;create a circular references.
         destructor! (fn []
                       (duckdb-ffi/duckdb_destroy_result query-ptr)
                       (native-buffer/free (.address query-ptr)))]
     (if-not success?
       (let [error-msg (dt-ffi/c->string (Pointer. (query-res :error-message)))]
         (destructor!)
         (throw (Exception. error-msg)))
       (resource/track query-res {:track-type (get options :resource-type :auto)
                                  :dispose-fn destructor!}))))
  ([conn sql]
   (run-query! conn sql nil)))


(defn create-table!
  "Create an sql table based off of the column datatypes of the dataset.  Note that users
  can also call [[execute-query!]] with their own sql create-table string.  Note that the
  fastest way to get data into the system is [[append-dataset!]].

  Options:

  * `:table-name` - Name of the table to create.  If not supplied the dataset name will
     be used.
  * `:primary-key` - sequence of column names to be used as the primary key."
  ([conn dataset options]
   (let [sql (sql/create-sql "duckdb" dataset)]
     (resource/stack-resource-context
      (run-query! conn sql))
     (sql/table-name dataset options)))
  ([conn dataset]
   (create-table! conn dataset nil)))


(sql/set-datatype-mapping! "duckdb" :boolean "bool" -7
                           sql/generic-sql->column sql/generic-column->sql)
(sql/set-datatype-mapping! "duckdb" :string "varchar" 12
                           sql/generic-sql->column sql/generic-column->sql)


(defn drop-table!
  [conn dataset]
  (let [ds-name (sql/table-name dataset)]
    (resource/stack-resource-context
     (run-query! conn (format "drop table %s" ds-name))
     ds-name)))


(defn- local-time->microseconds
  ^long [^LocalTime lt]
  (if lt
    (-> (.toNanoOfDay lt)
        (/ 1000))
    0))


(defn- dtype-type->duckdb
  [dt]
  (case dt
    :boolean duckdb-ffi/DUCKDB_TYPE_BOOLEAN
    :int8  duckdb-ffi/DUCKDB_TYPE_TINYINT
    :uint8 duckdb-ffi/DUCKDB_TYPE_UTINYINT
    :int16 duckdb-ffi/DUCKDB_TYPE_SMALLINT
    :uint16 duckdb-ffi/DUCKDB_TYPE_USMALLINT
    :int32 duckdb-ffi/DUCKDB_TYPE_INTEGER
    :uint32 duckdb-ffi/DUCKDB_TYPE_UINTEGER
    :int64 duckdb-ffi/DUCKDB_TYPE_BIGINT
    :uint64 duckdb-ffi/DUCKDB_TYPE_UBIGINT
    :float32 duckdb-ffi/DUCKDB_TYPE_FLOAT
    :float64 duckdb-ffi/DUCKDB_TYPE_DOUBLE
    :local-date duckdb-ffi/DUCKDB_TYPE_DATE
    :local-time duckdb-ffi/DUCKDB_TYPE_TIME
    :instant duckdb-ffi/DUCKDB_TYPE_TIMESTAMP
    :string duckdb-ffi/DUCKDB_TYPE_VARCHAR))


(defn- ptr->addr
  ^long [ptr]
  (.address (dt-ffi/->pointer ptr)))


(defn insert-dataset!
  "Append this dataset using the higher performance append api of duckdb.  This is recommended
  as opposed to using sql statements or prepared statements.  That being said the schema of this
  dataset must match *precisely* the schema of the target table."
  ([conn dataset options]
   (resource/stack-resource-context
    (let [table-name (sql/table-name dataset options)
          appender (dt-struct/new-struct :duckdb-appender {:container-type :native-heap
                                                           :resource-type :stack})
          app-status (duckdb-ffi/duckdb_appender_create conn "" table-name appender)
          ;;this is fine because we are hardcoding to track via stack.
          ;;Normally dispose-fn cannot reference things being tracked
          _ (resource/track appender {:track-type :stack
                                      :dispose-fn #(do #_(println "destroying appender")
                                                       (duckdb-ffi/duckdb_appender_destroy appender))})
          check-error (fn [status]
                        (when-not (= status duckdb-ffi/DuckDBSuccess)
                          (let [err (duckdb-ffi/duckdb_appender_error appender)]
                            (throw (Exception. (str err))))))
          _ (check-error app-status)
          n-rows (ds/row-count dataset)
          n-cols (ds/column-count dataset)
          colvec (vec (ds/columns dataset))
          dtypes (mapv (comp packing/unpack-datatype dt/elemwise-datatype) colvec)
          duckdb-type-ids (mapv dtype-type->duckdb dtypes)
          chunk-size (duckdb-ffi/duckdb_vector_size)
          n-chunks (quot (+ n-rows (dec chunk-size)) chunk-size)
          logical-types (dt-struct/new-array-of-structs :duckdb-logical-type n-cols
                                                        {:container-type :native-heap
                                                         :resource-type :stack})
          _ (resource/stack-resource-context
             (dotimes [cidx n-cols]
               (dt-struct/map->struct! (duckdb-ffi/duckdb_create_logical_type
                                        (duckdb-type-ids cidx))
                                       (logical-types cidx))))
          write-chunk (duckdb-ffi/duckdb_create_data_chunk logical-types n-cols)
          wrap-addr #(native-buffer/wrap-address
                      %1 %2 %3
                      (tech.v3.datatype.protocols/platform-endianness)
                      nil)]
      (try
        (dotimes [chunk n-chunks]
          ;;stack resource context is mainly for the string values.
          (resource/stack-resource-context
           (let [row-offset (* chunk chunk-size)
                 row-count (rem n-rows chunk-size)
                 n-valid (quot (+ row-count 63) 64)
                 string-allocs (ArrayList.)]
             ;;String are tracked in bulk to ease the burden on the resource system.
             (resource/track string-allocs
                             {:track-type :stack
                              :dispose-fn #(.forEach string-allocs
                                                     (reify java.util.function.Consumer
                                                       (accept [this data]
                                                         (native-buffer/free data))))})
             (duckdb-ffi/duckdb_data_chunk_set_size write-chunk row-count)
             (dotimes [col n-cols]
               (let [dvec (duckdb-ffi/duckdb_data_chunk_get_vector write-chunk col)
                     _ (duckdb-ffi/duckdb_vector_ensure_validity_writable dvec)
                     daddr (ptr->addr (duckdb-ffi/duckdb_vector_get_data dvec))
                     validity-data (-> (duckdb-ffi/duckdb_vector_get_validity dvec)
                                       (ptr->addr)
                                       (wrap-addr (* n-valid 8) :int64)
                                       (dt/->buffer))
                     subcol (dt/sub-buffer (colvec col) row-offset row-count)
                     missing (ds/missing subcol)
                     missing-card (dt/ecount missing)
                     col-dt (dtypes col)]
                 (cond
                   (== missing-card row-count)
                   (dt/set-constant! validity-data 0)
                   (== missing-card 0)
                   (dt/set-constant! validity-data -1)
                   :else
                   (do
                     (dt/set-constant! validity-data -1)
                     (let [miter (.getIntIterator ^RoaringBitmap missing)]
                       (loop [continue? (.hasNext miter)]
                         (when continue?
                           (let [ne (-> (.next miter)
                                        (Integer/toUnsignedLong))
                                 vidx (quot ne 64)
                                 cv (.readLong validity-data vidx)
                                 bit-idx (rem ne 64)]
                             (.writeLong validity-data vidx (bit-and-not cv (bit-shift-left 1 bit-idx)))
                             (recur (.hasNext miter))))))))

                 (case col-dt
                   (:int8 :uint8 :boolean) (dt/copy! subcol (wrap-addr daddr row-count (if (= :uint8 col-dt)
                                                                                        :uint8
                                                                                        :int8)))
                   (:int16 :uint16) (dt/copy! subcol (wrap-addr daddr (* 2 row-count) col-dt))
                   (:int32 :uint32 :float32) (dt/copy! subcol (wrap-addr daddr (* 4 row-count) col-dt))
                   (:int64 :uint64 :float64) (dt/copy! subcol (wrap-addr daddr (* 8 row-count) col-dt))
                   (:local-date :packed-local-date) (dt/copy! (if (= col-dt :local-date)
                                                                (packing/pack subcol)
                                                                subcol)
                                                              (wrap-addr daddr (* 4 row-count) :int32))
                   (:local-time :packed-local-time) (dt/copy! (if (= col-dt :local-time)
                                                                (packing/pack subcol)
                                                                subcol)
                                                              (wrap-addr daddr (* 8 row-count) :int64))
                   (:instant :packed-instant) (dt/copy! (if (= col-dt :instant)
                                                          (packing/pack subcol)
                                                          subcol)
                                                        (wrap-addr daddr (* 8 row-count) :int64))
                   ;;We cache strings per-column per-chunk as the translation into duckdb structures is tedious.
                   ;;This is also why we clean up resources per-chunk.
                   (:string :text)
                   (let [stable (hamf/java-hashmap)
                         nbuf (wrap-addr daddr (* 16 row-count) :int8)]
                     (dotimes [idx row-count]
                       (let [sval (str (subcol idx))]
                         (if-let [init-addr (.get stable sval)]
                           (dt/copy! (wrap-addr init-addr 16 :uint8)
                                     (dt/sub-buffer nbuf (* 16 idx) 16))
                           (let [bval (.getBytes sval)
                                 slen (alength bval)
                                 bufoff (* 16 idx)]
                             (.put stable sval (+ daddr bufoff))
                             (native-buffer/write-int nbuf bufoff slen)
                             (if (<= slen 12)
                               (let [bufoff (+ bufoff 4)]
                                 (dt/copy! bval (dt/sub-buffer nbuf bufoff slen)))
                               (let [bufoff (+ bufoff 8)
                                     valbuf (native-buffer/malloc slen {:resource-type nil
                                                                        :uninitialized? true})
                                     _ (.add string-allocs valbuf)
                                     bufaddr (ptr->addr valbuf)]
                                 (dt/copy! bval valbuf)
                                 (native-buffer/write-long nbuf bufoff bufaddr)))))))))))
             (check-error (duckdb-ffi/duckdb_append_data_chunk appender write-chunk))
             (duckdb-ffi/duckdb_data_chunk_reset write-chunk))))
        (finally
          (duckdb-ffi/duckdb_destroy_data_chunk write-chunk)
          (dotimes [cidx n-cols]
            (duckdb-ffi/duckdb_destroy_logical_type (logical-types cidx))))))))
  ([conn dataset] (insert-dataset! conn dataset nil)))


(defn- validity->missing
  "Validity is 64 bit."
  ^RoaringBitmap [^long n-rows ^Pointer nmask]
  (if (or (nil? nmask) (== 0 (.address nmask)))
    (bitmap/->bitmap)
    (let [nvals (quot (+ n-rows 63) 64)
          dvec (-> (native-buffer/wrap-address (.address nmask) (* nvals 8) nil)
                   (native-buffer/set-native-datatype :int64)
                   (dt/->buffer))
          rval (bitmap/->bitmap)]
      (dotimes [idx nvals]
        (let [lval (.readLong dvec idx)]
          (when-not (== lval -1)
            (let [logical-idx (* idx 64)]
              (loop [bit-idx 0]
                (when (< bit-idx 64)
                  (when (== 0 (bit-and lval (bit-shift-left 1 bit-idx)))
                    (.add rval (unchecked-int (+ bit-idx logical-idx))))
                  (recur (unchecked-inc bit-idx))))))))
      rval)))


(defn- coldata->buffer
  [^long n-rows ^long duckdb-type ^long data-ptr]
  (case (get duckdb-ffi/duckdb-type-map duckdb-type)
    :DUCKDB_TYPE_BOOLEAN
    (-> (native-buffer/wrap-address data-ptr n-rows nil)
        (dt/elemwise-cast :boolean))

    :DUCKDB_TYPE_TINYINT
    (native-buffer/wrap-address data-ptr n-rows nil)

    :DUCKDB_TYPE_SMALLINT
    (-> (native-buffer/wrap-address data-ptr (* 2 n-rows) nil)
        (native-buffer/set-native-datatype :int16))

    :DUCKDB_TYPE_INTEGER
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :int32))

    :DUCKDB_TYPE_BIGINT
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :int64))

    :DUCKDB_TYPE_UTINYINT
    (-> (native-buffer/wrap-address data-ptr n-rows nil)
        (native-buffer/set-native-datatype :uint8))

    :DUCKDB_TYPE_USMALLINT
    (-> (native-buffer/wrap-address data-ptr (* 2 n-rows) nil)
        (native-buffer/set-native-datatype :uint16))

    :DUCKDB_TYPE_UINTEGER
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :uint32))

    :DUCKDB_TYPE_UBIGINT
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :uint64))

    :DUCKDB_TYPE_FLOAT
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :float32))

    :DUCKDB_TYPE_DOUBLE
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :float64))

    :DUCKDB_TYPE_DATE
    (-> (native-buffer/wrap-address data-ptr (* 4 n-rows) nil)
        (native-buffer/set-native-datatype :packed-local-date))

    :DUCKDB_TYPE_TIME
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :packed-local-time))

    :DUCKDB_TYPE_TIMESTAMP
    (-> (native-buffer/wrap-address data-ptr (* 8 n-rows) nil)
        (native-buffer/set-native-datatype :packed-instant))

    :DUCKDB_TYPE_VARCHAR
;;     typedef struct {
;; 	union {
;; 		struct {
;; 			uint32_t length;
;; 			char prefix[4];
;; 			char *ptr;
;; 		} pointer;
;; 		struct {
;; 			uint32_t length;
;; 			char inlined[12];
;; 		} inlined;
;; 	} value;
;; } duckdb_string_t;
    (let [string-t-width 16
          inline-len 12
          nbuf (native-buffer/wrap-address data-ptr (* string-t-width n-rows) nil)]
      (dt/make-reader
       :string n-rows
       (let [len-off (* idx string-t-width)
             slen (native-buffer/read-int nbuf len-off)]
         #_(println "reading string at idx" idx len-off slen)
         (if (<= slen inline-len)
           (let [soff (+ len-off 4)]
             (String. (hamf/byte-array (dt/sub-buffer nbuf soff slen))))
           (let [ptr-off (+ len-off 8)
                 ptr-addr (native-buffer/read-long nbuf ptr-off)]
             (String. (hamf/byte-array (native-buffer/wrap-address ptr-addr slen nil))))))))
    (throw (RuntimeException. (format "Failed to get a valid column type for integer type %d" duckdb-type)))))


(defn- supplier-seq
  [^java.util.function.Supplier s]
  (when-let [item (.get s)]
    (cons item (lazy-seq (supplier-seq s)))))


(deftype MappingSupplier [map-fn ^Iterator src-iter immediate-release?]
  java.util.function.Supplier
  (get [this]
    (when (.hasNext src-iter)
      (map-fn (.next src-iter))))
  Seqable
  (seq [this] (supplier-seq this))
  ITypedReduce
  (reduce [this rfn acc]
    (let [iter src-iter]
      (if immediate-release?
        (loop [continue? (.hasNext iter)
               acc acc]
          (if (and continue? (not (reduced? acc)))
            (let [acc (resource/stack-resource-context (rfn acc (map-fn (.next iter))))]
              (recur (.hasNext src-iter) acc))
            (if (reduced? acc) @acc acc)))
        (loop [continue? (.hasNext iter)
               acc acc]
          (if (and continue? (not (reduced? acc)))
            (let [acc (rfn acc (map-fn (.next iter)))]
              (recur (.hasNext src-iter) acc))
            (if (reduced? acc) @acc acc)))))))


(defn- supplier-map
  [options map-fn iable]
  (MappingSupplier. map-fn (.iterator ^Iterable iable) (get options :immediate-release?)))


(defn- results->datasets
  [duckdb-result options]
  (let [metadata {:duckdb-result duckdb-result}
        n-cols (long (duckdb-ffi/duckdb_column_count duckdb-result))
        names (hamf/mapv #(dt-ffi/c->string (duckdb-ffi/duckdb_column_name duckdb-result %)) (hamf/range n-cols))
        types (hamf/mapv #(let [db-type (duckdb-ffi/duckdb_column_logical_type duckdb-result %)
                                ;;complex work-around so we have a pointer to release as the destroy fn takes
                                ;;a ptr and not the thing by copying.
                                type-ptr (dt-ffi/->pointer db-type)]
                            (resource/track db-type {:track-type :auto
                                                     :dispose-fn (fn [] (duckdb-ffi/duckdb_destroy_logical_type type-ptr))})
                            db-type)
                         (hamf/range n-cols))
        type-ids (lznc/map #(duckdb-ffi/duckdb_get_type_id %) types)
        realize-chunk (fn [data-chunk]
                        (try
                          (let [n-rows (duckdb-ffi/duckdb_data_chunk_get_size data-chunk)]
                            (->> (hamf/range n-cols)
                                 (hamf/mapv (fn [cidx]
                                              (let [vdata (duckdb-ffi/duckdb_data_chunk_get_vector data-chunk cidx)
                                                    ^Pointer data-ptr (duckdb-ffi/duckdb_vector_get_data vdata)
                                                    missing (duckdb-ffi/duckdb_vector_get_validity vdata)]
                                                #:tech.v3.dataset {:name (names cidx)
                                                                   :missing (validity->missing n-rows missing)
                                                                   :data (coldata->buffer n-rows
                                                                                          (type-ids cidx)
                                                                                          (.-address data-ptr))
                                                                   ;;skip any further scanning
                                                                   :force-datatype? true})))
                                 (ds/new-dataset options (assoc metadata :data-chunk data-chunk))))))]
    (if (== 0 (long (duckdb-ffi/duckdb_result_is_streaming duckdb-result)))
      (->> (hamf/range (duckdb-ffi/duckdb_result_chunk_count duckdb-result))
           (supplier-map
            options
            (fn [^long cidx]
              (let [chunk (duckdb-ffi/duckdb_result_get_chunk duckdb-result cidx)
                    chunk-ptr (dt-ffi/->pointer chunk)]
                (-> (resource/track chunk {:track-type :auto
                                           :dispose-fn #(duckdb-ffi/duckdb_destroy_data_chunk chunk-ptr)})
                    (realize-chunk))))))
      (throw (RuntimeException. "Streaming results are not supported at this time")))))


(defn sql->datasets
  "Execute a query returning a dataset iterator.  Most data will be read in-place in the result
  set which will be link via metadata to the returned dataset.  If you wish to release
  the data immediately wrap call in `tech.v3.resource/stack-resource-context` and clone
  each result.


Options:

  * `:immediate-release?` - defaults to false - When true the return value supports efficient
  reduction with the caveat the the datasets passed
  into the reduction are released immediately after the reduction fn returns.  This allows
  us to directly `reduce` very large result sets *but* it means that you cannot
  **ever let a non-cloned dataset out of the reduction context**.


Example:


```clojure

  ;; !!Recommended!! - Results copied into jvm and duckdb-result released immediately after query

tmducken.duckdb> (first (resource/stack-resource-context
                  (mapv dt/clone (sql->datasets conn \"select * from stocks\"))))
_unnamed [560 3]:

| symbol |       date | price |
|--------|------------|------:|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
|   MSFT | 2000-06-01 | 32.54 |
|   MSFT | 2000-07-01 | 28.40 |



  ;; Results read in-place, duckdb-result released as some point after dataset falls
  ;; out of scope.  Be extremely careful with this one.

  tmducken.duckdb> (ds/head (sql->dataset conn \"select * from stocks\"))
_unnamed [5 3]:

| symbol |       date | price |
|--------|------------|------:|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
```"
  ([conn sql options]
   (-> (run-query! conn sql options)
       (results->datasets options)))
  ([conn sql]
   (sql->datasets conn sql nil)))


(defn sql->dataset
  "Execute a query returning a single dataset.  This runs the query in a context that releases the memory used
  for the result set before function returns returning a dataset that has no native bindings."
  ([conn sql options]
   (resource/stack-resource-context
    (let [dsdata (vec (sql->datasets conn sql (assoc options :immediate-release? false)))]
      (if (== 1 (count dsdata))
        ;;ensure things get cloned into jvm heap.
        (dt/clone (dsdata 0))
        (apply ds/concat dsdata)))))
  ([conn sql] (sql->dataset conn sql nil)))

(comment
  (do
    (def stocks
      (-> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv" {:key-fn keyword})
          (vary-meta assoc :name :stocks)))
    (initialize!)
    (def db (open-db))
    (def conn (connect db))

    (create-table! conn stocks)
    (insert-dataset! conn stocks))
  (def res (run-query! conn "select * from stocks"))

  (def long-str-ds (ds/->dataset [{:a "one string longer than 12 characters" :b 1}
                                  {:a "another string longer than 12 characters" :b 2}]))

  (create-table! conn long-str-ds)
  (insert-dataset! conn long-str-ds)
  (def res (run-query! conn "select * from _unnamed"))


  )
