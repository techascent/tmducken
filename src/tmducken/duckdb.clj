(ns tmducken.duckdb
  "DuckDB C-level bindings for tech.ml.dataset.

  Current datatype support:

  * boolean, all numeric types int8->int64, uint8->uint64, float32, float64.
  * string
  * uuid
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
            [tech.v3.datatype.datetime.base :as dt-dt-base]
            [tech.v3.datatype.datetime.constants :as dt-dt-constants]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.resource :as resource]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.sql :as sql]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.reduce :as hamf-rf]
            [clojure.tools.logging :as log]
            [clojure.string :as str])
  (:import [java.nio.file Paths]
           [java.util Map Iterator ArrayList UUID]
           [java.util.function Supplier]
           [java.time LocalDate LocalTime Instant]
           [tech.v3.datatype.ffi Pointer]
           [tech.v3.datatype UnsafeUtil]
           [ham_fisted ITypedReduce IFnDef$LO Casts IFnDef]
           [tech.v3.datatype ObjectReader]
           [org.roaringbitmap RoaringBitmap]
           [clojure.lang Seqable IReduceInit Counted IDeref]
           [tech.v3.dataset.impl.column Column]
           [tech.v3.dataset.impl.dataset Dataset]
           [java.lang AutoCloseable]))


(set! *warn-on-reflection* true)


(defonce ^:private initialize* (atom false))

(defn initialized?
  []
  @initialize*)


(defn duckdb-library-version
  ^String []
  (dt-ffi/c->string (duckdb-ffi/duckdb_library_version)))


(defn- check-lib-version!
  []
  (let [lib-version (duckdb-library-version)]
    (when (= lib-version "v0.10.0")
      (throw (RuntimeException. (str "Invalid version: " lib-version " - " "this version of tmducken is meant for duckdb version 0.10.1 and up but should also work with 0.9.2"))))
    :ok))


(defn initialize!
  "Initialize the duckdb ffi system.  This must be called first should be called only once.
  It is safe, however, to call this multiple times.

  Options:

  * `:duckdb-home` - Directory in which to find the duckdb shared library.  Users can pass
  this in.  If not passed in, then the environment variable `DUCKDB_HOME` is checked.  If
  neither is passed in then the library will be searched in the normal system library
  paths."
  ([{:keys [duckdb-home
            lib-instance]}]
   (swap! initialize*
          (fn [is-init?]
            (when-not is-init?
              ;;Precompiled pathways will pass in the instance.
              (if lib-instance
                (dt-ffi/library-singleton-set-instance! duckdb-ffi/lib lib-instance)
                (let [duckdb-home (or duckdb-home
                                      (System/getenv "DUCKDB_HOME")
                                      (str "/" com.sun.jna.Platform/RESOURCE_PREFIX))
                      libpath (if-not (empty? duckdb-home)
                                (str (Paths/get duckdb-home
                                                (into-array String [(System/mapLibraryName "duckdb")])))
                                "duckdb")]
                  (if libpath
                    (log/infof "Attempting to load duckdb from \"%s\"" libpath)
                    (log/info "Attempting to load in-process duckdb"))
                  (duckdb-ffi/define-datatypes!)
                  (dt-ffi/library-singleton-set! duckdb-ffi/lib libpath))))
            (check-lib-version!)
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


(defn run-query!
  "Run a query ignoring the results.  Useful for queries such as single-row insert or update where
  you don't care about the results."
  ([conn sql options]
   (let [query-res (dt-struct/new-struct :duckdb-result {:container-type :native-heap
                                                         :resource-type nil})
         success? (= (duckdb-ffi/duckdb_query conn (str sql) query-res)
                     duckdb-ffi/DuckDBSuccess)
         query-ptr (dt-ffi/->pointer query-res)
         ;;destructor must only be called once and cannot reference query-res as that will
         ;;create a circular references.
         destroy-results* (delay (duckdb-ffi/duckdb_destroy_result query-ptr)
                                 (native-buffer/free (.address query-ptr)))]
     (when-not success?
       (let [error-msg (dt-ffi/c->string (Pointer. (query-res :error-message)))]
         @destroy-results*
         (throw (Exception. error-msg))))
     (deref destroy-results*)
     :ok))
  ([conn sql]
   (run-query! conn sql nil)))


(defn create-table!
  "Create an sql table based off of the column datatypes of the dataset.  Note that users
  can also call [[execute-query!]] with their own sql create-table string.  Note that the
  fastest way to get data into the system is [[insert-dataset!]].

  Options:

  * `:table-name` - Name of the table to create.  If not supplied the dataset name will
     be used.
  * `:primary-key` - sequence of column names to be used as the primary key."
  ([conn dataset options]
   (let [sql (sql/create-sql "duckdb" dataset)]
     (run-query! conn sql)
     (sql/table-name dataset options)))
  ([conn dataset]
   (create-table! conn dataset nil)))


(sql/set-datatype-mapping! "duckdb" :boolean "bool" -7
                           sql/generic-sql->column sql/generic-column->sql)
(sql/set-datatype-mapping! "duckdb" :string "varchar" 12
                           sql/generic-sql->column sql/generic-column->sql)
(sql/set-datatype-mapping! "duckdb" :uuid "UUID" 1111
                           sql/generic-sql->column sql/generic-column->sql)


(defn drop-table!
  [conn dataset]
  (let [ds-name (sql/table-name dataset)]
    (run-query! conn (format "drop table %s" ds-name))
    ds-name))


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
    :string duckdb-ffi/DUCKDB_TYPE_VARCHAR
    :uuid duckdb-ffi/DUCKDB_TYPE_UUID))


(defn- ptr->addr
  ^long [ptr]
  (.address (dt-ffi/->pointer ptr)))


(defn toggle-long-msb
  [x]
  (bit-xor x Long/MIN_VALUE))

(defn insert-dataset!
  "Append this dataset using the higher performance append api of duckdb.  This is recommended
  as opposed to using sql statements or prepared statements.  That being said the schema of this
  dataset must match *precisely* the schema of the target table."
  ([conn dataset options]
   (resource/stack-resource-context
    (let [table-name (sql/table-name dataset options)
          app-ptr-ptr (dt-ffi/make-ptr :pointer 0)
          app-status (duckdb-ffi/duckdb_appender_create conn "" table-name app-ptr-ptr)
          appender (Pointer. (app-ptr-ptr 0))
          ;;this is fine because we are hardcoding to track via stack.
          ;;Normally dispose-fn cannot reference things being tracked
          _ (resource/track appender {:track-type :stack
                                      :dispose-fn #(do #_(println "destroying appender")
                                                       (duckdb-ffi/duckdb_appender_destroy app-ptr-ptr))})
          check-error (fn [status]
                        (when-not (= status duckdb-ffi/DuckDBSuccess)
                          (let [err (duckdb-ffi/duckdb_appender_error appender)]
                            (throw (Exception. (dt-ffi/c->string err))))))
          _ (check-error app-status)
          n-rows (ds/row-count dataset)
          n-cols (ds/column-count dataset)
          colvec (vec (ds/columns dataset))
          dtypes (mapv (comp packing/unpack-datatype dt/elemwise-datatype) colvec)
          duckdb-type-ids (mapv dtype-type->duckdb dtypes)
          chunk-size (duckdb-ffi/duckdb_vector_size)
          n-chunks (quot (+ n-rows (dec chunk-size)) chunk-size)
          logical-types (native-buffer/alloc-zeros :int64 n-cols)
          _ (let [type-buffer (dt/->buffer logical-types)]
              (dotimes [cidx n-cols]
                (.writeLong type-buffer cidx
                            (.address ^Pointer
                                      (duckdb-ffi/duckdb_create_logical_type
                                       (duckdb-type-ids cidx))))))
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
                 row-count (min chunk-size (- n-rows row-offset))
                 n-valid (quot (+ row-count 63) 64)
                 string-allocs (ArrayList.)
                 reduce-groups (ArrayList.)]
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
                   :uuid (let [us (UnsafeUtil/getUnsafe)]
                           (.add reduce-groups
                                 (hamf/pgroups
                                  row-count
                                  (fn [^long sidx ^long eidx]
                                    (let [ne (- eidx sidx)]
                                      (dotimes [idx ne]
                                        (let [laddr (+ (* 16 idx) daddr)
                                              ^UUID uuid (subcol (+ idx sidx))]
                                          (if uuid
                                            (do
                                              (.putLong us laddr (.getLeastSignificantBits uuid))
                                              (.putLong us (+ laddr 8) (toggle-long-msb (.getMostSignificantBits uuid))))
                                            (do
                                              (.putLong us laddr 0)
                                              (.putLong us (+ laddr 8) 0))))))))))
                   ;;We cache strings per-column per-chunk as the translation into duckdb structures is tedious.
                   ;;This is also why we clean up resources per-chunk.
                   (:string :text)
                   (let [stable (hamf/java-concurrent-hashmap)
                         nbuf (wrap-addr daddr (* 16 row-count) :int8)]
                     (.add reduce-groups
                           (hamf/pgroups row-count
                                         (fn [^long sidx ^long eidx]
                                           (let [ne (- eidx sidx)]
                                             (dotimes [idx ne]
                                               (let [idx (+ sidx idx)
                                                     sval (str (subcol idx))]
                                                 (if-let [init-addr (.get stable sval)]
                                                   (UnsafeUtil/copyBytes (long init-addr) (+ daddr (* 16 idx)) 16)
                                                   (let [bval (.getBytes sval)
                                                         slen (alength bval)
                                                         bufoff (* 16 idx)]
                                                     (native-buffer/write-int nbuf bufoff slen)
                                                     (if (<= slen 12)
                                                       (let [bufoff (+ bufoff 4)]
                                                         (UnsafeUtil/copyBytes bval (+ daddr bufoff) slen))
                                                       (let [bufoff (+ bufoff 8)
                                                             valbuf (native-buffer/malloc slen {:resource-type nil
                                                                                                :uninitialized? true})
                                                             _ (locking string-allocs (.add string-allocs valbuf))
                                                             bufaddr (ptr->addr valbuf)]
                                                         (UnsafeUtil/copyBytes bval bufaddr slen)
                                                         (native-buffer/write-long nbuf bufoff bufaddr)))
                                                     (.put stable sval (+ daddr bufoff))))))))))))))
             ;;Force all parallelization requests to finish by now.
             (reduce #(dorun %2) nil reduce-groups)
             (check-error (duckdb-ffi/duckdb_append_data_chunk appender write-chunk))
             (duckdb-ffi/duckdb_data_chunk_reset write-chunk))))
        n-rows
        (finally
          (let [ptrptr (dt-ffi/make-ptr :pointer (.address ^Pointer write-chunk))]
            (duckdb-ffi/duckdb_destroy_data_chunk ptrptr))
          (let [types-addr (.address (dt-ffi/->pointer logical-types))]
            (dotimes [cidx n-cols]
              (duckdb-ffi/duckdb_destroy_logical_type (Pointer. (+ types-addr (* cidx 8)))))))))))
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


(defprotocol ^:private PDelayedClone
  (delayed-clone [this]))

(extend-type Object
  PDelayedClone
  (delayed-clone [this]
    (delay (dt/clone this))))


(deftype ^:private GenericObjReader [dtype cls-type ^IFnDef$LO accessor ^long sidx ^long eidx]
  ObjectReader
  (elemwiseDatatype [this] dtype)
  (lsize [this] (- eidx sidx))
  (readObject [this idx] (.invokePrim accessor (+ sidx idx)))
  (subBuffer [this ssidx seidx]
    (if (and (== sidx ssidx)
             (== eidx seidx))
      this
      (GenericObjReader. dtype cls-type accessor (+ ssidx sidx) (+ sidx seidx))))
  (cloneList [this] (dt/clone this))
  (reduce [this rfn acc]
    (loop [idx sidx
           acc acc]
      (if (and (< idx eidx) (not (reduced? acc)))
        (recur (unchecked-inc idx) (rfn acc (accessor idx)))
        (if (reduced? acc) @acc acc))))
  PDelayedClone
  (delayed-clone [this]
    (let [ne (- eidx sidx)
          ^objects sdata (make-array cls-type ne)
          gfn (fn [^long group-sidx group-eidx]
                (let [group-ne (- group-eidx group-sidx)
                      group-sidx (+ group-sidx sidx)]
                  (dotimes [idx group-ne]
                    (let [lidx (+ idx group-sidx)]
                      (aset sdata lidx (.invokePrim accessor lidx))))))
          group-data (hamf/pgroups ne gfn)]
      (delay
        (dorun group-data)
        (hamf/wrap-array sdata))))
  tech.v3.datatype.protocols/PClone
  (clone [this]
    @(delayed-clone this)))


(deftype ^:private UUIDReader [])


(defn- coldata->buffer
  [^RoaringBitmap missing ^long n-rows ^long duckdb-type ^long data-ptr]
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

    :DUCKDB_TYPE_UUID
    (GenericObjReader.
     :uuid UUID
     (let [us (UnsafeUtil/getUnsafe)]
       (reify IFnDef$LO
         (invokePrim [this idx]
           (let [laddr (+ data-ptr (* idx 16))]
             ;; typedef struct {
             ;;     uint64_t lower;
             ;;     int64_t upper;
             ;; } duckdb_hugeint;
             ;; For some reason the msb when r/w with duckdb api nee to be inverted.
             (let [msbytes (.getLong us (+ laddr 8))
                   msbytes (toggle-long-msb msbytes)
                   lsbytes (.getLong us laddr)]
               (UUID. msbytes lsbytes))))))
     0 n-rows)

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
      (GenericObjReader.
       :string String
       (reify IFnDef$LO
         (invokePrim [this idx]
           (if (.contains missing (unchecked-int idx))
             ""
             (let [len-off (* idx string-t-width)
                   slen (native-buffer/read-int nbuf len-off)]
               #_(println "reading string at idx" idx len-off slen)
               (if (<= slen inline-len)
                 (let [soff (+ len-off 4)]
                   (native-buffer/native-buffer->string nbuf soff slen))
                 (let [ptr-off (+ len-off 8)
                       ptr-addr (native-buffer/read-long nbuf ptr-off)]
                   (native-buffer/native-buffer->string (native-buffer/wrap-address ptr-addr slen nil))))))))
       0 n-rows))
    (throw (RuntimeException. (format "Failed to get a valid column type for integer type %d" duckdb-type)))))


(defn- supplier-seq
  [^Supplier s]
  (when-let [item (.get s)]
    (cons item (lazy-seq (supplier-seq s)))))


(deftype ^:private SupplierIter [^Supplier sup
                                 ^:unsynchronized-mutable val]
  Iterator
  (hasNext [this] (not (nil? val)))
  (next [this]
    (when-not val
      (throw (java.util.NoSuchElementException.)))
    (let [retval val]
      (set! val (.get sup))
      retval)))


(defn- track-chunk
  [chunk]
  (let [addr (.address ^Pointer chunk)]
    (resource/track chunk {:track-type :auto
                           :dispose-fn
                           #(duckdb-ffi/duckdb_destroy_data_chunk (dt-ffi/make-ptr :pointer addr))})))


(defn- destroy-chunk
  [chunk]
  (when (and chunk (not (== 0 (.address ^Pointer chunk))))
    (duckdb-ffi/duckdb_destroy_data_chunk (dt-ffi/make-ptr :pointer (.address ^Pointer chunk)))))


(defn- reduce-chunk
  [chunk realize-chunk reduce-type rf acc]
  (case reduce-type
    :clone
    (let [ds (realize-chunk chunk true)]
      (destroy-chunk chunk)
      (rf acc ds))
    :zero-copy-imm
    (try (rf acc (realize-chunk chunk false))
         (finally
           (destroy-chunk chunk)))
    :zero-copy
    (do
      (track-chunk chunk)
      (rf acc (realize-chunk chunk false)))))


(deftype ^:private RealizedResultChunks [sql
                                         ^long n-elems
                                         ^{:unsynchronized-mutable true
                                           :tag long} idx
                                         result
                                         destroy-result*
                                         realize-chunk
                                         reduce-type]
  java.lang.AutoCloseable
  (close [this] @destroy-result*)
  Supplier
  (get [this] (when (< idx n-elems)
                (let [chunk (duckdb-ffi/duckdb_result_get_chunk result idx)
                      chunk-ds (realize-chunk chunk true)]
                  (destroy-chunk chunk)
                  (set! idx (unchecked-inc idx))
                  chunk-ds)))
  Counted
  (count [this] n-elems)
  ;;Eduction-type pathways use iterable
  Iterable
  (iterator [this] (SupplierIter. this (.get this)))
  ;;zero-copy option for reductions
  ITypedReduce
  (reduce [this rfn acc]
    (let [ne n-elems]
      (loop [lidx idx
             acc acc]
        (if (and (< lidx ne) (not (reduced? acc)))
          (recur (unchecked-inc lidx)
                 (-> (duckdb-ffi/duckdb_result_get_chunk result lidx)
                     (reduce-chunk realize-chunk reduce-type rfn acc)))
          (do
            (set! idx lidx)
            (if (reduced? acc) @acc acc))))))
  ;;Non-chunked sequence pathway.
  Seqable
  (seq [this] (supplier-seq this))
  Object
  (toString [this] (str "#duckdb-realized-result-" n-elems "[\"" sql "\"]")))


(dtype-pp/implement-tostring-print RealizedResultChunks)


(deftype ^:private StreamingResultChunks [sql
                                          result
                                          destroy-result*
                                          realize-chunk
                                          reduce-type]
  java.lang.AutoCloseable
  (close [this] @destroy-result*)
  Supplier
  (get [this] (let [chunk (duckdb-ffi/duckdb_stream_fetch_chunk result)]
                (when (and chunk (not (== 0 (.address ^Pointer chunk))))
                  (let [ds (realize-chunk chunk true)]
                    (destroy-chunk chunk)
                    ds))))
  ITypedReduce
  (reduce [this rfn acc]
    (loop [acc acc]
      (let [chunk (duckdb-ffi/duckdb_stream_fetch_chunk result)]
        (if (and chunk
                 (not (== 0 (.address ^Pointer chunk)))
                 (not (reduced? acc)))
          (recur (reduce-chunk chunk realize-chunk reduce-type rfn acc))
          (do
            (destroy-chunk chunk)
            (if (reduced? acc) @acc acc))))))
  Iterable
  (iterator [this] (SupplierIter. this (.get this)))
  Seqable
  (seq [this] (supplier-seq this))
  Object
  (toString [this] (str "#duckdb-streaming-result"  "[\"" sql "\"]")))


(dtype-pp/implement-tostring-print StreamingResultChunks)


(defn- results->datasets
  ^AutoCloseable [sql duckdb-result options]
  (let [metadata {:duckdb-result duckdb-result}
        n-cols (long (duckdb-ffi/duckdb_column_count duckdb-result))
        names (hamf/mapv #(dt-ffi/c->string (duckdb-ffi/duckdb_column_name duckdb-result %)) (hamf/range n-cols))
        type-ids (hamf/mapv #(let [db-type (duckdb-ffi/duckdb_column_logical_type duckdb-result %)
                                   ;;complex work-around so we have a pointer to release as the destroy fn takes
                                   ;;a ptr and not the thing by copying.
                                   retval (duckdb-ffi/duckdb_get_type_id db-type)]
                               (-> (dt-ffi/make-ptr :pointer (.address ^Pointer db-type))
                                   (duckdb-ffi/duckdb_destroy_logical_type))
                               retval)
                            (hamf/range n-cols))
        ;;This function gets called a *lot*
        realize-chunk (fn [data-chunk clone?]
                        (let [n-rows (duckdb-ffi/duckdb_data_chunk_get_size data-chunk)
                              colmap (hamf/mut-map)
                              key-fn (get options :key-fn identity)
                              ;;Columns are processed in two stages to make sure we use all parallelism
                              ;;available if we have to do a nontrivial clone op such as copying strings
                              ;;into the jvm.  Since the chunksize is fairly small we attempt to launch all
                              ;;parallel clone ops before we force any of them to complete.
                              columns
                              (->> (hamf/range n-cols)
                                   (hamf/mapv (fn [cidx]
                                                (let [vdata (duckdb-ffi/duckdb_data_chunk_get_vector data-chunk cidx)
                                                      ^Pointer data-ptr (duckdb-ffi/duckdb_vector_get_data vdata)
                                                      missing (validity->missing
                                                               n-rows
                                                               (duckdb-ffi/duckdb_vector_get_validity vdata))
                                                      coldata (try
                                                                (coldata->buffer missing
                                                                                 n-rows
                                                                                 (type-ids cidx)
                                                                                 (.-address data-ptr))
                                                                (catch Exception e
                                                                  (throw (RuntimeException.
                                                                          (str "Error processing column " (names cidx)) e))))
                                                      cname (key-fn (names cidx))
                                                      delayed-data (if clone?
                                                                     (delayed-clone coldata)
                                                                     (delay coldata))]
                                                  (.put colmap cname cidx)
                                                  (delay
                                                    (Column. missing
                                                             (deref delayed-data)
                                                             {:name cname}
                                                             nil))))))]
                          (Dataset. (hamf/mapv deref columns)
                                    (persistent! colmap)
                                    {:data-chunk data-chunk
                                     :name :_unnamed}
                                    0 0)))
        reduce-type (get options :reduce-type :clone)]
    (if (== 0 (long (duckdb-ffi/duckdb_result_is_streaming duckdb-result)))
      (RealizedResultChunks. sql
                             (duckdb-ffi/duckdb_result_chunk_count duckdb-result)
                             0
                             duckdb-result
                             (delay nil)
                             realize-chunk
                             reduce-type)
      (StreamingResultChunks. sql
                              duckdb-result
                              (delay nil)
                              realize-chunk
                              reduce-type))))

(declare prepare)


(defn sql->datasets
  "Execute a query returning either a sequence of datasets or a single dataset.

  See documentation and options for [[prepare]].

  Examples:

```clojure
tmducken.duckdb> (first (sql->datasets conn \"select * from stocks\"))
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
  (^AutoCloseable [conn sql options]
   (with-open [^AutoCloseable stmt (prepare conn sql options)]
     (stmt)))
  (^AutoCloseable [conn sql]
   (sql->datasets conn sql nil)))



(defn sql->dataset
  "Execute a query returning a single dataset.  This runs the query in a context that releases the memory used
  for the result set before function returns returning a dataset that has no native bindings."
  ([conn sql options]
   (sql->datasets conn sql (assoc options :result-type :single)))
  ([conn sql] (sql->dataset conn sql nil)))


(defn- bind-prepare-param
  [stmt idx v]
  ;;type-id appears unreliable at this timee
  #_(let [errcode
        (if (nil? v)
          (duckdb-ffi/duckdb_bind_null stmt idx (if (Casts/booleanCast v) 1 0))
          (case type-id
            duckdb-ffi/DUCKDB_TYPE_BOOLEAN (duckdb-ffi/duckdb_bind_boolean stmt idx (if (Casts/booleanCast v) 1 0))
            duckdb-ffi/DUCKDB_TYPE_TINYINT (duckdb-ffi/duckdb_bind_int8 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_SMALLINT (duckdb-ffi/duckdb_bind_int16 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_INTEGER (duckdb-ffi/duckdb_bind_int32 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_BIGINT (duckdb-ffi/duckdb_bind_int64 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_UTINYINT (duckdb-ffi/duckdb_bind_uint8 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_USMALLINT (duckdb-ffi/duckdb_bind_uint16 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_UINTEGER (duckdb-ffi/duckdb_bind_uint32 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_UBIGINT (duckdb-ffi/duckdb_bind_uint64 stmt idx (Casts/longCast v))
            duckdb-ffi/DUCKDB_TYPE_FLOAT (duckdb-ffi/duckdb_bind_float stmt idx (Casts/doubleCast v))
            duckdb-ffi/DUCKDB_TYPE_DATE (duckdb-ffi/duckdb_bind_date stmt idx (if (number? v)
                                                                                (Casts/longCast v)
                                                                                (.toEpochDay ^LocalDate v)))
            duckdb-ffi/DUCKDB_TYPE_TIME (duckdb-ffi/duckdb_bind_time
                                         stmt idx (if (number? v)
                                                    (Casts/longCast v)
                                                    (quot (.toNanoOfDay ^LocalTime v)
                                                          dt-dt-constants/nanoseconds-in-microsecond)))
            duckdb-ffi/DUCKDB_TYPE_TIMESTAMP (duckdb-ffi/duckdb_bind_double
                                              stmt idx (if (number? v)
                                                         (Casts/longCast v)
                                                         (dt-dt-base/instant->microseconds-since-epoch v)))
            duckdb-ffi/DUCKDB_TYPE_VARCHAR
            (let [strval (str v)
                  len (.length strval)]
              (duckdb-ffi/duckdb_bind_varchar_length stmt idx strval len))))]
    (when-not (== 0 (long errcode))
      (let [errptr (duckdb-ffi/duckdb_prepare_error stmt)
            errstr (if errptr
                     (dt-ffi/c->string errptr)
                     "Unknown Error")]
        (throw (RuntimeException. (str "Failed to set param " idx ":" errstr))))))
  (cond
    (nil? v)
    (duckdb-ffi/duckdb_bind_null stmt idx)
    (integer? v)
    (duckdb-ffi/duckdb_bind_int64 stmt idx (Casts/longCast v))
    (or (float? v) (double? v))
    (duckdb-ffi/duckdb_bind_double stmt idx (double v))
    (string? v)
    (let [strval (str v)
          len (.length strval)]
      (duckdb-ffi/duckdb_bind_varchar_length stmt idx (dt-ffi/string->c strval) len))
    (instance? LocalDate v)
    (duckdb-ffi/duckdb_bind_date stmt idx (.toEpochDay ^LocalDate v))
    (instance? LocalTime v)
    (duckdb-ffi/duckdb_bind_time
     stmt idx (quot (.toNanoOfDay ^LocalTime v)
                    dt-dt-constants/nanoseconds-in-microsecond))
    (instance? Instant v)
    (duckdb-ffi/duckdb_bind_timestamp
     stmt idx (dt-dt-base/instant->microseconds-since-epoch v))
    :else
    (throw (RuntimeException. (str "Unable to discern binding type for value: " v)))))


(defn- -assert-n-args!
  "Assert that the `actual-n-args` matches the `n-args-desired` or throw an error if it
  does not."
  [n-args-desired actual-n-args]
  (when-not (= actual-n-args n-args-desired)
    (throw (RuntimeException.
             (format "Prepared statement defined for %d parameters -- %d given"
                     n-args-desired actual-n-args)))))

;;Deftype so we can overload the tostring method
(deftype ^:private PrepStatement [sql stmt n-params finalize-stmt]
  Object
  (toString [this]
    (str "#duckdb-prepared-statement-" n-params " [\"" sql "\"]"))
  IFnDef
  (invoke [this]
    (-assert-n-args! n-params 0)
    (finalize-stmt))
  (invoke [this v0]
    (-assert-n-args! n-params 1)
    (bind-prepare-param stmt 1 v0)
    (finalize-stmt))
  (invoke [this v0 v1]
    (-assert-n-args! n-params 2)
    (bind-prepare-param stmt 1 v0)
    (bind-prepare-param stmt 2 v1)
    (finalize-stmt))
  (invoke [this v0 v1 v2]
    (-assert-n-args! n-params 3)
    (bind-prepare-param stmt 1 v0)
    (bind-prepare-param stmt 2 v1)
    (bind-prepare-param stmt 3 v2)
    (finalize-stmt))
  (invoke [this v0 v1 v2 v3]
    (-assert-n-args! n-params 4)
    (bind-prepare-param stmt 1 v0)
    (bind-prepare-param stmt 2 v1)
    (bind-prepare-param stmt 3 v2)
    (bind-prepare-param stmt 4 v3)
    (finalize-stmt))
  (invoke [this v0 v1 v2 v3 v4]
    (-assert-n-args! n-params 5)
    (bind-prepare-param stmt 1 v0)
    (bind-prepare-param stmt 2 v1)
    (bind-prepare-param stmt 3 v2)
    (bind-prepare-param stmt 4 v3)
    (finalize-stmt))
  (applyTo [this args]
    (-assert-n-args! n-params (count args))
    (reduce (hamf-rf/indexed-accum
              acc idx v
              (bind-prepare-param stmt (inc idx) v))
            nil
            args)
    (finalize-stmt))
  AutoCloseable
  (close [this] nil))


(dtype-pp/implement-tostring-print PrepStatement)
(defn- datasets->dataset
  "Given a sequence of results return a single dataset.  This pathway relies on reduce-type being anything
  other than `:zero-copy-imm`.  It is designed for and is most efficient when used with `:zero-copy`."
  [^AutoCloseable results]
  (resource/stack-resource-context
   (let [dsdata (vec results)]
     (if (== 1 (count dsdata))
       ;;ensure things get cloned into jvm heap.
       (dt/clone (dsdata 0))
       (apply ds/concat dsdata)))))

(defn- ->error
  "Helper function to create an error from a potentially variadic number of err-ptrs."
  [msg & err-ptrs]
  (RuntimeException.
    (str (when msg (str msg ":\n"))
         (let [sub-msg (clojure.string/join
                         " - "
                         (for [err-ptr err-ptrs
                               :when   (some? err-ptr)]
                           (dt-ffi/c->string err-ptr)))]
           (if (seq sub-msg)
             sub-msg
             "Unknown Error")))))

(defn- -execute-statement!
  "Execute the statement at `stmt-ix` using the provided `conn`, extracted `stmts` and
  `stmt` pointer.

  This function is mostly used to handle the ceremony around extracting and preparing statements and
  the associated error handling.

  If an error is encountered all resources created up to the point will be cleaned up.

  There are also options to allow the sequence of steps to be skipped allowing callers to
  effectively \"pause\" and \"resume\"

  The steps are as follows:

  1) Prepare a statement (can be skipped with `skip-prepare?`) by loading `stmt-ix` from `stmts`
     into `stmt`. Can be skipped using `:skip-prepare?`

  2) Execute the `stmt` by loading it into `pending-result` and finally realize the result into
     `out-result`. Can be skipped using `:skip-execute?`

  3) Destroy per-statement resources (`pending-result` and `extracted-statement`). Can be skipped
     using `:skip-cleanup?`.

  Takes options for"
  [{:keys [conn stmts stmt-ix stmt streaming?
           pending-result out-result
           skip-prepare?
           skip-execute?
           skip-cleanup?]}]
  (let [prepare-err     (when-not skip-prepare?
                          (when-not (zero? (duckdb-ffi/duckdb_prepare_extracted_statement
                                             conn stmts stmt-ix stmt))
                            (->error
                              "Error preparing statement"
                              (duckdb-ffi/duckdb_prepare_error stmt))))
        create-pending* (delay
                          (if skip-execute?
                            0
                            (if streaming?
                              (duckdb-ffi/duckdb_pending_prepared_streaming stmt pending-result)
                              (duckdb-ffi/duckdb_pending_prepared stmt pending-result))))
        pending-err     (when (and (nil? prepare-err)
                                   (not (zero? @create-pending*)))
                          (->error
                            "Error executing prepared statement"
                            (duckdb-ffi/duckdb_pending_error pending-result)))
        realize-err     (when (and (nil? pending-err)
                                   (not skip-execute?)
                                   (not (zero? (duckdb-ffi/duckdb_execute_pending
                                                 pending-result
                                                 out-result))))
                      (->error "Error realizing pending result"
                               (duckdb-ffi/duckdb_pending_error pending-result)))
        err             (or prepare-err pending-err realize-err)]
    (when-not skip-cleanup?
      (duckdb-ffi/duckdb_destroy_pending pending-result)
      (duckdb-ffi/duckdb_destroy_prepare stmt))
    (when err
      (duckdb-ffi/duckdb_destroy_extracted stmts)
      (when realize-err
        (duckdb-ffi/duckdb_destroy_result out-result))
      (throw err))))


(defn- -new-struct
  "Helper function to make a new struct on the native heap for use with Duckdb.

  If provided a cleanup function we will automatically attach it to the resource and call it with
  the pointer

  If provided pinned resources we will block their release until after GC has run
  for the provided `struct`."
  [struct-kw & {:keys [cleanup-fn
                       pinned-resources]}]
   (let [struct (dt-struct/new-struct struct-kw {:container-type :native-heap :resource-type :auto})
         ;; Use `struct-ptr` to avoid keeping a reference to `struct` in the dispose-fn closure below.
         struct-ptr (dt-ffi/->pointer struct)]
     (when cleanup-fn
       (resource/track struct
                       {:track-type :auto
                        :dispose-fn #(do (count pinned-resources)
                                         (cleanup-fn struct-ptr))})
       struct)))


(defn prepare
  "Create a prepared statement returning a clojure function you can call taking args specified
  in the prepared statement.  This function is auto-closeable which releases the prepared statement - it is also
  registered with the resource system so it will be released when it is no longer reachable by the gc system.

  The function return value can be either a sequence of datasets or a single dataset.  For `:streaming`, the sequence
  is read from the result and has no count.  For `:realized` the sequence is of known length and the result
  is completely realized before the first dataset is read.  Finally you can have `single` which means
  the system will return a single dataset.  The default is `:streaming`.

  In the cases where a sequence is returned, the object returned is auto-closeable result object itself will
  be destroyed when the object is closed or when it is no longer reachable.

  In general datasets are copied into the JVM on a chunk-by-chunk basis.  If the user simply desires to reduce over
  the return value the datasets can be zero-copied during the reduction with an option to immediately release
  each dataset.  It is extremely quick to clone the dataset into jvm heap storage, however, so please stick with
  the defaults - which are safe and memory efficient - unless you have a very good reason to change them.

  Options are passed through to dataset creation.

  Options:
  * `:result-type` - one of `#{:streaming :realized :single}` - defaulting to `:streaming`.
     - `:streaming` - uncountable supplier/sequence of datasets - auto-closeable.
     - `:realized` - all results realized, countable supplier/sequence of datasets - auto-closeable.
     - `:single` - results realized into a single dataset with chunks and result being immediately released.
  * `:reduce-type - One of #{:clone :zero-copy-imm :zero-copy}` defaulting to `:clone`.  - When the result is
     reduced the dataset is initially read via zero-copy directly from the result batch.  Then one of three things happen:
     - `:clone` - dataset cloned and batch released just before rf - safest option and default.
     - `:zero-copy-imm` - rf called with zero-copy dataset and batch released just after.  This is very memory and cpu efficient
       but you need to ensure that no part of the dataset escapes rf.
     - `:zero-copy` - Datasets are merely passed to rf and batch is not released but is registered with the resource system.  This
     is used to efficiently concatenate the results into one dataset after which all batches are released.


  Examples:

```clojure
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
```"
  (^AutoCloseable [conn sql] (prepare conn sql nil))
  (^AutoCloseable [conn sql options]
   (let [stmts (-new-struct :duckdb-extracted-statements
                            {:cleanup-fn duckdb-ffi/duckdb_destroy_extracted})
         stmt (-new-struct :duckdb-prepared-statement
                           {:cleanup-fn duckdb-ffi/duckdb_destroy_prepare
                            :pinned-resources [stmts]})
         pending (-new-struct :duckdb-pending-result
                              {:cleanup-fn duckdb-ffi/duckdb_destroy_pending
                               :pinned-resources [stmt]})
         result (-new-struct :duckdb-result
                             {:cleanup-fn duckdb-ffi/duckdb_destroy_result})
         n-stmts (let [result (duckdb-ffi/duckdb_extract_statements conn sql stmts)]
                   (if (pos? result)
                     result
                     (throw (->error "Error extracting statements"
                                     (duckdb-ffi/duckdb_extract_statements_error stmts)))))
         n-params (loop [ix 0]
                    (let [last-stmt? (= ix (dec n-stmts))]
                      (-execute-statement! {:conn           conn
                                            :stmts          stmts
                                            :stmt           stmt
                                            :stmt-ix        ix
                                            :pending-result pending
                                            :out-result     result
                                            :skip-cleanup?  last-stmt?
                                            :skip-execute?  last-stmt?})
                      (when-not last-stmt?
                        (duckdb-ffi/duckdb_destroy_result result))
                      (if last-stmt?
                        (duckdb-ffi/duckdb_nparams stmt)
                        (recur (inc ix)))))
         result-type (get options :result-type :streaming)
         options     (if (identical? result-type :single)
                       (assoc options :reduce-type :zero-copy)
                       options)
         finalize-stmt (fn []
                         (-execute-statement!
                           {:conn           conn
                            :stmts          stmts
                            :stmt           stmt
                            :stmt-ix        (dec n-stmts)
                            :pending-result pending
                            :out-result     result
                            :streaming?     (identical? result-type :streaming)
                            :skip-prepare?  true})
                         (let [res-data (results->datasets sql result options)]
                           (case result-type
                             :streaming res-data
                             :realized  res-data
                             :single    (with-open [res-data res-data]
                                       (datasets->dataset res-data)))))]
     (PrepStatement. sql stmt n-params finalize-stmt))))


(comment
  (do
    (def stocks
      (-> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"
                        {:key-fn       keyword
                         :dataset-name :stocks})))

    (initialize!)

    (def db (open-db))
    (def conn (connect db))

    (create-table! conn stocks)
    (insert-dataset! conn stocks))

  (def long-str-ds (ds/->dataset [{:a "one string longer than 12 characters" :b 1}
                                  {:a "another string longer than 12 characters" :b 2}]))

  (create-table! conn long-str-ds)
  (insert-dataset! conn long-str-ds)


  )
