(ns tmducken.duckdb
  (:require [tmducken.duckdb.ffi :as duckdb-ffi]
            [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.struct :as dt-struct]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.resource :as resource]
            [tech.v3.dataset.sql.impl :as sql-impl]
            [tech.v3.dataset :as ds]
            [clojure.tools.logging :as log])
  (:import [java.nio.file Paths]
           [tech.v3.datatype.ffi Pointer]))


(defonce ^:private initialize* (atom false))

(defn initialized?
  []
  @initialize*)


(defn initialize!
  "Initialize the duckdb ffi system.  This must be called first should be called only once.
  It is safe, however, to call this multiple times."
  ([{:keys [duckdb-home in-process?]}]
   (swap! initialize*
          (fn [is-init?]
            (when-not is-init?
              (let [duckdb-home (or duckdb-home (System/getenv "DUCKDB_HOME"))
                    libpath (when-not in-process?
                              (if-not (empty? duckdb-home)
                                (str (Paths/get duckdb-home
                                                (into-array String [(System/mapLibraryName "duckdb")])))
                                "duckdb"))]
                (if libpath
                  (log/infof "Attempting to load duckdb from \"%s\"" libpath)
                  (log/infof "Attempting to load in-process duckdb" libpath))
                (dt-ffi/library-singleton-set! duckdb-ffi/lib libpath)
                (duckdb-ffi/define-datatypes!)))
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
   (run-query conn sql nil)))


(defn create-table!
  ([conn dataset options]
   (let [table-name (sql-impl/->str (or (:table-name options)
                                        (sql-impl/dataset->table-name dataset)))
         primary-key (or (:primary-key options)
                         (:primary-key (meta dataset)))
         primary-key (when primary-key
                       (cond
                         (string? primary-key) [primary-key]
                         (seq primary-key) primary-key
                         :else [primary-key]))
         sql (sql-impl/create-sql dataset table-name primary-key)]
     (resource/stack-resource-context
      (run-query! conn sql))
     :ok))
  ([conn dataset]
   (create-table! conn dataset nil)))


(defn append-dataset!
  "Append this dataset using the higher-performance append api of duckdb.  This is recommended
  as opposed to using sql statements or prepared statements."
  ([conn dataset options]
   )
  ([conn dataset] (append-dataset! conn dataset nil)))


(comment
  (def stocks
    (-> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv" {:key-fn keyword})
        (vary-meta assoc :name :stocks)))

  (def db (open-db))
  (def conn (connect db))

  (create-table! conn stocks)
  )
