(ns tmducken.duckdb.ffi
  (:require [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.struct :as dt-struct]
            [clojure.tools.logging :as log]
            [tech.v3.resource :as resource])
  (:import [java.nio.file Paths]
           [tech.v3.datatype.ffi Pointer]))



(def ^{:tag 'long} DUCKDB_TYPE_INVALID 0)
(def ^{:tag 'long} DUCKDB_TYPE_BOOLEAN 1)
(def ^{:tag 'long} DUCKDB_TYPE_TINYINT 2)
(def ^{:tag 'long} DUCKDB_TYPE_SMALLINT 3)
(def ^{:tag 'long} DUCKDB_TYPE_INTEGER 4)
(def ^{:tag 'long} DUCKDB_TYPE_BIGINT 5)
(def ^{:tag 'long} DUCKDB_TYPE_UTINYINT 6)
(def ^{:tag 'long} DUCKDB_TYPE_USMALLINT 7)
(def ^{:tag 'long} DUCKDB_TYPE_UINTEGER 8)
(def ^{:tag 'long} DUCKDB_TYPE_UBIGINT 9)
(def ^{:tag 'long} DUCKDB_TYPE_FLOAT 10)
(def ^{:tag 'long} DUCKDB_TYPE_DOUBLE 11)
(def ^{:tag 'long} DUCKDB_TYPE_TIMESTAMP 12)
(def ^{:tag 'long} DUCKDB_TYPE_DATE 13)
(def ^{:tag 'long} DUCKDB_TYPE_TIME 14)
(def ^{:tag 'long} DUCKDB_TYPE_INTERVAL 15)
(def ^{:tag 'long} DUCKDB_TYPE_HUGEINT 16)
(def ^{:tag 'long} DUCKDB_TYPE_VARCHAR 17)
(def ^{:tag 'long} DUCKDB_TYPE_BLOB 18)


(def ^{:tag 'long} DuckDBSuccess 0)
(def ^{:tag 'long} DuckDBError 1)


(dt-ffi/define-library!
  lib
  '{
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Database setup/teardown
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    :duckdb_malloc {:rettype :pointer
                    :argtypes [[size :size-t]]}
    :duckdb_free {:rettype :void
                  :argtypes [[ptr :pointer]]}

    :duckdb_open {:rettype :int32
                  :argtypes [[path :string]
                             [out_database :pointer]]
                  :doc "Creates a new database or opens an existing database file stored at the the given path.
If no path is given a new in-memory database is created instead.

* path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
* out_database: The result database object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure."}
    :duckdb_open_ext {:rettype :int32
                      :argtypes [[path :string]
                                 [out_datatypes :pointer]
                                 [config :pointer?]
                                 [out_error :pointer]]}
    :duckdb_close {:rettype :void
                   :argtypes [[database :pointer]]
                   :doc "Closes the specified database and de-allocates all memory allocated for that database.
This should be called after you are done with any database allocated through `duckdb_open`.
Note that failing to call `duckdb_close` (in case of e.g. a program crash) will not cause data corruption.
Still it is recommended to always correctly close a database object after you are done with it.

* database: The database object to shut down."}
    :duckdb_connect {:rettype :int32
                     :argtypes [[database :pointer]
                                [out_connection :pointer]]
                     :doc "Opens a connection to a database. Connections are required to query the database, and store transactional state
associated with the connection.

* database: The database file to connect to.
* out_connection: The result connection object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure."}
    :duckdb_disconnect {:rettype :void
                        :argtypes [[connection :pointer]]
                        :doc "Closes the specified connection and de-allocates all memory allocated for that connection.

* connection: The connection to close."}
    :duckdb_config_count {:rettype :size-t
                          :doc "This returns the total amount of configuration options available for usage with `duckdb_get_config_flag`.

This should not be called in a loop as it internally loops over all the options.

* returns: The amount of config options available."}
    :duckdb_create_config {:rettype :int32
                           :argtypes [[out_config :pointer]]
                           :doc "Initializes an empty configuration object that can be used to provide start-up options for the DuckDB instance
through `duckdb_open_ext`.

This will always succeed unless there is a malloc failure.

* out_config: The result configuration object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure."}
    :duckdb_get_config_flag {:rettype :int32
                             :argtypes [[index :size-t]
                                        [out_name :pointer?]
                                        [out_description :pointer?]]
                             :doc "Obtains a human-readable name and description of a specific configuration option. This can be used to e.g.
display configuration options. This will succeed unless `index` is out of range (i.e. `>= duckdb_config_count`).

The result name or description MUST NOT be freed.

* index: The index of the configuration option (between 0 and `duckdb_config_count`)
* out_name: A name of the configuration flag.
* out_description: A description of the configuration flag.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure."}
    :duckdb_set_config {:rettype :int32
                        :argtypes [[config :pointer]
                                   [name :string]
                                   [option :string]]
                        :doc "Sets the specified option for the specified configuration. The configuration option is indicated by name.
To obtain a list of config options, see `duckdb_get_config_flag`.

In the source code, configuration options are defined in `config.cpp`.

This can fail if either the name is invalid, or if the value provided for the option is invalid.

* duckdb_config: The configuration object to set the option on.
* name: The name of the configuration flag to set.
* option: The value to set the configuration flag to.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure."}
    :duckdb_destroy_config {:rettype :void
                            :argtypes [[config :pointer]]
                            :doc "Destroys the specified configuration option and de-allocates all memory allocated for the object.

* config: The configuration object to destroy."}


    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Generic bulk data upload - the appender interface
    ;;
    ;; Appenders are the most efficient way of loading data into DuckDB from within the
    ;; C interface, and are recommended for fast data loading. The appender is much faster
    ;; than using prepared statements or individual `INSERT INTO` statements.

    ;; Appends are made in row-wise format. For every column, a `duckdb_append_[type]` call
    ;; should be made, after which the row should be finished by calling
    ;; `duckdb_appender_end_row`. After all rows have been appended,
    ;; `duckdb_appender_destroy` should be used to finalize the appender and clean up the
    ;; resulting memory.

    ;; Note that `duckdb_appender_destroy` should always be called on the resulting
    ;; appender, even if the function returns
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    :duckdb_appender_create {:rettype :int32
                             :argtypes [[connection :pointer]
                                        [schema :string]
                                        [table :string]
                                        [out_appender :pointer]]
                             :doc "Returns the error message associated with the given appender.
If the appender has no error message, this returns `nullptr` instead.

The error message should not be freed. It will be de-allocated when `duckdb_appender_destroy` is called.

* appender: The appender to get the error from.
* returns: The error message, or `nullptr` if there is none."}
    :duckdb_appender_error {:rettype :string
                            :argtypes [[appender :pointer]]
                            :doc "Returns the error message associated with the given appender.
If the appender has no error message, this returns `nullptr` instead.

The error message should not be freed. It will be de-allocated when `duckdb_appender_destroy` is called.

* appender: The appender to get the error from.
* returns: The error message, or `nullptr` if there is none."}
    :duckdb_appender_destroy {:rettype :int32
                              :argtypes [[appender :pointer]]
                              :doc "Close the appender and destroy it. Flushing all intermediate state in the appender to the table, and de-allocating
all memory associated with the appender.

* appender: The appender to flush, close and destroy.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure."}

    :duckdb_appender_end_row {:rettype :int32
                              :argtypes [[appender :pointer]]
                              :doc "End the current row."}

    :duckdb_append_bool {:rettype :int32
                         :argtypes [[appender :pointer]
                                    [value :int8]]}

    :duckdb_append_int8 {:rettype :int32
                         :argtypes [[appender :pointer]
                                    [value :int8]]}
    :duckdb_append_int16 {:rettype :int32
                          :argtypes [[appender :pointer]
                                     [value :int16]]}
    :duckdb_append_int32 {:rettype :int32
                          :argtypes [[appender :pointer]
                                     [value :int32]]}
    :duckdb_append_int64 {:rettype :int32
                          :argtypes [[appender :pointer]
                                     [value :int64]]}

    ;; potentially can't work without pass-by-value support.
    :duckdb_append_hugeint {:rettype :int32
                            :argtypes [[appender :pointer]
                                       [lower :int64]
                                       [upper :int64]]}
    :duckdb_append_uint8 {:rettype :int32
                          :argtypes [[appender :pointer]
                                     [value :int8]]}
    :duckdb_append_uint16 {:rettype :int32
                           :argtypes [[appender :pointer]
                                      [value :int16]]}
    :duckdb_append_uint32 {:rettype :int32
                           :argtypes [[appender :pointer]
                                      [value :int32]]}
    :duckdb_append_uint64 {:rettype :int32
                           :argtypes [[appender :pointer]
                                      [value :int64]]}
    :duckdb_append_float {:rettype :int32
                          :argtypes [[appender :pointer]
                                     [value :float32]]}
    :duckdb_append_double {:rettype :int32
                           :argtypes [[appender :pointer]
                                      [value :float64]]}
    :duckdb_append_date {:rettype :int32
                         :argtypes [[appender :pointer]
                                    ;;epoch days
                                    [value :int32]]}
    :duckdb_append_time {:rettype :int32
                         :argtypes [[appender :pointer]
                                    ;;microseconds since 00:00:00
                                    [value :int64]]}
    :duckdb_append_timestamp {:rettype :int32
                              :argtypes [[appender :pointer]
                                         ;;microseconds since 2970-01-01
                                         [value :int64]]}

    ;; Not sure we can support this without pass-by-value support
    ;; :duckdb_append_interval {:rettype :int32
    ;;                          :argtypes [[appender :pointer]
    ;;                                     [duckdb_interval value]]}

    :duckdb_append_varchar {:rettype :int32
                            :argtypes [[appender :pointer]
                                       [value :string]]}
    :duckdb_append_varchar_length {:rettype :int32
                                   :argtypes [[appender :pointer]
                                              [value :pointer]
                                              [length :int64]]}
    :duckdb_append_blob {:rettype :int32
                         :argtypes [[appender :pointer]
                                    [data :pointer]
                                    [length :int64]]}
    :duckdb_append_null {:rettype :int32
                         :argtypes [[appender :pointer]]}


    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Query Execution
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    DUCKDB_API duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result)
    DUCKDB_API void duckdb_destroy_result(duckdb_result *result)
    DUCKDB_API const char *duckdb_column_name(duckdb_result *result, idx_t col)
    DUCKDB_API idx_t duckdb_column_count(duckdb_result *result)
    DUCKDB_API idx_t duckdb_row_count(duckdb_result *result)
    DUCKDB_API idx_t duckdb_rows_changed(duckdb_result *result)
    DUCKDB_API void *duckdb_column_data(duckdb_result *result, idx_t col)
    DUCKDB_API char *duckdb_result_error(duckdb_result *result)


    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Prepared Statements
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    DUCKDB_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                                                             duckdb_prepared_statement *out_prepared_statement)
    DUCKDB_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement)
    DUCKDB_API const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement)
    DUCKDB_API idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement)
    DUCKDB_API duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx)
    DUCKDB_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val)
    DUCKDB_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val)
    DUCKDB_API duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val)
    DUCKDB_API duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val)
    DUCKDB_API duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val)
    DUCKDB_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                          duckdb_hugeint val)
    DUCKDB_API duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val)
    DUCKDB_API duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val)
    DUCKDB_API duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val)
    DUCKDB_API duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val)
    DUCKDB_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val)
    DUCKDB_API duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val)
    DUCKDB_API duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                       duckdb_date val)
    DUCKDB_API duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                       duckdb_time val)
    DUCKDB_API duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                            duckdb_timestamp val)
    DUCKDB_API duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                           duckdb_interval val)
    DUCKDB_API duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                          const char *val)
    DUCKDB_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                                 const char *val, idx_t length)
    DUCKDB_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                                       const void *data, idx_t length)
    DUCKDB_API duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx)
    DUCKDB_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement,
                                                                              duckdb_result *out_result)
    }

  nil
  nil)


(defn initialize!
  ([{:keys [duckdb-home in-process?]}]
   (let [duckdb-home (or duckdb-home (System/getenv "DUCKDB_HOME"))
         libpath (when-not in-process?
                   (if-not (empty? duckdb-home)
                     (str (Paths/get duckdb-home
                                     (into-array String [(System/mapLibraryName "duckdb")])))
                     "duckdb"))]
     (if libpath
       (log/infof "Attempting to load duckdb from \"%s\"" libpath)
       (log/infof "Attempting to load in-process duckdb" libpath))
     (dt-ffi/library-singleton-set! lib libpath)))
  ([] (initialize! nil)))


(defn get-config-options
  []
  (resource/stack-resource-context
   (->> (range (duckdb_config_count))
        (mapv (fn [^long idx]
                (let [msg-ptr (dt-ffi/make-ptr :pointer 0)
                      desc-ptr (dt-ffi/make-ptr :pointer 0)]
                  (duckdb_get_config_flag idx msg-ptr desc-ptr)
                  {:name (dt-ffi/c->string (Pointer. (msg-ptr 0)))
                   :desc (dt-ffi/c->string (Pointer. (desc-ptr 0)))}))))))


(defn open-db
  ([^String path config-options]
   (resource/stack-resource-context
    (let [path (or path "")
          config-ptr (when-not (empty? config-options)
                       (let [config-ptr (dt-ffi/make-ptr :pointer 0)
                             _ (duckdb_create_config config-ptr)
                             cfg (Pointer. (config-ptr 0))]
                         (doseq [[k v] config-options]
                           (duckdb_set_config cfg (str k) (str v)))
                         config-ptr))
          config (when config-ptr
                   (Pointer. (config-ptr 0)))
          db-ptr (dt-ffi/make-ptr :pointer 0)
          err (dt-ffi/make-ptr :pointer 0)
          open-retval (duckdb_open_ext path db-ptr config err)]
      (when config-ptr
        (duckdb_destroy_config config-ptr))
      (when-not (= open-retval DuckDBSuccess)
        (let [err-ptr (Pointer. (err 0))
              err-str (dt-ffi/c->string err-ptr)]
          (duckdb_free err-ptr)
          (throw (Exception. (format "Error opening database: %s" err-str)))))
      (Pointer. (db-ptr 0)))))
  ([^String path]
   (open-db path nil))
  ([]
   (open-db "")))


(defn close-db
  [^Pointer db]
  (resource/stack-resource-context
   (let [db-ptr (dt-ffi/make-ptr :pointer (.address db))]
     (duckdb_close db-ptr))))


(defn connect
  [^Pointer db]
  (resource/stack-resource-context
   (let [ctx-ptr (dt-ffi/make-ptr :pointer 0)]
     (duckdb_connect db ctx-ptr)
     (Pointer. (ctx-ptr 0)))))


(defn disconnect
  [^Pointer conn]
  (resource/stack-resource-context
   (let [conn-ptr (dt-ffi/make-ptr :pointer (.address conn))]
     (duckdb_disconnect conn-ptr))))
