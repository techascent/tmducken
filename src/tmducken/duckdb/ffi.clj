(ns tmducken.duckdb.ffi
  (:require [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [tech.v3.datatype.struct :as dt-struct]
            [clojure.tools.logging :as log]
            [tech.v3.resource :as resource])
  (:import [java.nio.file Paths]
           [tech.v3.datatype.ffi Pointer]))


(def ^:private -DUCKDB-TYPES
  '{DUCKDB_TYPE_INVALID      0
    DUCKDB_TYPE_BOOLEAN      1
    DUCKDB_TYPE_TINYINT      2
    DUCKDB_TYPE_SMALLINT     3
    DUCKDB_TYPE_INTEGER      4
    DUCKDB_TYPE_BIGINT       5
    DUCKDB_TYPE_UTINYINT     6
    DUCKDB_TYPE_USMALLINT    7
    DUCKDB_TYPE_UINTEGER     8
    DUCKDB_TYPE_UBIGINT      9
    DUCKDB_TYPE_FLOAT        10
    DUCKDB_TYPE_DOUBLE       11
    DUCKDB_TYPE_TIMESTAMP    12
    DUCKDB_TYPE_DATE         13
    DUCKDB_TYPE_TIME         14
    DUCKDB_TYPE_INTERVAL     15
    DUCKDB_TYPE_HUGEINT      16
    DUCKDB_TYPE_UHUGEINT     32
    DUCKDB_TYPE_VARCHAR      17
    DUCKDB_TYPE_BLOB         18
    DUCKDB_TYPE_DECIMAL      19
    DUCKDB_TYPE_TIMESTAMP_S  20
    DUCKDB_TYPE_TIMESTAMP_MS 21
    DUCKDB_TYPE_TIMESTAMP_NS 22
    DUCKDB_TYPE_ENUM         23
    DUCKDB_TYPE_LIST         24
    DUCKDB_TYPE_STRUCT       25
    DUCKDB_TYPE_MAP          26
    DUCKDB_TYPE_UUID         27
    DUCKDB_TYPE_UNION        28
    DUCKDB_TYPE_BIT          29
    DUCKDB_TYPE_TIME_TZ      30
    DUCKDB_TYPE_TIMESTAMP_TZ 31})

(defmacro define-long-enums
  []
  (let [sym-defs     (for [[sym-name val] -DUCKDB-TYPES]
                       `(def ~(with-meta sym-name {:tag 'long}) ~val))
        type-map-def (reduce
                       (fn [type-map-def [sym-name val]]
                         (assoc type-map-def val (keyword sym-name)))
                       {}
                       -DUCKDB-TYPES)]
    `(do ~@sym-defs
         (def duckdb-type-map ~type-map-def))))

(define-long-enums)


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
                                 [out_database :pointer]
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
    :duckdb_library_version {:rettype :pointer}
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

    ;;DUCKDB_API idx_t duckdb_vector_size();
    :duckdb_vector_size {:rettype :int64}


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
                                        [out_appender :pointer]]}
    :duckdb_appender_error {:rettype :pointer?
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


    ;;DUCKDB_API duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk);
    :duckdb_append_data_chunk {:rettype :int32
                               :argtypes [[appender :pointer]
                                          [data-chunk :pointer]]}


    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Query Execution
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    :duckdb_query {:rettype :int32
                   :argtypes [[connection :pointer]
                              [query :string]
                              [out_result :pointer]]}
    :duckdb_row_count {:rettype :int64
                       :argtypes [[result :pointer]]}
    :duckdb_column_count {:rettype :int64
                          :argtypes [[result :pointer]]}
    :duckdb_destroy_result {:rettype :void
                            :argtypes [[result :pointer]]}

    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Legacy column functions
    :duckdb_column_name {:rettype :pointer
                         :argtypes [[result :pointer]
                                    [col :int64]]}
    :duckdb_column_type {:rettype :int64
                         :argtypes [[result :pointer]
                                    [col :int64]]}

    ;;DUCKDB_API bool duckdb_result_is_streaming(duckdb_result result);
    :duckdb_result_is_streaming {:rettype :int8
                                 :argtypes [[result (by-value :duckdb-result)]]}

    ;;DUCKDB_API duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col);
    :duckdb_column_logical_type {:rettype :pointer
                                 :argtypes [[result :pointer]
                                            [cidx :int64]]}

    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Data Chunks - new API
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    ;; DUCKDB_API idx_t duckdb_result_chunk_count(duckdb_result result);
    :duckdb_result_chunk_count {:rettype :int64
                                :argtypes [[result (by-value :duckdb-result)]]}

    ;;DUCKDB_API duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_index);
    :duckdb_result_get_chunk {:rettype :pointer
                              :argtypes [[result (by-value :duckdb-result)]
                                         [chunk-index :int64]]}

    ;;DUCKDB_API duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count);
    :duckdb_create_data_chunk {:rettype :pointer
                               :argtypes [[types :pointer]
                                          [column-count :int64]]}

    ;;DUCKDB_API void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk);
    :duckdb_destroy_data_chunk {:rettype :void
                                :argtypes [[chunk :pointer]]} ;;ptr-to-chunk
    ;;DUCKDB_API idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk);
    :duckdb_data_chunk_get_column_count {:rettype :int64
                                         :argtypes [[chunk :pointer]]}

    ;;DUCKDB_API idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk);
    :duckdb_data_chunk_get_size {:rettype :int64
                                 :argtypes [[chunk :pointer]]}

    ;;DUCKDB_API duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx);
    :duckdb_data_chunk_get_vector {:rettype :pointer
                                   :argtypes [[chunk :pointer]
                                              [col-idx :int64]]}

    ;;DUCKDB_API duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector)
    :duckdb_vector_get_column_type {:rettype :pointer
                                    :argtypes [[vector :pointer]]}

    ;;DUCKDB_API void *duckdb_vector_get_data(duckdb_vector vector)
    :duckdb_vector_get_data {:rettype :pointer
                             :argtypes [[vector :pointer]]}

    ;;DUCKDB_API uint64_t *duckdb_vector_get_validity(duckdb_vector vector)
    :duckdb_vector_get_validity {:rettype :pointer
                                 :argtypes [[vector :pointer]]}

    ;;DUCKDB_API void duckdb_vector_ensure_validity_writable(duckdb_vector vector);
    :duckdb_vector_ensure_validity_writable {:rettype :void
                                             :argtypes [[vector :pointer]]}

    ;;DUCKDB_API duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector);
    :duckdb_list_vector_get_child {:rettype :pointer
                                   :argtypes [[vector :pointer]]}
    ;;DUCKDB_API idx_t duckdb_list_vector_get_size(duckdb_vector vector);
    :duckdb_list_vector_get_size {:rettype :int64
                                  :argtypes [[vector :pointer]]}

    ;;DUCKDB_API duckdb_logical_type duckdb_create_logical_type(duckdb_type type);
    :duckdb_create_logical_type {:rettype :pointer
                                 :argtypes [[duckdb_type :int32]]}

    ;;DUCKDB_API void duckdb_destroy_logical_type(duckdb_logical_type *type);
    :duckdb_destroy_logical_type {:rettype :void
                                  :argtypes [[type :pointer]]}

    ;;DUCKDB_API duckdb_type duckdb_get_type_id(duckdb_logical_type type);
    :duckdb_get_type_id {:rettype :int32
                         :argtypes [[type :pointer]]}

    ;;DUCKDB_API duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type);
    :duckdb_list_type_child_type {:rettype :pointer
                                  :argtypes [[type :pointer]]}

    ;;DUCKDB_API void duckdb_data_chunk_reset(duckdb_data_chunk chunk);
    :duckdb_data_chunk_reset {:rettype :void
                              :argtypes [[chunk :pointer]]}

    ;;DUCKDB_API void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size);
    :duckdb_data_chunk_set_size {:rettype :void
                                 :argtypes [[chunk :pointer]
                                            [size :int64]]}

    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Prepared Statements
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; DUCKDB_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
    ;;                                    duckdb_prepared_statement *out_prepared_statement);
    :duckdb_prepare {:rettype :int32
                     :argtypes [[connection :pointer]
                                [query :string]
                                [out-prepared-statement :pointer]]}
    ;; DUCKDB_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);
    :duckdb_destroy_prepare {:rettype :void
                             :argtypes [[prepared-statement :pointer]]}

    ;; DUCKDB_API const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement);
    :duckdb_prepare_error {:rettype :pointer?
                           :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]]}
    ;; DUCKDB_API idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement);
    :duckdb_nparams {:rettype :int64
                     :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]]}
    ;; DUCKDB_API duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx);
    :duckdb_param_type {:rettype :int32
                        :argtypes [[prepared-statement :pointer]
                                   [param-idx :int64]]}
    ;; DUCKDB_API duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement);
    :duckdb_clear_bindings {:rettype :int32
                            :argtypes [[prepared-statement :pointer]]}
    ;; DUCKDB_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);
    :duckdb_bind_boolean {:rettype :int32
                          :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                     [param-idx :int64]
                                     [val :int8]]}
    ;; DUCKDB_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
    :duckdb_bind_int8 {:rettype :int32
                       :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                  [param-idx :int64]
                                  [val :int8]]}
    :duckdb_bind_int16 {:rettype :int32
                        :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                   [param-idx :int64]
                                   [val :int16]]}
    :duckdb_bind_int32 {:rettype :int32
                        :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                   [param-idx :int64]
                                   [val :int32]]}
    :duckdb_bind_int64 {:rettype :int32
                        :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                   [param-idx :int64]
                                   [val :int64]]}
    :duckdb_bind_uint8 {:rettype :int32
                        :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                   [param-idx :int64]
                                   [val :int8]]}
    :duckdb_bind_uint16 {:rettype :int32
                         :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                    [param-idx :int64]
                                    [val :int16]]}
    :duckdb_bind_uint32 {:rettype :int32
                         :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                    [param-idx :int64]
                                    [val :int32]]}
    :duckdb_bind_uint64 {:rettype :int32
                         :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                    [param-idx :int64]
                                    [val :int64]]}

    :duckdb_bind_float {:rettype :int32
                        :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                   [param-idx :int64]
                                   [val :float32]]}
    :duckdb_bind_double {:rettype :int32
                         :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                    [param-idx :int64]
                                    [val :float64]]}
    :duckdb_bind_date {:rettype :int32
                       :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                  [param-idx :int64]
                                  [val :int32]]}
    :duckdb_bind_time {:rettype :int32
                       :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                  [param-idx :int64]
                                  [val :int64]]}

    :duckdb_bind_timestamp {:rettype :int32
                            :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                       [param-idx :int64]
                                       [val :int64]]}
    ;; DUCKDB_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                              const char *val, idx_t length);
    :duckdb_bind_varchar_length {:rettype :int32
                                 :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                            [param-idx :int64]
                                            [val :pointer]
                                            [length :int64]]}

    :duckdb_bind_null {:rettype :int32
                       :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                  [param-idx :int64]]}

    :duckdb_result_statement_type {:rettype :int32
                                   :argtypes [[result (by-value :duckdb-result)]]}
    :duckdb_extract_statements {:rettype :int64
                                :argtypes [[connection :pointer]
                                           [query :string]
                                           [out-extracted-statements :pointer]]}
    :duckdb_prepare_extracted_statement {:rettype :int32
                                         :argtypes [[connection :pointer]
                                                    [statement (by-value :duckdb-extracted-statements)]
                                                    [statement-idx :int64]
                                                    [out-prepared-statement :pointer]]}
    :duckdb_extract_statements_error {:rettype  :pointer?
                                      :argtypes [[statements (by-value :duckdb-extracted-statements)]]}
    :duckdb_destroy_extracted {:rettype :void
                               :argtypes [[extracted-statements :pointer]]}
    :duckdb_result_return_type {:rettype :int32
                                :argtypes [[result (by-value :duckdb-result)]]}
    :duckdb_prepared_statement_type {:rettype :int32
                                     :argtypes [[statement (by-value :duckdb-prepared-statement)]]}

    ;; ;; DUCKDB_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;; ;;                                                                       duckdb_hugeint val);
    ;; ;; DUCKDB_API duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;; ;;                                                                       duckdb_decimal val);
    ;; ;; DUCKDB_API duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;; ;;                                                                    duckdb_date val);
    ;; ;; DUCKDB_API duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;; ;;                                                                    duckdb_time val);
    ;; ;; DUCKDB_API duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;; ;;                                                                         duckdb_timestamp val);
    ;; ;; DUCKDB_API duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;; ;;                                                                        duckdb_interval val);
    ;; ;; DUCKDB_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;; ;;                                                                    const void *data, idx_t length);

    ;; ;; DUCKDB_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement,
    ;; ;;                                                                           duckdb_result *out_result);

    ;; ;; DUCKDB_API duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement,
    ;; ;;                                                                           duckdb_pending_result *out_result);
    :duckdb_pending_prepared {:rettype :int32
                              :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                         [out-pending-result :pointer]]}
    ;; ;; DUCKDB_API duckdb_state duckdb_pending_prepared_streaming(duckdb_prepared_statement prepared_statement,
    ;; ;;                                                                                     duckdb_pending_result *out_result);
    :duckdb_pending_prepared_streaming {:rettype :int32
                                        :argtypes [[prepared-statement (by-value :duckdb-prepared-statement)]
                                                   [out-pending-result :pointer]]}
    ;; ;; DUCKDB_API void duckdb_destroy_pending(duckdb_pending_result *pending_result);
    :duckdb_destroy_pending {:rettype :void
                             :argtypes [[pending-result :pointer]]}
    :duckdb_pending_error {:rettype :pointer?
                           :argtypes [[pending-result (by-value :duckdb-pending-result)]]}
    ;; ;; DUCKDB_API const char *duckdb_pending_error(duckdb_pending_result pending_result);
    ;; ;; DUCKDB_API duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result);
    ;; ;; DUCKDB_API duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result);
    :duckdb_execute_pending {:rettype :int32
                             :argtypes [[pending-result (by-value :duckdb-pending-result)]
                                        [duckdb-result :pointer]]}

    ;;DUCKDB_API duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result);
    :duckdb_stream_fetch_chunk {:rettype :pointer?
                                :argtypes [[result (by-value :duckdb-result)]]}}
  nil
  nil)

;; typedef struct {
;; 	void *data;
;; 	bool *nullmask;
;; 	duckdb_type type;
;; 	char *name;
;; 	void *internal_data;
;; } duckdb_column;

;; These definitions are delayed so same jar will work on both 32-bit and 64-bit systems.
(defonce ptr-dtype* (delay (ffi-size-t/ptr-t-type)))


(defonce blob-def* (delay (dt-struct/define-datatype! :duckdb-blob
                            [{:name :data
                              :datatype @ptr-dtype*}
                             {:name :size
                              :datatype :uint64}])))


(defonce column-def* (delay (dt-struct/define-datatype! :duckdb-column
                              [{:name :data
                                :datatype @ptr-dtype*}
                               {:name :nullmask
                                :datatype @ptr-dtype*}
                               ;;duckdb-type
                               {:name :type
                                :datatype :int32}
                               {:name :name
                                :datatype @ptr-dtype*}
                               {:name :internal_data
                                :datatype @ptr-dtype*}])))


(defonce result-def* (delay
                       (dt-struct/define-datatype! :duckdb-result
                         [{:name :column-count
                           :datatype :uint64}
                          {:name :row-count
                           :datatype :uint64}
                          {:name :rows-changed
                           :datatype :uint64}
                          {:name :columns
                           :datatype @ptr-dtype*}
                          {:name :error-message
                           :datatype @ptr-dtype*}
                          {:name :internal-data
                           :datatype @ptr-dtype*}])))



(defonce logical-type-def*
  (delay
    (dt-struct/define-datatype! :duckdb-logical-type
      [{:name :__lglt
        :datatype @ptr-dtype*}])))


(defonce data-chunk-def*
  (delay
    (dt-struct/define-datatype! :duckdb-data-chunk
      [{:name :__dtck
        :datatype @ptr-dtype*}])))


(defonce vector-def*
  (delay
    (dt-struct/define-datatype! :duckdb-vector
      [{:name :__vec
        :datatype @ptr-dtype*}])))


(defonce appender-def*
  (delay
    (dt-struct/define-datatype! :duckdb-appender
      [{:name :__appn
        :datatype @ptr-dtype*}])))

(defonce prepared-statement-def*
  (delay
    (dt-struct/define-datatype! :duckdb-prepared-statement
      [{:name :__prep
        :datatype @ptr-dtype*}])
    (dt-struct/define-datatype! :duckdb-extracted-statements
      [{:name :__extrac
        :datatype @ptr-dtype*}])
    (dt-struct/define-datatype! :duckdb-pending-result
      [{:name :__pend
        :datatype @ptr-dtype*}])
    ))



(defn define-datatypes!
  []
  @blob-def*
  @column-def*
  @result-def*
  @logical-type-def*
  @data-chunk-def*
  @vector-def*
  @appender-def*
  @prepared-statement-def*)
