(ns tmducken.duckdb.ffi
  (:require [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
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
;; decimal
(def ^{:tag 'long} DUCKDB_TYPE_DECIMAL 19)
;; duckdb_timestamp, in seconds
(def ^{:tag 'long} DUCKDB_TYPE_TIMESTAMP_S 20)
;; duckdb_timestamp, in milliseconds
(def ^{:tag 'long} DUCKDB_TYPE_TIMESTAMP_MS 21)
;; duckdb_timestamp, in nanoseconds
(def ^{:tag 'long} DUCKDB_TYPE_TIMESTAMP_NS 22)
 ;; enum type, only useful as logical type
(def ^{:tag 'long} DUCKDB_TYPE_ENUM 23)
;; list type, only useful as logical type
(def ^{:tag 'long} DUCKDB_TYPE_LIST 24)
;; struct type, only useful as logical type
(def ^{:tag 'long} DUCKDB_TYPE_STRUCT 25)
;; map type, only useful as logical type
(def ^{:tag 'long} DUCKDB_TYPE_MAP 26)
;; duckdb_hugeint
(def ^{:tag 'long} DUCKDB_TYPE_UUID 27)
;; union type, only useful as logical type
(def ^{:tag 'long} DUCKDB_TYPE_UNION 28)
;; duckdb_bit
(def ^{:tag 'long} DUCKDB_TYPE_BIT 29)


(def duckdb-type-map
  {DUCKDB_TYPE_INVALID :DUCKDB_TYPE_INVALID
   DUCKDB_TYPE_BOOLEAN :DUCKDB_TYPE_BOOLEAN
   DUCKDB_TYPE_TINYINT :DUCKDB_TYPE_TINYINT
   DUCKDB_TYPE_SMALLINT :DUCKDB_TYPE_SMALLINT
   DUCKDB_TYPE_INTEGER :DUCKDB_TYPE_INTEGER
   DUCKDB_TYPE_BIGINT :DUCKDB_TYPE_BIGINT
   DUCKDB_TYPE_UTINYINT :DUCKDB_TYPE_UTINYINT
   DUCKDB_TYPE_USMALLINT :DUCKDB_TYPE_USMALLINT
   DUCKDB_TYPE_UINTEGER :DUCKDB_TYPE_UINTEGER
   DUCKDB_TYPE_UBIGINT :DUCKDB_TYPE_UBIGINT
   DUCKDB_TYPE_FLOAT :DUCKDB_TYPE_FLOAT
   DUCKDB_TYPE_DOUBLE :DUCKDB_TYPE_DOUBLE
   DUCKDB_TYPE_TIMESTAMP :DUCKDB_TYPE_TIMESTAMP
   DUCKDB_TYPE_DATE :DUCKDB_TYPE_DATE
   DUCKDB_TYPE_TIME :DUCKDB_TYPE_TIME
   DUCKDB_TYPE_INTERVAL :DUCKDB_TYPE_INTERVAL
   DUCKDB_TYPE_HUGEINT :DUCKDB_TYPE_HUGEINT
   DUCKDB_TYPE_VARCHAR :DUCKDB_TYPE_VARCHAR
   DUCKDB_TYPE_BLOB :DUCKDB_TYPE_BLOB
   DUCKDB_TYPE_DECIMAL :DUCKDB_TYPE_DECIMAL
   DUCKDB_TYPE_TIMESTAMP_S :DUCKDB_TYPE_TIMESTAMP_S
   DUCKDB_TYPE_TIMESTAMP_MS :DUCKDB_TYPE_TIMESTAMP_MS
   DUCKDB_TYPE_TIMESTAMP_NS :DUCKDB_TYPE_TIMESTAMP_NS
   DUCKDB_TYPE_ENUM :DUCKDB_TYPE_ENUM
   DUCKDB_TYPE_LIST :DUCKDB_TYPE_LIST
   DUCKDB_TYPE_STRUCT :DUCKDB_TYPE_STRUCT
   DUCKDB_TYPE_MAP :DUCKDB_TYPE_MAP
   DUCKDB_TYPE_UUID :DUCKDB_TYPE_UUID
   DUCKDB_TYPE_UNION :DUCKDB_TYPE_UNION
   DUCKDB_TYPE_BIT :DUCKDB_TYPE_BIT }
  )

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


    ;;DUCKDB_API duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk);
    :duckdb_append_data_chunk {:rettype :int32
                               :argtypes [[appender (by-value :duckdb-appender)]
                                          [data-chunk (by-value :duckdb-data-chunk)]]}

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
    :duckdb_nullmask_data {:rettype :pointer
                           :argtypes [[result :pointer]
                                      [col :int64]]}
    :duckdb_column_type {:rettype :int64
                         :argtypes [[result :pointer]
                                    [col :int64]]}
    :duckdb_column_data {:rettype :pointer
                         :argtypes [[result :pointer]
                                    [col :int64]]}


    ;;DUCKDB_API bool duckdb_result_is_streaming(duckdb_result result);
    :duckdb_result_is_streaming {:rettype :int8
                                 :argtypes [[result (by-value :duckdb-result)]]}

    ;;DUCKDB_API duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col);
    :duckdb_column_logical_type {:rettype (by-value :duckdb-logical-type)
                                 :argtypes [[result :pointer]
                                            [cidx :int64]]}

    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Data Chunks - new API
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

    ;; DUCKDB_API idx_t duckdb_result_chunk_count(duckdb_result result);
    :duckdb_result_chunk_count {:rettype :int64
                                :argtypes [[result (by-value :duckdb-result)]]}

    ;;DUCKDB_API duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_index);
    :duckdb_result_get_chunk {:rettype (by-value :duckdb-data-chunk)
                              :argtypes [[result (by-value :duckdb-result)]
                                         [chunk-index :int64]]}

    ;;DUCKDB_API duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count);
    :duckdb_create_data_chunk {:rettype (by-value :duckdb-data-chunk)
                               :argtypes [[types :pointer]
                                          [column-count :int64]]}

    ;;DUCKDB_API void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk);
    :duckdb_destroy_data_chunk {:rettype :void
                                :argtypes [[chunk :pointer]]} ;;ptr-to-chunk
    ;;DUCKDB_API idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk);
    :duckdb_data_chunk_get_column_count {:rettype :int64
                                         :argtypes [[chunk (by-value :duckdb-data-chunk)]]}

    ;;DUCKDB_API idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk);
    :duckdb_data_chunk_get_size {:rettype :int64
                                 :argtypes [[chunk (by-value :duckdb-data-chunk)]]}

    ;;DUCKDB_API duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx);
    :duckdb_data_chunk_get_vector {:rettype (by-value :duckdb-vector)
                                   :argtypes [[chunk (by-value :duckdb-data-chunk)]
                                              [col-idx :int64]]}

    ;;DUCKDB_API duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector)
    :duckdb_vector_get_column_type {:rettype (by-value :duckdb-logical-type)
                                    :argtypes [[vector (by-value :duckdb-vector)]]}

    ;;DUCKDB_API void *duckdb_vector_get_data(duckdb_vector vector)
    :duckdb_vector_get_data {:rettype :pointer
                             :argtypes [[vector (by-value :duckdb-vector)]]}

    ;;DUCKDB_API uint64_t *duckdb_vector_get_validity(duckdb_vector vector)
    :duckdb_vector_get_validity {:rettype :pointer
                                 :argtypes [[vector (by-value :duckdb-vector)]]}


    ;;DUCKDB_API duckdb_logical_type duckdb_create_logical_type(duckdb_type type);
    :duckdb_create_logical_type {:rettype (by-value :duckdb-logical-type)
                                 :argtypes [[duckdb_type :int32]]}

    ;;DUCKDB_API void duckdb_destroy_logical_type(duckdb_logical_type *type);
    :duckdb_destroy_logical_type {:rettype :void
                                  :argtypes [[type :pointer]]}

    ;;DUCKDB_API duckdb_type duckdb_get_type_id(duckdb_logical_type type);
    :duckdb_get_type_id {:rettype :int32
                         :argtypes [[type (by-value :duckdb-logical-type)]]}

    ;;DUCKDB_API void duckdb_data_chunk_reset(duckdb_data_chunk chunk);
    :duckdb_data_chunk_reset {:rettype :void
                              :argtypes [[chunk (by-value :duckdb-data-chunk)]]}

    ;;DUCKDB_API void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size);
    :duckdb_data_chunk_set_size {:rettype :void
                                 :argtypes [[chunk (by-value :duckdb-data-chunk)]
                                            [size :int64]]}

    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; Prepared Statements
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;; DUCKDB_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
    ;;                                                          duckdb_prepared_statement *out_prepared_statement)
    ;; DUCKDB_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement)
    ;; DUCKDB_API const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement)
    ;; DUCKDB_API idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement)
    ;; DUCKDB_API duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx)
    ;; DUCKDB_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val)
    ;; DUCKDB_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                       duckdb_hugeint val)
    ;; DUCKDB_API duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val)
    ;; DUCKDB_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val)
    ;; DUCKDB_API duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val)
    ;; DUCKDB_API duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                    duckdb_date val)
    ;; DUCKDB_API duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                    duckdb_time val)
    ;; DUCKDB_API duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                         duckdb_timestamp val)
    ;; DUCKDB_API duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                        duckdb_interval val)
    ;; DUCKDB_API duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                       const char *val)
    ;; DUCKDB_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                              const char *val, idx_t length)
    ;; DUCKDB_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx,
    ;;                                                                    const void *data, idx_t length)
    ;; DUCKDB_API duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx)
    ;; DUCKDB_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement,
    ;;                                                                           duckdb_result *out_result)
    }

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



(defn define-datatypes!
  []
  @blob-def*
  @column-def*
  @result-def*
  @logical-type-def*
  @data-chunk-def*
  @vector-def*
  @appender-def*)
