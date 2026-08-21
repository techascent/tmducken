(ns tmducken.duckdb.tablefn
  "Experimental - expose client-provided data to duckdb via a table function.

  A table function is registered against a connection and is then callable from sql
  as `select * from my-fn(...)`.  Duckdb drives it through three callbacks:

  * `bind` - called once at plan time, declares the result schema.
  * `init` - called once per scan, allocates whatever state the scan needs.
  * `function` - called repeatedly, each call filling one data chunk.  A chunk of
    size zero signals end-of-scan.

  This namespace is a deliberately minimal first step: [[register-42!]] registers a
  zero-argument table function producing a single bigint column holding a single row
  whose value is 42.

```clojure
tmducken.duckdb.tablefn> (register-42! conn \"foo\")
:ok
tmducken.duckdb.tablefn> (duckdb/sql->dataset conn \"select * from foo()\")
:_unnamed [1 1]:

| forty-two |
|----------:|
|        42 |
```"
  (:require [tmducken.duckdb.ffi :as duckdb-ffi]
            [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.native-buffer :as native-buffer])
  (:import [tech.v3.datatype.ffi Pointer]
           [sun.misc Unsafe]))


(set! *warn-on-reflection* true)


;;Duckdb invokes these callbacks from its own threads and holds onto the function
;;pointers for as long as the function is registered - which, for a connection-scoped
;;registration, is until the database goes away.  foreign-interface-instance->c is
;;only valid while the instance it came from is reachable, so instances are retained
;;here and never dropped.  This does mean each distinct callback allocated is retained
;;for the life of the process; the callbacks below are created once via delay, so that
;;is a fixed handful rather than a leak that grows with registrations.
(defonce ^:private retained-callbacks* (atom []))


(defonce ^:private void-ptr-iface*
  (delay (dt-ffi/define-foreign-interface :void [:pointer])))


(defonce ^:private void-ptr-ptr-iface*
  (delay (dt-ffi/define-foreign-interface :void [:pointer :pointer])))


(defn- ->callback
  "Instantiate ifn against iface-def, returning a pointer callable from C."
  ^Pointer [iface-def ifn]
  (let [inst (dt-ffi/instantiate-foreign-interface iface-def ifn)
        ptr  (dt-ffi/foreign-interface-instance->c iface-def inst)]
    (swap! retained-callbacks* conj inst)
    ptr))


(defn- destroy-logical-type!
  [logical-type]
  ;;duckdb_destroy_logical_type takes a pointer-to-logical-type
  (-> (dt-ffi/make-ptr :pointer (.address ^Pointer logical-type))
      (duckdb-ffi/duckdb_destroy_logical_type)))


;;TODO: an exception thrown out of any callback below unwinds into duckdb's C stack - catch and report via duckdb_{bind,init,function}_set_error instead.
(defn- bind-42
  "Declare the result schema - one bigint column.  The column name is baked in here
  rather than derived from a parameter; parameters are the next step."
  [^Pointer info]
  (let [logical-type (duckdb-ffi/duckdb_create_logical_type duckdb-ffi/DUCKDB_TYPE_BIGINT)]
    (try
      (duckdb-ffi/duckdb_bind_add_result_column info "forty-two" logical-type)
      (finally
        (destroy-logical-type! logical-type)))))


(defn- free-init-data
  [^Pointer ptr]
  (duckdb-ffi/duckdb_free ptr))


(defonce ^:private free-callback*
  (delay (->callback @void-ptr-iface* free-init-data)))


(defn- init-42
  "Allocate the scan state: a single int64 counting rows emitted so far.  Duckdb owns
  the allocation from here and calls the destroy callback when the scan finishes,
  which is why this uses duckdb_malloc rather than a jvm-tracked buffer."
  [^Pointer info]
  (let [state (duckdb-ffi/duckdb_malloc 8)]
    (.putLong (native-buffer/unsafe) (.address ^Pointer state) 0)
    (duckdb-ffi/duckdb_init_set_init_data info state @free-callback*)))


(defn- function-42
  "Fill one chunk.  The first call emits a single row; every call after that emits a
  zero-length chunk, which is how duckdb is told the scan is finished."
  [^Pointer info ^Pointer output]
  (let [unsafe     (native-buffer/unsafe)
        state-addr (.address ^Pointer (duckdb-ffi/duckdb_function_get_init_data info))
        emitted    (.getLong ^Unsafe unsafe state-addr)]
    (if (== 0 emitted)
      (let [data (-> (duckdb-ffi/duckdb_data_chunk_get_vector output 0)
                     (duckdb-ffi/duckdb_vector_get_data))]
        (.putLong ^Unsafe unsafe (.address ^Pointer data) 42)
        (.putLong ^Unsafe unsafe state-addr 1)
        (duckdb-ffi/duckdb_data_chunk_set_size output 1))
      (duckdb-ffi/duckdb_data_chunk_set_size output 0))))


(defonce ^:private callbacks-42*
  (delay {:bind     (->callback @void-ptr-iface* bind-42)
          :init     (->callback @void-ptr-iface* init-42)
          :function (->callback @void-ptr-ptr-iface* function-42)}))


(defn register-42!
  "Register a zero-argument table function named fn-name on conn.  Querying it
  produces a one-column, one-row dataset whose single value is 42.

  Note the parens at the call site - `select * from foo()`, not `select * from foo`.
  Making a bare identifier resolve to client data is a separate mechanism (a
  replacement scan) layered on top of this one."
  [conn fn-name]
  (let [{:keys [bind init function]} @callbacks-42*
        table-fn (duckdb-ffi/duckdb_create_table_function)]
    (try
      (duckdb-ffi/duckdb_table_function_set_name table-fn fn-name)
      (duckdb-ffi/duckdb_table_function_set_bind table-fn bind)
      (duckdb-ffi/duckdb_table_function_set_init table-fn init)
      (duckdb-ffi/duckdb_table_function_set_function table-fn function)
      (when-not (== duckdb-ffi/DuckDBSuccess
                    (duckdb-ffi/duckdb_register_table_function conn table-fn))
        (throw (RuntimeException. (str "Failed to register table function \"" fn-name "\""))))
      :ok
      (finally
        ;;duckdb copies the definition on register, so the handle is ours to destroy
        ;;either way.  Destroy takes a pointer-to-table-function.
        (-> (dt-ffi/make-ptr :pointer (.address ^Pointer table-fn))
            (duckdb-ffi/duckdb_destroy_table_function))))))


(comment

  (require '[tmducken.duckdb :as duckdb])

  (duckdb/initialize! {:duckdb-home "binaries"})

  (def db (duckdb/open-db))

  (def conn (duckdb/connect db))

  (register-42! conn "foo")

  (duckdb/sql->dataset conn "select * from foo()")

  :-)
