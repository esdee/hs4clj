(ns hs4clj.core
  (:import [com.google.code.hs4j Filter FindOperator HSClient]
           com.google.code.hs4j.Filter$FilterType
           [com.google.code.hs4j.impl HSClientImpl IndexSessionImpl ResultSetImpl]))


(defn open-client
  [& {:keys [host-name pool-size port]
      :or {host-name "localhost"
           pool-size 1
           port 9999}}]
  (HSClientImpl. host-name port pool-size))

; TODO remove duplications
(defn open-session
  [client {:keys [db table index columns filter-columns]}]
  (let [arr #(into-array (map name %))]
    (if filter-columns
      {:index-session (.openIndexSession client
                                         (name db)
                                         (name table)
                                         (name index)
                                         (arr columns)
                                         (arr filter-columns))
       :filter-columns filter-columns}
      {:index-session (.openIndexSession client
                                         (name db)
                                         (name table)
                                         (name index)
                                         (arr columns))})))

(def ^:dynamic *session*)

(defmacro within-session
  "Bind a session before executing commands. Does not close the session on
   completion."
  [session & body]
  `(binding [*session* ~session]
     ~@body))

; These are the query operators that handler socket supports -------------------
(def operators {= FindOperator/EQ
                > FindOperator/GT
                < FindOperator/LT
                >= FindOperator/GE
                <= FindOperator/LE})

; Helper function to return the handler socket expression for a clojure exp
; e.g. (operator-for =) => FindOperator/EQ
(defn- operator-for
  [operator]
  (get operators operator))

; Helper Functions for querying data

; harray is a function that will convert a seq of values into a java array
; of java.lang.String
; Handles nils and ISequables
(defprotocol IHArray
  (stringify [this]))

(extend-protocol IHArray
  nil
  (stringify [_] nil)
  clojure.lang.PersistentArrayMap
  (stringify [m] (pr-str m))
  clojure.lang.PersistentHashSet
  (stringify [h] (pr-str h))
  clojure.lang.PersistentVector
  (stringify [v] (pr-str v))
  Object
  (stringify [o] (str o)))

(defn- harray
  ([vs cfn]
   (into-array String
               (map cfn vs)))
  ([vs]
   (harray vs stringify)))

; parse is a function that will convert data from a handler socket query to
; the appropriate type
(defmulti parse
  (fn [^ResultSetImpl result-set ^long column make-type]
    make-type))

(def hs-nil (str (char 0)))

(defn not-nil?
  [v]
  (not= (str v) hs-nil))

(defmethod parse :string
  [^ResultSetImpl result-set ^long column _]
  (let [value (.getString result-set (inc column))]
    (when (not-nil? value)
          value)))

(defmethod parse :int
  [^ResultSetImpl result-set ^long column _]
  (.getInt result-set (inc column)))

; TODO remove duplications
(defn- get-result-set
  [session select {:keys [filters limit offset ]
                   :or {limit 1, offset 0}
                   :as query-options}]
  (let [operator (first select)
        index-values (harray (remove keyword? (rest select)))
        isession (:index-session session)]
    (if filters
      (.find isession index-values (get operators operator) limit offset filters)
      (.find isession index-values (get operators operator) limit offset))))

; --- Getting data -------------------------------------------------------------
(defn query
  "query records from the database, using a session"
  ([session select {:keys [map-fn parse-fn] :as query-options}]
   (let [result-set (get-result-set session select query-options)
         columns (.getColumns (:index-session session))
         vfn (or parse-fn
                 (fn [^ResultSetImpl rset] (map #(parse rset % :string)
                                                (range 0 (count columns)))))
         records (fn thisfn []
                   (when (.next result-set)
                     (cons (zipmap (map keyword columns) (vfn result-set))
                           (lazy-seq (thisfn)))))]
    (if map-fn (map map-fn (records))
               (records))))
  ([select query-options]
   (query *session* select query-options)))

(defn filters
  "Create filters for use in querying records"
  [& opts]
  (let [[session fs] (if (map? (first opts))
                               [(first opts) (flatten (rest opts))]
                               [*session* (flatten opts)])
        filter-columns (:filter-columns session)
        make-filter (fn [[operator column value]]
                       (Filter. (Filter$FilterType/FILTER)
                                (operator-for operator)
                                (.indexOf filter-columns column)
                                (if value (str value) hs-nil)))]
    (into-array (map make-filter (partition 3 fs)))))

(defn insert
  ([{isession :index-session} rows]
   (doseq [row rows]
     (.insert isession (harray row)))
   rows)
  ([rows]
   (insert *session* rows)))
