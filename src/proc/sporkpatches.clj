;;A namespace to apply patches until the low side
;;version of spork can catch up.
(ns proc.sporkpatches
  (:require [spork.util.table]
            [spork.util.clipboard]))



(defmacro within-ns [ns & expr]
  `(do (ns ~ns)
       ~@expr
       (ns ~'proc.sporkpatches)))

;;Patched as of 2/9/17 by tom, migrating to spork master.
(ns spork.util.table)

(import '[java.util.concurrent ConcurrentHashMap])
(defn ->string-pool
  "Creates a canonicalized string pool, starting with n, bounded up to 
   size bound.  Can be used likea function to canonicalize strings. When
   applied to a string, if the string exists in the known set of strings,
   the sole known reference to the already-existing string is returned, 
   rather than the 'new' string.  For datasets with a low cardinality, 
   we get a lot of referential sharing using this strategy.  Typically 
   only used during parsing."
  [n bound]
  (let [^ConcurrentHashMap cm (ConcurrentHashMap. (int n))]
    (reify clojure.lang.IFn
      (invoke [this  x]
        (do (when  (> (.size cm) (int bound))
              (.clear cm))
            (if-let [canon (.putIfAbsent cm x x)]
              canon
              x)))
      clojure.lang.IDeref
      (deref [this] (map key (seq cm))))))

;;derive-schema is used by lines->records and others to try to
;;"guess" the schema of a potential line-driven datasource based
;;off of the types in the first row (second line)
;;However, if we have no data, we have no first row...
;;This throws a null-pointer-error in split-by-tab,
;;and obfuscates the problem.  In reality, if we have
;;no data, our schema is the same as the empty schema {},
;;which is "parse everything as a string".
(defn derive-schema [row & {:keys [parsemode]}]
  (if-not row {} ;;return the empty schema.
    (let [xs     (split-by-tab row)
          parser (if (= parsemode :scientific)  parse/parse-string
                     parse/parse-string-nonscientific);clojure.edn/read-string
          types  (mapv (comp type parser) xs)]    
      (mapv (fn [t]
              (cond
                (identical? t java.lang.String) :text
                (identical? t java.lang.Integer) :long
                (identical? t java.lang.Long)    :long
                (identical? t java.lang.Double) :double
                :else (throw (Exception. "unsupported parsing type " t)))) types)
      )))

;;--Migrated from proc.util by Tom--

;patched this for 2 reasons.  
;1) If the last field has no value, we need to patch in an empty string for it.  Else, we try to aget an array                               
;index which isn't present.                               
;2) We can now bind *split-by* to #"," for csv files                           
#_(def ^:dynamic *split-by* #"\t")

;;tom: now using pooled-parsing-scheme in spork.util.table,
;;obviates the need for this.
#_(defn lines->records
  "Produces a reducible stream of 
   records that conforms to the specifications of the 
   schema.  Unlike typed-lines->table, it does not store
   data as primitives.  Records are potentially ephemeral 
   and will be garbage collected unless retained.  If no 
   schema is provided, one will be derived."
  [ls schema & {:keys [parsemode keywordize-fields?] 
                :or   {parsemode :scientific
                       keywordize-fields? true}}]
  (let [raw-headers   (mapv clojure.string/trim (clojure.string/split  (general/first-any ls) *split-by*))
        fields        (mapv (fn [h]
                              (let [root  (if (= (first h) \:) (subs h  1) h)]
                                (if keywordize-fields?
                                  (keyword root)
                                  root)))
                            raw-headers)
        schema    (if (empty? schema)
                    (let [types (derive-schema (general/first-any (r/drop 1 ls)) :parsemode parsemode)]
                      (into {} (map vector fields types)))
                    schema)
        s         (unify-schema schema fields)
        parser    (spork.util.parsing/parsing-scheme s)
        idx       (atom 0)
        idx->fld  (reduce (fn [acc h]
                            (if (get s h)
                              (let [nxt (assoc acc @idx h)
                                    _   (swap! idx unchecked-inc)]
                                nxt)
                              (do (swap! idx unchecked-inc) acc))) {} fields)
        ;;throw an error if the fld is not in the schema.
        _ (let [known   (set (map name (vals idx->fld)))
                missing (filter (complement known) (map name (keys s)))]
            (assert (empty? missing) (str [:missing-fields missing])))
        ;;added by Tom, Craig's stuff was killing us with reflection costs...
        ;;don't need this anymore, all output should be tabbed now.
        ^String sep "\t" ;(if (= (.pattern *split-by*) "\\t") "\t" ",")
        ;;our string-pool
        sp     (s/->string-pool 100 10000)
        parse! (fn [x] (if (string? x) (sp x) x))
        ]                                          
    (->> ls
         (r/drop 1)
         (r/map  (fn [^String ln] (.split ln sep)))
         (r/map  (fn [^objects xs]
                   ;if the final field is empty, we won't get an extra empty string when splitting by tab.  If other fields are empty, we'll get an extra string.
                  (let [last-fld-idx (apply max (keys idx->fld))
                        last-fld-empty? (= (alength xs) last-fld-idx)]
                   (reduce-kv (fn [acc idx fld]
                                (let [new-val (if (and (= idx last-fld-idx) last-fld-empty?) "" (aget xs idx))]
                                  (assoc acc fld (parse! (parser fld new-val))
                                         
                                         )))
                              {} idx->fld))))
         )))

;;Note: unpatched, doesn't make sense for main spork.

;wanted to patch this in case there is no value for the last field but didn't do anything yet (see comment)
#_(defn typed-lines->table
  "A variant of lines->table that a) uses primitives to build
   up our table columns, if applicable, using rrb-trees.
   b) enforces the schema, ignoring fields that aren't specified.
   c) throws an exception on missing fields."
  [ls schema & {:keys [parsemode keywordize-fields?] 
                :or   {parsemode :scientific
                       keywordize-fields? true}}]
  (let [raw-headers   (mapv clojure.string/trim (clojure.string/split  (general/first-any ls) #"\t" ))
        fields        (mapv (fn [h]
                              (let [root  (if (= (first h) \:) (subs h  1) h)]
                                (if keywordize-fields?
                                  (keyword root)
                                  root)))
                            raw-headers)
        s         (unify-schema schema fields)
        parser    (spork.util.parsing/parsing-scheme s)
        idx       (atom 0)
        idx->fld  (reduce (fn [acc h]
                            (if (get s h)
                              (let [nxt (assoc acc @idx h)
                                    _   (swap! idx unchecked-inc)]
                                nxt)
                              (do (swap! idx unchecked-inc) acc))) {} fields)
        ;;throw an error if the fld is not in the schema.
        _ (let [known   (set (map name (vals idx->fld)))
                missing (filter (complement known) (map name (keys s)))]
            (assert (empty? missing) (str [:missing-fields missing :names (vals idx->fld)])))
        cols    (volatile-hashmap! (into {} (for [[k v] s]
                                              [k (typed-col v)])))]                                          
    (->> ls
       
         (r/drop 1)
         (r/map  (fn [^String ln] (.split ln "\t")))
         (reduce (fn [acc  ^objects xs] ;xs are a tab-split line)
                   (let [last-fld-idx (apply max (keys idx->fld))
                        last-fld-empty? (= (alength xs) last-fld-idx)]
                   (reduce-kv (fn [acc idx fld]
                                (let [c (get cols fld)]
                                  (do (vswap! c conj! 
                                              ;(if (and (= idx last-fld-idx) last-fld-empty?)
                                                ;"" This doesn't work.  Besides, we'd have strings mixed with other types now.
                                                (parser fld  (aget xs idx))) 
                                                
                                      acc))) acc idx->fld))) cols)
    
 
         
         (unvolatile-hashmap!)
         (make-table)
         )))

;;Note: Migrated calls to use the idiomatic API in spork.util.table, delegating functionality to
;;spork.util.stream functions.

;patched because craig wanted to specify an order of the fields and wanted to write to the same file multiple times in separate calls.
;;this is a pretty useful util function.  could go in table.
#_(defn records->file [xs dest & {:keys [flds append? headers?] :or {flds (vec (keys (first xs))) append? false headers? true}}]
  (let [hd   (first xs)
        sep  (str \tab)
        header-record (reduce-kv (fn [acc k v]
                                   (assoc acc k
                                          (name k)))
                                 hd
                                 hd)
        
        write-record! (fn [^java.io.BufferedWriter w r]
                        (doto ^java.io.BufferedWriter
                          (reduce (fn [^java.io.BufferedWriter w fld]
                                    (let [x (get r fld)]
                                      (doto w
                                        (.write (str x))
                                        (.write sep))))
                                  w
                                  flds)
                            (.newLine)))]
    (with-open [out (clojure.java.io/writer dest :append append?)]
      (do (when headers? (write-record! out header-record))
      (reduce (fn [o r]                
                (write-record! o r))
               out
              xs)))))

(ns proc.sporkpatches)

;;applied these patches to spork 0.1.9.6
;;patch...
;;Jump into the table namespace
;(ns spork.util.table)

;; (defn check-header
;;   "Checks to see if a stringified header begins with a :.  If it does, removes the :" 
;;   [h]
;;   (if (= (str (first h)) ":") (subs h 1) h))

;; (defn- select- 
;;   "A small adaptation of Peter Seibel's excellent mini SQL language from 
;;    Practical Common Lisp.  This should make life a little easier when dealing 
;;    with abstract tables...."   
;;   [columns from where unique orderings]
;;   (if (and (not (database? from)) (tabular? from)) ;simple selection....
;;     (->> (select-fields columns from);extract the fields.
;;       (process-if where (partial filter-records where))
;;       (process-if unique select-distinct)
;;       (process-if orderings (partial order-by orderings))
;;       )))
 
;; (defn select
;;     "Wrapper around select..."
;;     [& {:keys [fields from where unique orderings]
        
;;         :or {fields :* unique nil
;;              }}]
    
;;     (select- fields from where unique orderings))
  
;; (defn lines->table 
;;   "Return a map-based table abstraction from reading a string of tabdelimited 
;;    text.  The default string parser tries to parse an item as a number.  In 
;;    cases where there is an E in the string, parsing may return a number or 
;;    infinity.  Set the :parsemode key to any value to anything other than 
;;    :scientific to avoid parsing scientific numbers."
;;    [lines & {:keys [parsemode keywordize-fields? schema] 
;;               :or   {parsemode :scientific
;;                      keywordize-fields? true
;;                      schema {}}}] 
;;   (let [tbl   (->column-table 
;;                  (vec (map (if keywordize-fields?  
;;                              (comp keyword check-header clojure.string/trim)
;;                              identity) (split-by-tab (first lines)))) 
;;                  [])
;;         parsef (parse/parsing-scheme schema :default-parser  
;;                  (if (= parsemode :scientific) parse/parse-string
;;                      parse/parse-string-nonscientific))
;;         fields (table-fields tbl)      
;;         parse-rec (comp (parse/vec-parser fields parsef) split-by-tab)]
;;       (->> (conj-rows (empty-columns (count (table-fields tbl))) 
;;                       (map parse-rec (rest lines)))
;;            (assoc tbl :columns)))) 

;; ;patched in order to add the option of not including the field of the table in the returned string
;; (defn table->string  
;;   "Render a table into a string representation.  Uses clojure.string/join for
;;    speed, which uses a string builder internally...if you pass it a separator.
;;    By default, converts keyword-valued field names into strings.  Caller 
;;    may supply a different function for writing each row via row-writer, as 
;;    well as a different row-separator.  row-writer::vector->string, 
;;    row-separator::char||string" 
;;   [tbl & {:keys [stringify-fields? row-writer row-separator include-fields?] 
;;           :or   {stringify-fields? true 
;;                  row-writer  row->string
;;                  row-separator \newline
;;                  include-fields? true}}]
;;   (let [xs (concat (when include-fields?
;;                      (if stringify-fields? 
;;                      [(vec (map field->string (table-fields tbl)))]  
;;                      [(table-fields tbl)])) 
;;                    (table-rows tbl))]
;;     (strlib/join row-separator (map row-writer xs))))


;;Patched in 0.1.9.6
;;patch these in temporarily.
;; (within-ns 
;;  spork.util.table
;;  (defn paste-table! [t]  (spork.util.clipboard/paste! (table->tabdelimited t)))
;;  (defn add-index [t] (conj-field [:index (take (record-count t) (iterate inc 0))] t))
;;  (defn no-colon [s]   (if (or (keyword? s)
;;                               (and (string? s) (= (first s) \:)))
;;                         (subs (str s) 1)))



;;  (defn collapse [t root-fields munge-fields key-field val-field]
;;    (let [root-table (select :fields root-fields   :from t)]
;;      (->>  (for [munge-field munge-fields]
;;              (let [munge-col  (select-fields [munge-field] t)
;;                    munge-name (first (get-fields munge-col))
                   
;;                    key-col    [key-field (into [] (take (record-count root-table) 
;;                                                         (repeat munge-name)))]
;;                    val-col    [val-field  (first (vals (get-field munge-name munge-col)))]]
;;                (conj-fields [key-col val-col] root-table)))
;;            (concat-tables)          
;;            (select-fields  (into root-fields [key-field val-field])))))

;;  (defn rank-by  
;;    ([trendf rankf trendfield rankfield t]
;;       (->> (for [[tr xs] (group-by trendf  (table-records t))] 
;;              (map-indexed (fn [idx r] (assoc r trendfield tr  rankfield idx)) (sort-by rankf xs)))
;;            (reduce      concat)
;;            (records->table)
;;            (select-fields (into (table-fields t) [trendfield rankfield]))))
;;    ([trendf rankf t] (rank-by trendf rankf :trend :rank t)))

;;  (defn ranks-by [names-trends-ranks t]    
;;    (let [indexed (add-index t)
;;          new-fields (atom [])
;;          idx->rankings 
;;          (doall (for [[name [trendf rankf]] names-trends-ranks]
;;                   (let [rankfield (keyword (str (no-colon name) "-rank"))
;;                         _ (swap! new-fields into [name rankfield])]
;;                     (->> indexed
;;                          (rank-by trendf rankf name rankfield)
;;                          (select-fields [:index name rankfield])
;;                          (reduce (fn [acc r]
;;                                    (assoc acc (:index r) [(get r name) (get r rankfield)])) {})))))]
;;      (conj-fields      
;;       (->> idx->rankings
;;            (reduce (fn [l r] 
;;                      (reduce-kv (fn [acc idx xs]
;;                                   (update-in acc [idx]
;;                                              into xs))
;;                                 l r)))
;;            (sort-by first)
;;            (mapv second)
;;            (spork.util.vector/transpose)
;;            (mapv vector @new-fields)
;;            )
;;       indexed)))
;; )

;;patched in 0.1.9.6

;;tHIS IS A HACK UNTIL I CAN CHANGE IT BAAAH
;;we're just fixing the vector parsing code, I'll commit this and
;;deploy correctly tomorrow.
;; (within-ns spork.util.parsing
;;   (defn vec-parser 
;;     "Given a set of fields, and a function that maps a field name to 
;;    a parser::string->'a, returns a function that consumes a sequence
;;    of strings, and parses fields with the corresponding 
;;    positional parser.  Alternately, caller may supply a parser as a 
;;    single argument, to be applied to a vector of strings."
;;     ([fields field->value]
;;        (let [xs->values  (vec (map #(partial2 field->value %) fields))
;;              bound       (count xs->values)
;;              uptolast    (dec bound)]
;;          (fn [xs]
;;            (let [res 
;;                  (loop [acc []
;;                         idx 0]
;;                    (if (= idx uptolast) acc
;;                        (recur (conj acc ((nth xs->values idx) (nth xs idx)))
;;                               (inc idx))))]
;;              (conj res 
;;                    (cond (== (count xs) bound) 
;;                          ((nth xs->values uptolast) (nth xs uptolast)) ;;append the last result
;;                          (== (count xs) uptolast) nil))))))      ;;add an empty value                 
;;     ([f] 
;;        (let [parsefunc (lookup-parser f identity)]
;;          (fn [xs]         
;;            (reduce (fn [acc x] 
;;                      (conj acc (parsefunc x)))  []
;;                      xs))))))


;;Patched in 0.1.9.6
;; (ns spork.util.temporal)

;; (defn temporal-profile 
;;   "Extracts an event-driven profile of the concurrent records over time from a 
;;    sequence of values, where start-func is a function that yields a start time 
;;    for each record, and duration-func is a function that yields a numeric 
;;    duration for each record."
;;   [xs & {:keys [start-func duration-func]
;;          :or {start-func :Start duration-func :Duration}}] 
;;   (let [add-demand  (fn [t x] {:t t :type :add  :data x}) ;this returned map is an even or "e". data is the record
;;         drop-demand (fn [t x] {:t t :type :drop :data x}) ;t is the time the event will occur
;;         resample    (fn [t]   {:t t :type :resampling :data nil})
;;         earliest    (fn [l r] (compare (:t l) (:t r)))
;;         handle (fn [e [es actives state]] ;es are pending states ;actives are a set. state is a keyword as to what happened last
;;                  (let [t       (:t e)
;;                        data    (:data e)]
;;                    (case (:type e)
                     
;;                      :resampling [es actives :sampled]
;;                      :add (let [to-drop (drop-demand (+ t (duration-func data)) data) ;to-drop is an "e"
;;                                 nxt     (conj es [to-drop (:t to-drop)])] ;this is how es are sent to the queu for sorting [event time]
;;                             [nxt 
;;                              (conj actives data)
;;                              :added])
;;                      :drop (let [res (disj actives data)]
;;                              [es res :dropped])
;;                      (throw (Exception. (str "unknown event" e))))))
;;         initial-events (into pq/emptyq   ;activities have starts and durations
;;                              (map (fn [x] [(add-demand (start-func x) x) (start-func x)]) xs))] ; now have a pending set of activities
;;     (gen/unfold (fn [state]  (empty? (first state)))  ;halt when no more events.            
;;                 (fn [state]                
;;                   (let [es      (first state)
;;                         actives (second state)
;;                         s       (nth state 2) ;action that was done last
;;                         event               (peek es)
;;                         current-time        (:t event)		      		       
;;                         remaining-events    (pop es)
;;                         ]
;;                     (if (or (= (:type event) :resampling) ;Add and drop at beginning of day. Resample after these changes.
;;                             (when-let [ne (peek remaining-events)]
;;                               (= current-time (:t ne))))
;;                       (handle event [remaining-events actives s]) ;handle next event
;;                       (handle event 
;;                               [(conj remaining-events [(resample current-time)
;;                                                        current-time])
;;                                actives s])
;;                       ))) 
;;                 [initial-events #{} :init])))
;; ;output is sequence of [pendingevents actives laststate]
;; ;turining things on and off as move through pending set of activites

;; (defn peak-activities
;;   "Computes peak concurrent activities, as per activity-profile, for a sequence 
;;    of temporal records xs.  Returns the top N active days.  Caller may supply 
;;    a custom peak-function, which operates on records of 
;;    {:actives #{...} :count n}" ;just wanted to correct this doc string of what we're operating on.
;;   [xs & {:keys [start-func duration-func peak-function] 
;;          :or   {start-func :Start duration-func :Duration 
;;                 peak-function (fn [r] (:count r))}}]
;;   (let [sorted  (->> (activity-profile (sort-by start-func xs) 
;;                             :start-func start-func 
;;                             :duration-func duration-func)
;;                      (sort-by (fn [[t r]] (peak-function r))) 
;;                      (reverse)
;;                      )
;;         peak    (peak-function (second (first sorted)))]
;;     (take-while (fn [[t r]] (= (peak-function r) peak))
;;                 sorted))) 
 
;;   ;;jump back to proc.util
;; (ns proc.util)
;; ;;end patch
