;;temporary storage until we get into spork.
(ns proc.util
  (:require [clojure.edn]
            [incanter.core :refer  :all]
            [incanter.io   :refer  :all]
            [proc.schemas          :as schemas]
            [proc.dataset] ;incanter dataset wrapper for tables.
            [clojure.core.matrix.dataset :as ds]
            [clojure.core.reducers :as r]
            [clojure.java.io :as jio]
            [iota :as iota]
            [spork.util
             [temporal :as temp]
             [io       :as io]
             [table    :as tbl]
             [parsing  :as parse]
             [zipfile  :as z]
             [general  :as general]
             [stream :as stream]]
            [spork.util.excel.core :as xl]))

(defn load-records
   "Function for loading records from one or more formats.
   If obj is a string or a resource, it will be slurped and parsed, for a tab
  delimited table.  If obj is a sequence of maps, obj will be returned."
  [obj & {:keys [schema]}]
  (cond (or (instance? java.net.URL obj) (string? obj))
        (tbl/table-records (tbl/tabdelimited->table (slurp obj)
                                               :schema schema
                                               :parsemode :noscience))
        (and (coll? obj) (map? (first obj))) obj))

(defn records
  "Aux function to wrap the legacy incanter dataset and new implementation.
   Allows us to avoid using (:rows data) to get the seq of maps originally 
   stored in the old datasets.  We use the protocol functions to access 
   column stores now."
  [ds]
  (ds/row-maps ds))

(defn map-records
  "Aux function to map functions to a record-based view 
   of the dataset or table.  Keeps tables in tables.."
  [f ds]
  (->> (records ds)
       (map f)
       ((if (tbl/tabular? ds)
          spork.util.table/records->table
          ds/dataset))))
  

;;deleted existing spork patches...
;;Moved most functions over to spork, temporarily bridging via
(defn path! [paths]
  (->> paths
       (filter (fn [^String p]
                 (when (io/fexists?
                        (if (.contains p " ")
                          (java.io.File. p)
                          (io/uri->file
                           (io/path->uri p)))))))
       (first)))

;;Utility to help grab resources, primarily test data.
;;DEPRECATED/MOVED to spork.io
(defn get-res
  "Gets the resource provided by the path.  If we want a text file, we
  call '(get-res \"blah.txt\")"
  [nm]
  (io/get-resource nm))

;;DEPRECATED/MOVED to spork.io
(defn resource-lines
  "Given a string literal that encodes a path to a resource, i.e. a
  file in the /resources folder, returns a reducible obj that iterates
  over each line (string) delimited by \newline."
  [filename]
  (io/resource-lines filename))

;DEPRECATED  spork.util.general/distinct-zipped
(defn distinct-zipped [n-colls]
  (general/distinct-zipped n-colls))

;;DEPRECATED to spork.util.general/ref?
(defn ref? [obj] (general/ref? obj))

;;DEPRECATED to spork.util.stream/mstream
(defn mstream
  "Creates a multi-file-stream abstraction.  Serves as a handle for
  multiple writers, supporting functions write-in! and writeln-in!"
  [root name headers & {:keys [childname] :or {childname (fn [x] (str x ".txt"))}}]
  (stream/mstream root name headers :childname childname))

;;DEPRECATED to spork.util.stream/get-writer!
(defn ^java.io.BufferedWriter get-writer! [ms nm]
  (stream/get-writer! ms nm))

;; on close, we want to record a manifest, in the root folder, of the 
;;files in the multistream, so that we can read the manifest and get a 
;;corresponding multireader of it

;;DEPRECATED spork.util.stream
(defn close-all! [ms]
  (stream/close-all! ms))

;;DEPRECATED spork.util.stream
(defn write-in! [ms k v]
  (stream/write-in! ms k v))

;;DEPRECATED spork.util.stream
(defn writeln-in! [ms k v]
  (stream/writeln-in! ms k v))

;;to keep from having to reload datasets that may be expensive, we
;;can keep a cache of recently loaded items.
(defn un-key [keywords]
  (mapv (fn [k] 
          (keyword (apply str (drop 2 (str k))))) keywords))

(defn read-keyed [path] 
  (let [d     (read-dataset path :header true :delim \tab :keyword-headers false)
        names (into {} (map (fn [k] [k (clojure.edn/read-string k)])  (:column-names d)))]
    (rename-cols  names d)))

;;This lets us extract unfilled-trends from demand-trends.
(defn first-line [path]
  (with-open [rdr (jio/reader path)]
    (first (line-seq rdr))))

(defn raw-headers [path] 
  (tbl/split-by-tab (first-line path)))

(defn get-headers [path] 
 (mapv keyword (tbl/split-by-tab (first-line path))))

(defn keyed-headers? [path]
  (= (first (first (raw-headers path))) \:))
  
(defn unkeyed-headers [path]
  (if (keyed-headers? path)
    (mapv (fn [x] (subs x 1)) (raw-headers path))))

;
;;These are questionable wrappers...I think we only did for
;;for marginal performance gains...
(defmacro write! [w ln]
  (let [w (vary-meta w assoc :tag 'java.io.BufferedWriter)
        ln (vary-meta ln assoc :tag 'String)]
    `(doto ~w (.write ~ln))))

(defmacro new-line! [w]
  (let [w (vary-meta w assoc :tag 'java.io.BufferedWriter)]
    `(doto ~w (.newLine))))

(defmacro writeln! [w ln]
  (let [w (vary-meta w assoc :tag 'java.io.BufferedWriter)
        ln (vary-meta ln assoc :tag 'String)]
    `(doto ~w (.write ~ln)
              (.newLine))))

(def re-csv #", |,")
(defn csv->tab [^String s]
  (clojure.string/replace s re-csv "\t"))

(defn csv-file->tab-file [inpath outpath]
  (with-open [^java.io.BufferedWriter out (clojure.java.io/writer outpath)
              in  (clojure.java.io/reader inpath)]
    (doseq [l (line-seq in)]
      (.write out (str (csv->tab l) \newline)))))
  
(defn read-tsv-dataset [path]
   (if (keyed-headers? path)
     (read-keyed path)
     (read-dataset path :header true :delim \tab)))

(defmulti as-dataset (fn [ds & opts]
                       (cond (string? ds)  :string
                             (dataset? ds) :dataset                                
                             (tbl/tabular? ds) :table
                             :else (type ds))))
(defmethod as-dataset :string [ds & opts]  (read-tsv-dataset ds))
(defmethod as-dataset :dataset [ds & opts] ds)

(defmethod as-dataset :table [ds]
  ;;shouldn't be necessary now...
  (incanter.core/dataset (spork.util.table/table-fields  ds)
                         (spork.util.table/table-records ds)))

(defn table->lines [t]
  (let [cols (tbl/table-columns t)]
    (cons (clojure.string/join \tab (tbl/table-fields t))
          (map
           (fn [idx] 
             (clojure.string/join \tab (map #(nth % idx) cols)))
           (range (tbl/record-count t))))))

(defn spit-table [path t]
  (with-open [^java.io.BufferedWriter out (jio/writer path)]
    (doseq [^String ln (table->lines t)]
      (io/writeln! out ln))))

(defn fit-schema [s path]
  (let [hs (map (fn [kw] (subs (str kw) 1)) (get-headers path))]
    (reduce (fn [acc fld]
              (if-let [res (or (get acc fld)
                               (get acc (keyword fld)))]
                acc 
                (assoc acc fld :text)))
            s hs))) 

;;DEPRECATED spork.util.genera/drop-nth now
(defn drop-nth
  "Drops the n item in coll"
  [n coll]
  (general/drop-nth n coll))

(defn next-last
  "return the next to last item in xs."
  [xs]
  (if (> (count xs) 1)
    (nth xs (- (count xs) 2))
    (throw (Exception. "(count xs) must be >1!"))))

;;make it easy to get at the state in our charts..
;;note, this only really works well if we have ;;compatible charts, specifically if there's nothing weird in the ;;plot, like having multiple trends, or axes.  I'm assuming a simple ;;set of axes and coords here.
(defprotocol IState  (state [obj]))
(extend-protocol IState
  nil
  (state [o] nil)
  org.jfree.chart.JFreeChart
  (state [obj]
    (let [plt    (.getPlot obj)
          x      (.getDomainAxis plt)
          xrange (state x)
          y      (.getRangeAxis plt)
          yrange (state y)]
      {:x-axis  (.getDomainAxis plt)
       :x-range xrange
       :bounds    [(state xrange)
                   (state yrange)]
       :y-axis  (.getRangeAxis  plt)
       :y-range yrange
       }
      ))
  org.jfree.chart.axis.NumberAxis
  (state [obj] (.getRange obj))
  org.jfree.chart.axis.CategoryAxis
  (state [obj] nil) ;;categories are weird....
  org.jfree.data.Range
  (state [obj] [(.getLowerBound obj) (.getUpperBound obj)]))

(defn sync-scales 
  "Syncs the scales of simple charts so that they all have the same x and y bounds.  
  Use :axis :y-axis to the sync the y axes and use :axis :x-axis to sync the x axes"
  [charts & {:keys [axis] :or {axis :y-axis}}]
  (let [dims (map (juxt axis (case axis :y-axis (comp second :bounds) :x-axis (comp first :bounds))) (map state charts))
        [axmin axmax] (reduce (fn [[axmin axmax] [xs [axmin? axmax?]]]                                       
                                [(min axmin axmin?)
                                 (max axmax axmax?)])
                              [Double/MAX_VALUE
                               Double/MIN_VALUE]
                              dims)]
    (doseq [plt-axis (map first dims)]
      (.setRange plt-axis axmin axmax))))

(defn sync-xy 
  "Syncs the x and y axes of our charts."
  [charts]
  (doto charts
    (sync-scales :axis :y-axis)
    (sync-scales :axis :x-axis)))

(defn set-bounds 
  "Takes a chart and sets the lower and/or upper bounds for an x or y axis. Use :y-axis or :x-axis for axis I think."
  [chart axis & {:keys [lower upper]}]
  (let [s (state chart)
        setaxis (fn [ax] (.setRange (axis s) (if lower lower (.getLowerBound (axis s)))
                                               
                                               (if upper upper (.getUpperBound (axis s)))))]
        (setaxis axis)))


;didn't finish after a while... Not sure if this works. Do i still need this?
(defn fills->records [fillp]
  (with-open [rdr (jio/reader fillp)]
          (tbl/table-records 
            (tbl/lines->table  (line-seq rdr) :parsemode :noscience :schema schemas/fillrecord))))
 
                                   
(defn delta-t-sum 
    "An attempt at using iota to quickly sum up :deltat for a 2GB txt file. Might not be taking the iota route with new Spork, so
 probably delete soon."
  [fill]
  (let [fillseq (iota/seq fill)
        headers (map keyword (-> (first fillseq)
                               (clojure.string/replace #":|\r" "")
                               (clojure.string/split  #"\t")))
        
        linef (fn [l] 
                (let [parsef (parse/parsing-scheme schemas/fillrecord :default-parser 
                                                   parse/parse-string-nonscientific)
                      parse-vec (parse/vec-parser headers parsef)
                      strings (clojure.string/split (clojure.string/replace l #"\r" "") #"\t")]
                  (zipmap headers (parse-vec strings))))]
    (time (r/fold + (time (r/map (comp :deltat linef) (rest fillseq)))))))

(defn filter-map 
  "Filters a map by calling valf on the values."
  [valf m]
  (->> (filter (fn [[k v]] (valf v)) m)
    (reduce concat)
    (apply hash-map)))

(defn files-in 
  "useful for pulling out all of the root directories for a set of marathon runs in a folder located at path"
  [path]
  (let [paths (map (fn [p] (.getPath p)) (vec (.listFiles (jio/file path))))]
    (do (spit (str path "filenames.txt") (clojure.string/join "\r\n" paths))
      paths)))


;We're not able to shared records efficiently.
;;It'd be nice if we had a column-backed record sequence, where 
;;any updates to the record create a hashmap, but the underlying 
;;values for the record are read from a vector.
;;That way, we're not having to 

(defn row-col [row col ^clojure.lang.PersistentVector cols]
   (.nth ^clojure.lang.PersistentVector (.nth cols col) row))
(defn index-of [itm xs]
  (reduce-kv (fn [acc idx x]
               (if (= x itm)
                 (reduced idx)
                 acc)) nil xs))

;;flyrecords now live in spork.data.sparse, and are accessed
;;easily via spork.util.table for our needs.
;;This is just a convenenience wrapper for the moment.
(defn table->flyrecords [t] (tbl/table->flyrecords t))

(defn up-one 
  "takes filepath and returns the directory name containing the folder"
  [filepath]
  (str (->> (clojure.string/split filepath #"/")
         (butlast)
         (clojure.string/join "/"))
       "/"))
      

(def fillr (into {} (for [[k v] schemas/fillrecord] [ (str k ) v])))

(defn fill-records 
  "Given a path to a fills file, returns the records for that file"
  [path] 
  (let [t  (tbl/tabdelimited->table 
            (slurp path) 
            :keywordize-fields? false 
            :schema fillr)            
        flds (mapv (fn [kw] (keyword (subs kw 1))) (tbl/table-fields t))]
  (-> t                  
    (assoc :fields flds)
    (tbl/table-records))))
 
(defn filter-by-vals [column oper values xs]
  "(filter column oper values xs)
   Takes a sequence of records, xs, and returns a subset of those records where header is the field name.
   If oper is =, returns the records where the header value is equal to at least one value in values.
   If oper is not=, returns the records where the header is not= to all values in values."
  (if (= oper =)
    (filter (fn [rec] (contains? (set values) (column rec))) xs)
    (reduce (fn [recs val] (filter (fn [rec] (oper (column rec) val)) recs)) xs values)))
              
                  
;;craig utilities originally in proc.tests

;_____________________________________________

(defn check-row-val-count 
  "does every row have the same number of values in it?"
  [path]
  (with-open [rdr (jio/reader path)] 
    (every? (fn [[count1 count2]] (= count1 count2))
            (partition 2 1 (map (comp count tbl/split-by-tab) (line-seq rdr))))))

(defn files=
  "Are these two files the same?  Compares the two files line-by-line."
  [file1 file2]
  (with-open [rdr1 (jio/reader file1)
              rdr2 (jio/reader file2)]
    (every? (fn [[line1 line2]] (= line1 line2))
            (map vector (rest (line-seq rdr1)) (rest (line-seq rdr2))))))

(defn vis-files=? [file1 file2]
  (with-open [rdr1 (jio/reader file1)
              rdr2 (jio/reader file2)]
    (first (remove (fn [[line1 line2]] (= line1 line2))
                   (map vector (rest (line-seq rdr1)) (rest (line-seq rdr2)))))))

(defn line-count [path]
  (with-open [rdr (jio/reader path)] 
    (count (line-seq rdr))))

(defn bad-rows 
  "returns the rows that don't have the same number of values in them"
  [path]
  (with-open [rdr (jio/reader path)] 
    (filter (fn [[row1 row2]] (= (count row1) (count row2)))
            (partition 2 1  (map tbl/split-by-tab (line-seq rdr))))))

;;moved to util.
(defn hash-file 
  "takes the path to a txt file filepath and spits out a text file containing the hashcodes for each line"
  [filepath]
  (let [infilename (last (clojure.string/split filepath #"/"))
        outfile (str (up-one filepath) "hashes-" infilename) ]
    (with-open [w (jio/writer outfile)
                rdr (jio/reader filepath)]
      (doseq [l (line-seq rdr)]
        (io/writeln! w (str (hash l)))))))

(defn separate-by
  "Like group-by, but if f returns a collection, merge the items into the return map for each item in the sequence. Doesn't
  include items that return nil when f is called on them."
  [f coll]
  (let [groups (group-by f coll)]
    (reduce-kv (fn [acc k v] (if k (if (coll? k)
                               (reduce (fn [m grp]
                                         (assoc m grp (concat v (get m grp [])))) acc k)
                               (assoc acc k (concat v (get acc k []))))
                                 acc)) {} groups)))

;;compute the areas in each sequence where the hashes do not align.
;;We probably need to avoid using map here because it may well be that
;;one of our sequences is longer.  map will impliclty truncate the sequence.
(defn hash-misses [hashed xs]
  (->> (map (fn [l r] (= l (hash r))) hashed xs)
       (map-indexed vector)
       (filter (fn [[idx x]]
                 (when (not x) idx)))))

(defn string->int [^String x]  (Integer/parseInt x))
                             
(defmacro with-rdrs [rdr-paths & body]
  `(with-open ~(reduce (fn [acc [l r]] (conj acc l r)) '[]
                         (for [[rdr path] (partition 2 rdr-paths)]
                           [rdr `(jio/reader ~path)]))
     ~@body))
                               
(defn filter-by-vals [column oper values xs]
  "(Filter header oper values xs)
   Takes a sequence of records, xs, and returns a subset of those records where header is the field name.
   If oper is =, returns the records where the header value is equal to at least one value in values.
   If oper is not=, returns the records where the header is not= to all values in values."
  (if (= oper =)
    (filter (fn [rec] (contains? (set values) (column rec))) xs)
    (reduce (fn [recs val] (filter (fn [rec] (oper (column rec) val)) recs)) xs values)))

(defn priority-map [coll]
  (into {} (map-indexed (fn [i e] [e i]) coll)))

(defn sorted-by-vals
  "Sort a collection by by calling the juxt of fns on each item in collection.  Order is determined by
   the order of juxt fns and the values in vals."
  [fns vals coll]
  (let [priority (priority-map vals)]
    ;(+ 1 (count priority)) so that any value not in vals will go to the end of the returned collection.
    (->> (sort-by #((apply juxt fns) %) coll)
     (sort-by #(vec (map (fn [v] (get priority v (+ 1 (count priority))))  ((apply juxt fns) %)))))))

;;pulled from commented-out spork.util.table
(definline zip-record! [xs ys]
  (let [ks  (with-meta (gensym "ks") {:tag 'objects})
        vs  (with-meta (gensym "vs") {:tag 'objects})
        arr (with-meta (gensym "arr") {:tag 'objects})]
    `(let [~ks ~xs
           ~vs ~ys
           ~arr (object-array (unchecked-multiply 2 (alength ~ks)))
           bound#  (alength ~ks)]
       (loop [idx# 0
              inner# 0]
         (if (== idx# bound#) (clojure.lang.PersistentArrayMap/createAsIfByAssoc ~arr)
             (do (aset ~arr inner# (aget ~ks idx#))
                 (aset ~arr (unchecked-inc inner#) (aget ~vs idx#))
                 (recur (unchecked-inc idx#)
                        (unchecked-add inner# 2))))))))

;;__Compression utilities__
;;#todo
;;  Add easy functions for writing to zipstreams (already in spork.util.zipfile)
;;  Extend general/line-reducer to account for zipped files.
;;  So far:
;;DEPRECATED  spork.util.general/compress-file!
(defn compress-file!
  [from & {:keys [type] :or {type :gzip}}]
  (general/compress-file! from :type type))

;;overloaded to allow compressed streams.
;;DEPRECATED: Now in spork.util.general/line-reducer
(defn line-reducer
  "Small wrapper around the original line-reducer.
   Now we can automatically grab lines from compressed files too."
  [path-or-string]
  (general/line-reducer path-or-string))

;;__Notes on compression__
;;Gzip is smaller (typically 3x less than lz4), but lz4 is typically much
;;faster (~3-5x), and still produces nice compression results.  70 mb vs
;;745...It also takes about 2.5 seconds to traverse the lines of either
;;compressed archive, and 22 seconds to read the allfillsfast.txt , so
;;IO is killing us.  Oh yeah, as a bonus, we can read all of the bytes
;;for the compressed archive into memory, buying us even more performance.

;;R and friends should be able to read from .gz natively (from what I've seen),
;;while .lz4 may require a separate lib (dunno).
;;Going forward, we may just ditch the fills folder and rip out what we need
;;from a compressed allfills.txt.gz|lz4 file.  Not sure yet.

;;generic
(defn load-trends
  [root]
  (->> (tbl/tabdelimited->records (str root "DemandTrends.txt")
         :parsemode :noscience :schema (assoc schemas/dschema :deltaT :int))
       (into [])))

;;sparkcharts and activity profile: if the entire demand record is the same as another, only one of the records will
;;remain in the activities
;;m4: only one of the duplicate demand records is kept (they all have the same demand name"
;;vba TADMUDI: duplicate demand records are counted
;;demand builder: not sure, but guess is that multiple vignette consolidated records create two demandrecords that are
;;exactly the same.  -the user should be notified in this case since duplicate demand records are not handled in m4 anymore.
(defn duplicate-demands
  "returns a sequence of demand names for which there are duplicate demand records. A demand name is a string defined like
  priority_vignette_SRC_[startday...endday]. Input is demand records."
  [drecs]
  (->> drecs
       (group-by (fn [{:keys [Priority Vignette SRC StartDay Duration]}]
              (str Priority "_"
                   Vignette "_"
                   SRC "_["
                   StartDay "..."
                   (+ StartDay Duration) "]")))
       (filter (fn [[dname recs]] (> (count recs) 1)))
       (map (fn [[dname recs]] dname))))

(defn parse-enabled
  "To prevent librecalc from converting false to =FALSE() which won't
  parse in spork, we use 'false, which will parse as \"false\" so now
  we need to read-string that."
  [{:keys [Enabled] :as r}]
  (assoc r :Enabled
         (if (string? Enabled)
           (read-string Enabled)
           Enabled)))
  
(defn xl->records
  "returns records from a table in a workbook within an Excel
  workbook."
  [wkbk-path wks-name]
    (->
     (xl/as-workbook wkbk-path)
     (xl/wb->tables :sheetnames [wks-name])
     ;;return the actual [sheetname table] tuple
     (first)
     ;;return the table
     (second)
     (tbl/keywordize-field-names)
     (tbl/table-records)
     ((fn [recs] (map parse-enabled recs)))))

(defn records-from
  "Returns records from an audit trail root dir, xl file, or
  if obj is already records, just returns obj. For now, input is the
  schema used to parse the txt file or the worksheet name for an xlsx
  file."
  [obj sheet-name schema]
  (if (or (instance? java.lang.String obj)
          (instance? java.net.URL obj))
    ;;Might need to cast java.new.URL to str for this to work.
    (let [extension (.toLowerCase (io/fext (str obj)))]
      (case extension
        "txt" 
        (load-records obj :schema
                      schema)
        "xlsx"
        (xl->records obj sheet-name)
        ))
    ;;otherwise, these are probably already records
    ;;keep load-records to check if they are indeed records
    (load-records obj)))

(defn enabled-records
  [obj sheet-name schema]
  (->> (records-from obj sheet-name schema)
       (filter (fn [{:keys [Enabled]}] Enabled))))
  
(defn resource-check "If the file doesn't exist as a local file path,
  check to see if it's on the resources path."
  [obj]
  (if (or (coll? obj) (instance? java.net.URL obj) (io/file? obj))
    obj
    (jio/resource obj)))

(defn make-record-fn
  "Define a function for us that returns records from a resource, path
  to an excel workbook, directory with a text file base on sheet-name,
  or just returns the obj if it is already a collection. If we want to
  only return the records that are :Enabled, then indicate so with enabled?."
  [enabled? sheet-name schema]
  (fn [obj]
    (let [record-getter (if enabled? enabled-records records-from)
          ;;to add the standard file name if needed.
          file-path (str obj "AUDIT_" sheet-name ".txt")
          checked-file (cond (or (coll? obj)
                                 (instance? java.net.URL obj))
                             obj ;;already cast
                             (or (io/folder? obj)
                                 (jio/resource file-path))
                             file-path
                             ;;Passing the path directly.
                           :else obj)]
            (record-getter (resource-check checked-file) sheet-name schema))))

(defmacro defrecord-getter [fn-name enabled? sheet-name schema]
  `(def ~fn-name
     (make-record-fn ~enabled? ~sheet-name ~schema)))

(defrecord-getter demand-records-init true "DemandRecords"
  schemas/drecordschema)

(defrecord-getter supply-records true "SupplyRecords"
  schemas/supply-recs)

(defrecord-getter period-records-init false "PeriodRecords"
  schemas/periodrecs)

(defrecord-getter parameter-records false "Parameters"
  schemas/parameters)

(defn demand-records
  "Return enabled DemandRecords and prints the number of duplicate
  demand records."
  [root]
  (let [drecs (demand-records-init root)
        dupes (duplicate-demands drecs)]
    (println "There are"(count dupes) "demand names with multiple
  demand records.")
    (println "There are " (- (count drecs) (count (into #{} drecs)))
            " duplicate demand records.")
    drecs))

;;Changed 
;this is a hack to ensure we are generating demand names just like Marathon (without Out-ofscope)
(defn inscope-srcs [path]
   (->> (tbl/tabdelimited->table (slurp path) :schema (fit-schema schemas/inscope-schema path))
       (tbl/table-records)
       (map :SRC)
       (set)))

(defn last-deactivation
  "Returns the day of the last inscope demand deactivation."
  [root]
  (let [inscopes (inscope-srcs (str root "AUDIT_InScope.txt"))]
  (->> (demand-records root)
       (filter (fn [r] (contains? inscopes (:SRC r))))
       (reduce (fn [acc {:keys [StartDay Duration]}] (max acc (+ StartDay Duration))) 0))))

(defn last-day-default 
  "Returns last day default from Parameters."
  [root]
  (let [last-day
        (->> (parameter-records root)
             (filter (fn [{:keys [ParameterName]}]
                       (= ParameterName "LastDayDefault")))
             (first)
             (:Value))]
    (case  (type last-day)
      java.lang.String (read-string last-day)
      ;;a big number.  Activities will be cut off at last
      ;;deactivation anyways
      nil 9999999
      ;;Already a number
      last-day)))
       
(defn last-day
  "Returns the last processed day of the simulation as computed by m4."
  [root]
  (inc (min (last-day-default root) (last-deactivation root))))

(defn replace-inf
  "Replace any inf ToDays in the PeriodRecords with
  the LastDayDefault for  now since we don't necessarily have
  AUDIT_InScope.txt to compute the last deactivation."
  [period-recs last-day-pulled]
  (map (fn [{:keys [ToDay] :as r}]
         (if (= ToDay "inf")
           (assoc r :ToDay last-day-pulled)
           r))
       period-recs))

(defn period-records-from
  "Returns the period records.  There is no Enabled for these!"
  [obj]
  (->> (period-records-init obj)
       (filter (fn [r] (not (= (:Name r) "Initialization"))))))

(defn audit-trail? "Can we access our marathon tables from either an
  audit trail on the filesystem or resources on the classpath?"
  [path]
  (let [audit-files (map (fn [file-name] (str path file-name))
                         ["AUDIT_Parameters.txt"])]
    (or
     (and (io/folder? path)
          (every? io/file? audit-files))
     (every? jio/resource audit-files))))
     
(defn tables-available? "Is this an audit-trail or m4 workbook path from
  which we can pull any of our m4 input tables from?"
  [obj]
  (let [extension (.toLowerCase (io/fext obj))]
    (or (= extension "xlsx") (audit-trail? obj))))
  
(defn period-records
  [obj]
  (let [period-recs (period-records-from obj)]
    (if (tables-available? obj)
      (replace-inf period-recs (last-day-default obj))
      period-recs)))

(defn dups-from
  "returns duplicate demands from an audit trail root dir"
  [root]
  (duplicate-demands (demand-records root)))

(defn period-map
  "Given period records, returns vectors of key, value pairs  where keys are the period names and vals are two item vectors with start and end of each period."
 [recs]
  (reduce (fn [acc {:keys [Name FromDay ToDay]}] (conj acc [Name [FromDay ToDay]])) [] recs))

(defn period-map-from
  "load a period map from a marathon audit trail. Compute last-day instead of using ToDay from period records."
  [root]
  (let [keyvals (period-map (period-records root))
        [nm [from to]] (last keyvals)
        lastperiod [nm [from (last-day root)]]]
    (into {} (conj (pop keyvals) lastperiod))))

;;Incanter patches.....
(in-ns 'incanter.io)
;;This is just a hack to allow us to parse 9-digit SRCs
;;without interpreting them as scientific numbers.
(defn parse-string [x & [default-value]]
  (if  (and (== (count x) 9)
            (= (nth x 5) \E))
    x
    (or (spork.util.parsing/parse-string x)
        default-value)))
(in-ns 'proc.util)

;;This is to try to speed up our damn charts...
;;proc.stacked/dwell-over-fill doesn't need the
;;entire fills table....in fact, we ignore
;;most of the table entirely.  So, we spend more
;;time parsing and garbage collecting useless
;;crap.  Also, we can use string 
(def fill-schema
  {:fill-type         :text
   :DwellBeforeDeploy :int
   :Component   :text 
   :DwellYearsBeforeDeploy :float
   :DemandGroup :text
   :Period      :text
   :sampled     :text
   :start  :int
   :duration :int
   :quantity :int
   })

(def deploy-schema
  {:DeployInterval :int
   :DwellYearsBeforeDeploy :float 
   :Component :text
   :FollowOnCount :int
   :Demand :text
   :DemandType :text
   :Period :text})

