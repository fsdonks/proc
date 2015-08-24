;;temporary storage until we get into spork.
(ns proc.util
  (:require [spork.util.io    :as io]
            [spork.util.table :as tbl]
            [clojure.edn]
            [incanter.core :refer :all]
            [incanter.io :refer :all]))

;;to keep from having to reload datasets that may be expensive, we can
;;keep a cache of recently loaded items.
;(defn recently-loaded 

;;I'm using this instead of the java interfact in java.io   
(defprotocol ICloseable 
  (close [x]))

(defn un-key [keywords]
  (mapv (fn [k] 
          (keyword (apply str (drop 2 (str k))))) keywords))

(defn read-keyed [path] 
  (let [d (read-dataset path :header true :delim \tab :keyword-headers false)
        names (into {} (map (fn [k] [k (clojure.edn/read-string k)])  (:column-names d)))]
    (rename-cols  names d)))



(defn writeln! [^java.io.BufferedWriter w ^String ln]
  (do  (.write w ln)
       (.newLine w)))

(defn write! [^java.io.BufferedWriter w ^String ln]
  (do  (.write w ln)))



;;This lets us extract unfilled-trends from demand-trends.
(defn first-line [path]
  (with-open [rdr (clojure.java.io/reader path)]
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


(defn distinct-zipped [n-colls]
   (let [knowns   (atom (mapv (fn [i] (transient #{})) (range (count (first n-colls)))))
         add-row (fn [xs] (reduce (fn [idx x]
                                    (let [known (nth @knowns idx)]
                                      (do (when (not (known x)) 
                                            (swap! knowns assoc idx (conj! known x)))
                                          (unchecked-inc idx))))
                                  0
                                  xs))]
     (do (doseq [xs n-colls]  (add-row xs))
         (mapv persistent! @knowns))))
;;what we want is a multimap...
;;a map of streams we can push to.
;;that are keyed..

(declare close-all!)
(defrecord multistream [^String root filename  childname ^String headers ^clojure.lang.Atom writers]
  ICloseable 
  (close [x] (close-all! x)))

(defn mstream [root name headers & {:keys [childname] :or {childname (fn [x] (str x ".txt"))}}]
  (->multistream root name childname headers (atom {})))

(defn ref? [obj] (instance? clojure.lang.IDeref obj))

(defn ^java.io.BufferedWriter get-writer! [^multistream ms nm]
  (if-let [w (get (deref (.writers ms)) nm)]
    w
    (let [newfile (str (.root ms) "/" ((.childname ms) nm))
          _       (io/hock newfile "")
          ^java.io.BufferedWriter w  (clojure.java.io/writer newfile)
          _  (println [:writing-to newfile])
          _  (swap! (.writers ms) assoc nm w)
          _  (writeln! w (if (ref? (.headers ms)) (deref (.headers ms)) (.headers ms)))]
      w)))

;; on close, we want to record a manifest, in the root folder, of the 
;;files in the multistream, so that we can read the manifest and get a 
;;corresponding multireader of it.
(defn close-all! [^multistream ms] 
  (let [ws (deref (.writers ms))
        root (.root ms)
        manifest {root (vec (keys ws))}]
    (do (doseq [[nm ^java.io.BufferedWriter w] ws]
          (.close w))
        (reset! (.writers ms) {}) ;closed writers.
        (spork.util.io/hock (str (.root ms) "/" (.filename ms)) (str manifest)))))

(defn write-in! [^multistream ms k v]
  (write! (get-writer! ms k) (str k)))
(defn writeln-in! [^multistream ms k v]
  (writeln! (get-writer! ms k) (str k)))

(defmacro write! [w ln]
  (let [w (vary-meta w assoc :tag 'java.io.BufferedWriter)
        ln (vary-meta ln assoc :tag 'String)]
    `(doto ~w (.write ~ln))))

(defmacro new-line! [w]
  (let [w (vary-meta w assoc :tag 'java.io.BufferedWriter)]
    `(doto ~w (.newLine ))))

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
  

(defn table->lines [t]
  (let [cols (tbl/table-columns t)]
    (cons (clojure.string/join \tab (tbl/table-fields t))
          (map
           (fn [idx] 
             (clojure.string/join \tab (map #(nth % idx) cols)))
           (range (tbl/record-count t))))))

(defn spit-table [path t]
  (with-open [^java.io.BufferedWriter out (clojure.java.io/writer path)]
    (doseq [^String ln (table->lines t)]
      (writeln! out ln))))

(defn fit-schema [s path]
  (let [hs (map (fn [kw] (subs (str kw) 1)) (get-headers path))]
    (reduce (fn [acc fld]
              (if-let [res (or (get acc fld)
                               (get acc (keyword fld)))]
                acc 
                (assoc acc fld :text)))
            s hs)))         
