
;;Namespace to create and manipulate fills files.  Can add fields to fills files, merge fills files, etc.
(ns proc.fillsfiles
  (:require 
            [spork.util.table :as tbl]
            [proc.schemas :as schemas]
            [proc.interests :as ints]
            [proc.util :as util]
            [clojure.string :as str]
            [proc.example :refer [run-sample!]])
  (:use [proc.core]
        [incanter.core]
        [proc.constants :as consts]))


(defn one-fill 
  "make one fills table out of multiple fills files"
  [fillpaths] 
  (let [headstr (with-open [rdr (clojure.java.io/reader (first fillpaths))]
                  (first (line-seq rdr)))
        headers (map keyword (str/split (str/replace headstr #":" "") #"\t"))] ;use util/get-headers
    (->> (map util/as-dataset fillpaths)
      (apply conj-rows)
      (:rows)
      (tbl/records->table)
      (tbl/order-fields-by (vec headers)))))

(defn get-fill-paths 
  "returns the path for all fills files generated from the most recent run-sample!"
  [root]
  (let [fsregex (->> (slurp (str root "fills/fills.ms.edn"))
                  (re-find #"\[([^]]+)\]")
                  (second))
        splits (str/split (str/replace fsregex #"\"" "'") #"' '")
        fnames (map #(str/replace % #"'" "") splits)] ;escape character is finicky here
    (map #(str root "fills/" % ".txt") fnames)))

(defn consolidate-fills-in 
  "We want to take all fills files and make one fill file for a marathon run"
  [root]
  (util/spit-table 
    (str root "fills/allsrcs.txt") 
    (one-fill (get-fill-paths root)))) 
                         
(defn hix-bounds [dwelldays compo]
  (if (= compo "AC")
        (cond (< dwelldays 91) "T4"
          (< dwelldays 146) "T3"
          (< dwelldays 181) "T2"
          :else "T1")
        (cond (< dwelldays 226) "T4"
          (< dwelldays 364) "T3"
          (< dwelldays 451) "T2"
          :else "T1")))

(defn simultaneity-bounds [dwelldays compo]
  (if (= compo "AC")
        (cond (< dwelldays 90) "T4"
          (< dwelldays 145) "T3"
          (< dwelldays 180) "T2"
          :else "T1")
        (cond (< dwelldays 731) "T2"
          :else "T1")))
  
(defn train-level [r]
  (let [dwelldays (:DwellBeforeDeploy r)
        compo (:compo r)]
    (when (and (:dwell-plot? r) (= (:fill-type r) "Filled"))
      (simultaneity-bounds dwelldays compo))))

;Actually, I think the start day we want is the first day that SRC is demanded,
;which might not be the minimum time for PHII....
(defn get-ph2-start 
  "Returns an integer for the start day of PHII"
  [dmd dmdrecs]
  (let [starts consts/starts
        drecs (filter #(and (= (:DemandGroup %) dmd) (if (contains?  starts (:DemandGroup %))
                                                       (= (:Operation %) (starts (:DemandGroup %)))
                                                       true ;return the lowest start day then
                                                       )) dmdrecs)
        ts (map :StartDay drecs)]
    (if (not (empty? ts)) (apply min ts))))
  
(defn get-phase2-starts 
  "Returns a map with keys of demandgroup mapping to the starts of PHII"
  [root]
  (let [dmdrecs (tbl/table-records (tbl/tabdelimited->table (slurp (str root "AUDIT_DemandRecords.txt")) :parsemode :noscience 
                                                     :schema schemas/drecordschema))
        all-demand-groups (set (map :DemandGroup dmdrecs))]
    (reduce (fn [acc dmd] (assoc acc (str/upper-case dmd) (get-ph2-start dmd dmdrecs))) {} all-demand-groups)))

;;it would probably be better to scrape the fills folder for all SRC
;;filenames instead of get-fill-paths which will pull the fill path
;;filenames from the fills edn file which holds filenames for the
;;latest run-sample! run.
;;Tom: rewrote this to use the tbl/records->file function, which
;;delegates all of the appending and manual work to spork.util.stream,
;;which streams sequences of records in a consistent tab-delimited format
;;defined by the caller, handles appending and all that.  trying to prune
;;the definition from proc.sporkpatches...
(defn fills+fields
  "Spits out a new fills file which is a subset of the fills plus additional fields" 
  [root]
  (let [ph2starts (get-phase2-starts root)
        paths     (get-fill-paths root)
        outpath   (str root "allfills+fields.txt")
        fillpath->recs
          (fn [p] 
            (files->records [p] :computed-fields
                  (assoc {:TrainLevel train-level
                  ;had to upper-case since fills have UnGrouped where demand records are Ungrouped
                          :DaysAfterPHII
                          (fn [r]
                            (if-let [ph2start (get ph2starts (str/upper-case (:DemandGroup r)))]
                              (- (:start r) ph2start)))}
                         :run (fn [r] root))))]
    (-> (mapcat fillpath->recs paths)
        (tbl/records->file outpath))))

(defn samples-and-fills+ [root]
  (#(do (run-sample! % :eachsrc true) (fills+fields %)) root))


(defn all-fills
  [root & {:keys [outpath] :or {outpath "allfillsfast.txt"}}]  
  (proc.core/files->file-complete (ints/src-fill-paths root) (str root outpath)))

(defn bar-fills 
  "Makes the barfills.txt input file for Mike Dinh's R barcharts script.  This defaults to only including fills filenames which 
resemble a 9 digit src. Bind ints/*include-fn* to another fn in order to include other filenames."
  [root]
  (let [rfields [":compo" ":OITitle" ":DemandGroup" ":operation" ":start" ":end" ":quantity" ":fill-type" 
                          ":DwellBeforeDeploy" ":deltat" ":DemandType" ":SRC"]]
    (proc.core/files->file (ints/src-fill-paths root) (str root "barfills.txt") :keptfields rfields)))

(defn bar-fills-not-all-9-digit-srcs [root]
  (binding [ints/*include-fn* identity] 
    (bar-fills root)))

(defn add-title 
  "append title 32 or title 10 data to our fills.  Work in progress."
  [fillroot]
  (let [lines (tbl/lines->records fillroot)]))
