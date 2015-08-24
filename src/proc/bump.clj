;;This is a quick example in clojure, using utilities in spork, 
;;of how we could be computing bump charts automatically.  We 
;;either read from Excel or several text files, and build 
;;a table of dwell-bands, by src, by period, by compo.
(ns proc.bump
  (:require [spork.util.excel [core :as xl]]
            [spork.util.table :as tbl]
            [spork.util.io :as io]))

;;IO and Helper Functions
;;=======================

;;Grabbing data from Excel and friends...
;;declare lets us reserve some temporary (currently useless) symbols
;;so that the compiler doesn't complain.  We have to be responsible
;;to actually define what these symbols mean by the time we actually 
;;"use" them, i.e try to evaluate them.  For now, they are handy stubs
;;for some as-of-yet undefined functions.
(declare xl->samples path->samples)

;;We'll define a simple function that lets us determine if we're
;;trying to read a resource that may point to a workbook.  Right now,
;;only workbooks are implemented (although text files are simple using
;;spork.util.table too.
(defn res-type [x] 
  (cond  (and (string? x) (.contains x "xls")) :workbook-path
         (string? x) :path
         (coll? x)  :multiple-paths
         :else (throw (Exception. (str "No idea how to process that dude...." x)))))

;;To make it easy to define ad-hoc resource types, we define a
;;multimethod called get-samples, which changes the implementation
;;depending on what (res-type) returns as its result.  This is kind of 
;;like a super function, with the ability to dispatch to other
;;functions depending on some arbitrary logic.  Note: we could do the 
;;same thing in a function with a big case or cond statement; in that 
;;scenario, we'd just add conditions and branches.  Multimethods take 
;;care of that for us, and allow us to just define different ways to 
;;handle specific cases.  Note: they are slower than normal functions, 
;;so don't use them inside performance intensive areas. 
(defmulti   get-samples (fn [x] (res-type x)))
(defmethod  get-samples :workbook-path   [wbpath]     (xl->samples wbpath))
(defmethod  get-samples :path            [filepath]   (path->samples filepath))
(defmethod  get-samples :multiple-paths  [xs]         (mapcat get-samples xs))

;;Our first helper function scrapes a workbook, looking for a
;;worksheet named "data", and extracts the contiguous information from
;;that sheet into a spork.util.table table.  We expect that the field
;;names are in the first column, and that the data is contiguous
;;(i.e. if we selected the current-region in excel, it would contain
;;the data).
(defn xl->samples [wbpath] (-> wbpath 
                               (xl/xlsx->tables  :sheetnames ["data"] :ignore-dates? true)
                               (get "data")
                               (tbl/keywordize-field-names)))

;;Now that we can suck data in from an arbitrary workbook, we can use 
;;get-samples to pull in data from workbook paths and bind that data 
;;to tables.

;;For our purposes, we need, at least, some supply data and some
;;dwell samples to read in.  The formats are explained below.

;;I'm pulling from two different workbooks, we could just as easily
;;have a single workbook setup, with data residing on different
;;sheets.
(defn default-dwell-samples []  (get-samples ".\\resources\\sampledata.xlsx"))
(defn default-supply-data    []  (get-samples ".\\resources\\supplydata.xlsx"))

;;If we call (default-dwell-samples), and bind it, we get a table of
;;records with these fields (per the source data):
;; (def dwell-fields 
;;   [:Run
;;    :DeploymentID
;;    :Location
;;    :Demand
;;    :DwellBeforeDeploy
;;    :BogBudget
;;    :CycleTime
;;    :DeployInterval
;;    :DeployDate
;;    :FillType
;;    :FillCount
;;    :UnitType
;;    :DemandType
;;    :DemandGroup
;;    :Unit
;;    :Policy
;;    :AtomicPolicy
;;    :Component
;;    :Period
;;    :FillPath
;;    :PathLength
;;    :FollowOn
;;    :FollowOnCount
;;    :DeploymentCount
;;    :Category
;;    :DwellYearsBeforeDeploy
;;    :OITitle])

;;Since the data is stored in a different format, 
;;let's keep things consistent.  We know that DemandType is really 
;;SRC, and we're using Compo as a shorthand for Component:
(def dwell-samples 
  (->>  (default-dwell-samples)
        (tbl/rename-fields {:DemandType :SRC
                            :Component  :Compo})))

;;We also read in supply, which is a table with fields [:SRC :Compo :Qty].
;;These will be used to weight our supply during the cumulative 
;;density function computation.
(def supply        (default-supply-data))


;;Computing Average Dwell
;;=======================

;;Since the focus of our effort is on computing bump-charts, we 
;;need to organize the data by src, component, and period.  We
;;actually  have another dimension in the dwell samples, Run, 
;;which we want to aggregate out (via averaging).  So  if we 
;;create a compound key for [src compo period], and use that 
;;as a basis for computing statistics for each record in 
;;dwell-samples, then we can accomplish our task pretty easily.

;;dwell-key is just a convenience function around another 
;;useful clojure function called juxt.  juxt will take one or 
;;more functions as arguments, and then return a function that, 
;;when applied to a single argument, returns a vector where 
;;each entry is the application of the input sequence of functions 
;;applied to the argument.  So, ((juxt odd? even?) 2) => [false true]
;;juxt is useful for creating compound keys, which we can then store 
;;very easily in a hashmap.
(def dwell-key (juxt :SRC :Compo :Period))

;;We want to aggregate our dwell statistics across run, but the 
;;basis needs to be in [src, compo, period].  Using dwell-key above, 
;;and reduce, we can reduce over the records in the table.  Each 
;;record has the same keys as the table fields.  runs->dwell-map 
;;reduces the multiple-run data (read in from our dwell data
;;workbook), and turns it into a map of  
;;{[SRC Compo Period] [number-of-amples sum-of-dwell}.
;;This will be the minimal data we need to compute an average.
(defn runs->dwell-map [tbl]
  (->> tbl 
       (reduce (fn [acc r]
                 (let [k (dwell-key r)
                       dwell (:DwellYearsBeforeDeploy r)]
                   (if-let [oldv (get acc k)]
                     (let [[n sum] oldv]
                       (assoc! acc k [(inc n) (+ sum dwell)]))
                     (assoc! acc k [1 dwell]))))
               (transient {}))       
       (persistent!)))

;;We can transform the dwell-map into a sequence of
;;average-dwell-records pretty easily.  We simple reduce over 
;;the hashmap (using reduce-kv, ask me why sometime).  We destructure
;;the data and compute a corresponding record with and entry for the
;;average dwell.  Note that this average is relative to the
;;[src,compo,period] key we computed earlier, and have embedded back
;;into the record.  It's the average across all the runs.
(defn average-dwell-records [dwell-map]
  (reduce-kv (fn [acc [src compo period] [n total]]
               (conj acc 
                     {:SRC src :Compo compo :Period period :n n :total total 
                      :average  (float (/ total n))})) []  dwell-map))

;;Computing Bump Charts or Peculiar CDFs
;;======================================
;;Bump charts are "really" derived from a CDF.  We compute the CDF and 
;;pick specific coordinates along the CDF to sample from.  The CDF is
;;drawn relative to a population of average dwell samples.

;;By default, we have a non-uniform set of density points to sample. 
(def +default-widths+ [0 0.125 0.25 0.5 0.75 0.875 1.0])
;;"Binning" within adjacent points implies membership in a labeled
;;partition.  These labels imply the population's deviation from th
;;median.  Default labels for the default partitions are:
(def +default-labels+   
  (zipmap (partition 2 1 +default-widths+)
          [:L3 :L2 :L1 :R1 :R2 :R3])) 

;;Evaluating the CDF at the points of interest is pretty simple.
;;If we can evaluate it, then we can derive the bands by 
;;examining pairwise-adjacent points on the CDF.

;;Given a sequence of numeric samples, and the CDF points (widths),
;;we can compute the bump-chart bands. 
(defn compute-bands [xs widths]
  (let [xs     (sort  xs) 
        n      (count xs)
        widths (sort widths)]
    (->> widths
         (map (fn [w]                 
                [w (nth xs (quot (* w (dec n)) 1))]))
         (partition 2 1)
         (map (fn [[[lb ld] [rb rd]]]  [[lb rb] [ld rd]])))))

;;We also typically label the bands, so given a computed set 
;;of bands, we can label them with an arbitrary map of labels.
(defn label-bands [labels bands]
  (mapv (fn [[lbl band]]
            [(get  labels lbl :unlabeled) band])
        bands))

;;We typically deal with a weighted population, where weight 
;;corresponds to the quantity of an [src compo] in supply.

;;In general, we can expaned a set of samples into weighted samples
;;in a simple, naive fashion.  Assume some of the samples are numbers,
;;and some are [sample weight] pairs.  We can build up a 
;;list of weighted (i.e. repeated) samples trivially.
;;=>(weighted-samples [0 [1 2] 2 [3 2] 4 [5 2] 6 [7 2] 8 [9 2])
;;[0 1 1 2 3 3 4 5 5 6 7 7 8 9 9]
(defn weighted-samples [xs]
  (->> xs
       (reduce 
        (fn [acc x]          
          (if-let [n (and (coll? x) (second x))]
            (let [x (first x)]
              (loop [idx 0
                     acc acc]
                (if (== idx n) acc
                    (recur (unchecked-inc idx) 
                           (conj! acc x)))))
            (conj! acc x)))
        (transient []))
       (persistent!)))

;;Note, we don't actually have to expand the supply like they do 
;;to compute the weighted-index.  For our purposes, it's okay though.


;;Computing Labeled Bands by Compo and Period
;;===========================================

;;Assuming we have a sequence of average dwell records, as 
;;produced by average-dwell-records, we can produce labeled 
;;dwell bands by [compo period] 
(defn average-dwell-map->band-map
  [dwell-records & {:keys [widths labels weightfn] 
                :or {widths +default-widths+
                     labels +default-labels+
                     weightfn (fn [x] 1)}}]
  (into {} 
        (for [[compo periods] (group-by  :Compo   dwell-records)
              [period recs]   (group-by  :Period  periods)]
          (let [dwells (map (fn [r] [(:average r)
                                     (weightfn r)]) recs)]
            [[compo period]
             (label-bands labels
                          (compute-bands 
                           (weighted-samples dwells) widths))]))))

;;Computing Percentages
;;=====================
;;exercise for the reader.

(defn compute-band [x ordered-bands]
  (reduce (fn [acc [lbl [l r]]]
               (if (and (>= x l)
                        (<= x r))
                  (reduced [lbl (float (/ (- x l) (- r l)))])
                  acc))
             nil 
             ordered-bands))

;;given a dwell record, and a band, 
;;turn it into a record of {SRC compo-period percentage band average}
(defn oi-records [dwell-recs band-map]
  (let [compo-periods  (distinct (map (juxt :Compo :Period) dwell-recs))]
    (->  (flatten 
           (for [[src xs]            (group-by :SRC dwell-recs)
                 [compo-period recs] (group-by (juxt :Compo :Period) xs)]       
             (let [band (get band-map compo-period)]
               (for [rec recs] 
                 (let [avg               (:average     rec) 
                       [band percentage] (compute-band avg band)]
                   {:SRC src 
                    :compo-period compo-period
                    :percentage   percentage
                    :band         band
                    :average avg})))))
          (with-meta {:compo-periods  compo-periods}))))


;adds the appropriate percentage fields
(defn compute-fields 
 [{:keys [compo-period percentage band average]}]
 (let [k (clojure.string/join "-" compo-period)]
   {k               average
    (str k "-%")    percentage
    (str k "-Band") band}))

(defn extended-fields [compo-periods]
  (mapv (fn [[compo period]] 
          (str compo "-" period)) compo-periods))
(defn full-fields [xfields] 
  (vec (mapcat (fn [k] [k (str k "-%") (str k "-Band")]) xfields)))


(defn oi-records->full-records
  [oirecs]
  (let [fields (into [] (full-fields (extended-fields (:compo-periods (meta oirecs)))))]    
    (for [[src recs] (group-by :SRC oirecs)]
      (persistent!
       (reduce (fn [uber-rec r]               
                 (let [computed (compute-fields r)]
                   (reduce (fn [acc fld]
                             (assoc! acc fld (get computed fld nil)))
                           uber-rec
                           fields)))
               (transient {"SRC" (get (first recs) :SRC)}) recs)))))

;;It looks like the merge order is off here...
(defn oi-records->table [oirecs]  
  (let [rtable  (tbl/records->table (sort-by #(get % "SRC") (oi-records->full-records oi-recs)))
        fields  (vec (cons "SRC" (sort (disj (set (tbl/table-fields rtable)) "SRC")))) ]
    (tbl/select-fields  fields rtable)))


                    
                    

                
                     
  
     
     

  
