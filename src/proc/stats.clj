
(ns proc.stats
  (:require [spork.util.table :as tbl]
            [proc.sporkpatches]
            [proc.util :as util]))

(defn txt? [^String nm] (.contains nm ".txt"))

(defn interest->filename 
  "Expects k to be a keyword"
  [k]
  (str "/" (subs (str k) 1) ".txt"))

(def fill-names {:BCT "/BCT.txt"})

;;Compute aggregate fills 
;;Given a path to different project folders, 
;;build a dataset of each file in fills, 
;;naming the case as the path, and the interest as 
;;the filename.
;interests are optional keyword arguments like :BCT :DIV
(defn build-fill-tables [name-paths & interests]
    (->>  (for [[nm root] name-paths
                [fill-file tr] (if (empty? interests)
                                 (map (juxt spork.util.io/fpath spork.util.io/fname) (spork.util.io/list-files root))
                                 (map (fn [interest] [(str root (interest->filename interest)) interest]) interests))
                :when (txt? fill-file)]
          (let [hdrs (clojure.string/join \tab (map (fn [x] (subs x 1)) (proc.util/raw-headers fill-file)))
                fd (with-open [rdr (clojure.java.io/reader fill-file)]
                     (spork.util.table/lines->table (cons hdrs (rest (line-seq rdr))) :schema proc.util/fillr))
                trend (clojure.string/replace tr ".txt" "")
                _ (println trend)]
            (->>  (spork.util.table/table-records fd)
                  (map (fn [r] (assoc r :name nm :trend trend))))))
       (reduce concat)
       (spork.util.table/records->table)
        ))

(defn spit-fill-stats [t root & {:keys [out] :or {out "fillstats.txt"}}]
  (with-open [out (clojure.java.io/writer (str root out))]
    (doseq [ln (util/table->lines t)]
      (util/writeln! out ln))))
  
(defn fill-stats [xs]
  (let [missed   (reduce + (map (fn [x] (* (inc (:duration x)) (:quantity x))) 
                                (filter (fn [x] (= (:compo x) "Ghost")) xs)))
        total   (reduce + (map (fn [x] (* (inc (:duration x)) (:quantity x))) 
                                xs))]
    {:missed  missed
     :total    total
     :proportion (float (/ missed total))}))

(defn weighted-sample-by 
  "(weighted-sample-by xs valfd weightfd)
   Takes a sequence of records and computes the value in the valfd field multiplied by the value in the weightfd
   field for each record.  Returns the sum of these computations."
   [xs valfd weightfd]
  (reduce + (map (fn [x] (* (valfd x) (weightfd x))) xs)))

(defn average-by [valkey xs]
  "(average-by valkey xs)
   Takes a sequence of records and computes the average of the values in the valkey column. If xs is empty, returns
   nil."
  (if (not (empty? xs)) (double (/ (reduce + (map (fn [x] (valkey x)) xs)) 
            (count xs)))))

(defn get-gen-fills [recs]
  (->> (util/filter-by-vals :DemandGroup not= ["NotUtilized"] recs)
    (util/filter-by-vals :FollowOn = [false "false"])))
  
(defn dwell-stats
    "This stats function is intended for records pulled from post processor 'fills' folder.
     (dwell-stats recs compos)
     Returns a map where the keys are :compo+avgdwell and the values are the average dwells for each
     compo."
  [recs & {:keys [compos] :or {compos ["AC" "NG"]}}]
  (let [compfs (group-by :compo (get-gen-fills recs))]      
    (zipmap (map #(keyword (str % "avgdwell")) compos) (->> (map #(get compfs %) compos)
                                                         (map #(util/filter-by-vals :dwell-plot? = [true "true"] %))
                                                         (map #(average-by :DwellYearsBeforeDeploy %))))))


(def bcts {"SBCTs" ["47112R000"] "IBCTs" ["77302R500" "77302R600"] "ABCTs" ["87312R000"]})

(defn sub-stats 
  "This stats function is intended for records pulled from post processor 'fills' folder.
  (sub-stats dmdsbysrc subtypes subd)
  Returns a map with vals as the percent of substitutions accounted for by each SRC demandtype grouping in subtypes."
  [dmdsbysrc subtypes subd]
  (if (not (zero? subd))
    (let [subdays (->>(map #(reduce concat (map dmdsbysrc %)) (vals subtypes))
                    (map #(weighted-sample-by % :deltat :quantity)))]
      (merge (zipmap (map #(keyword (str % "ubdays")) (keys subtypes)) subdays)
             (zipmap (map #(keyword (str "perc" % "ubs")) (keys subtypes))
                     (map #(double (/ % subd)) subdays))))))

(defn more-stats 
  "This stats function is intended for records pulled from post processor 'fills' folder.
   (more-stats recs subtypes)
   Returns a map with values of average dwell stats for each compo, percent substitutions, percent demand
   satisfied, and the percent of substitutions accounted for by each SRC demandtype grouping in subtypes."
  [recs & {:keys [subtypes] :or {subtypes bcts}}]
  (let [genuinefills (get-gen-fills recs)  
        realfills (reduce concat (map (group-by :compo genuinefills) ["AC" "NG"]))
        subs (util/filter-by-vals :FillType = ["Substitute"] realfills)
        dmdtypes (group-by :DemandType subs)
        [rf sub gf] (map #(weighted-sample-by % :deltat :quantity) [realfills subs genuinefills])]
    (merge (dwell-stats recs) (sub-stats dmdtypes subtypes sub) 
           {:percsubs  (if (not (zero? rf)) (double (/ sub rf))) :subdays sub :filldays rf  :demandays gf
            :percsat   (if (not (zero? gf)) (double (/ rf gf)) 0)})))

(defn fill-table
  ([t statsf grpkey & subtypes]
    (->>  (for [[tr xs] (group-by grpkey (tbl/table-records t))]
            (assoc (if (empty? subtypes) (statsf xs) (statsf xs (first subtypes))) grpkey tr))
      (tbl/records->table)))
  ([t] (fill-table t fill-stats :trend bcts)))






(def more-stats-order [:name :demandays :filldays :percsat :ACavgdwell	:NGavgdwell :subdays :percsubs	
                       ;:IBCTsubdays :percIBCTsubs :SBCTsubdays :percSBCTsubs 
                       ;:ABCTsubdays :percABCTsubs	;commented these out in case we don't have BCT subs
                       ])
;I think if we have substitutions, we want them to create a new field for it

(defn get-stats-from [name-paths destpath & {:keys [interest] :or {interest :BCT}}]
  (let [stable (-> (build-fill-tables name-paths interest)  ;;if take out BCTs, get an out of memory error with all interests
                 (fill-table more-stats :name))]
    (spit-fill-stats (tbl/order-fields-by more-stats-order stable) 
      ;stable
                     destpath)))

(def behruns {"RunName" "path1"
              "RunName2" "path2"})

;_____________ stats revamped.  I think from here down, this isn't working right now, and it's not a priority.  
;I might come back to this later
;if we need it.
;probably better to make two tables with two different functions like:

;This table might make more sense:
; (def stats-by-demand-type ":name	:demandtype	:filler	:demandays	:filldays	:percsat	:ACavgdwell	:NGavgdwell	:percsubs	:percdmdtype
;Runame1	77302R500	77302R500	same	(just filler fill)	:filldays/:demanddays	just filler dwells	just filler dwells	:fillsdays/ total :subdays  for run	:filldays/total :filldays for this demandtype
;Runame1	77302R500	77302R600	same	just filler fill	:filldays/:demanddays	just filler dwells	just filler dwells		
;Runame1	77302R500	77302K000	same	just filler fill	:filldays/:demanddays	just filler dwells	just filler dwells"	)	
 
;where :demandtype and :filler could be by interest, too

;along with a comprehensive table like 

;	(def run-stats					
;":name	:demandays	:filldays	:percsat	:ACavgdwell	:NGavgdwell	:subdays
;Runame1					
;Runame1		")				


(defn dwell-stats2
  "This stats function is intended for records pulled from post processor 'fills' folder.
     (dwell-stats recs compos)
     Returns a map where the keys are :compo+avgdwell and the values are the average dwells for each
     compo."
  [recs & {:keys [compos] :or {compos ["ACavgdwell" "NGavgdwell" "RCavgdwell"]}}]
  (let [compfs (group-by :compo (get-gen-fills recs))
        avgs (reduce-kv (fn [acc compo xs] (assoc acc (keyword (str compo "avgdwell"))
                                                  (->> (util/filter-by-vals :dwell-plot? = [true "true"] xs)
                                                    (average-by :DwellYearsBeforeDeploy)))) {} compfs)]
    
    (into {} (map (fn [k] [k (avgs k)]) compos))))


(defn stats-by-dmdtype
  "We would probably like to build a table with stats for each demandtype SRC for each run
This stats function is intended for records pulled from post processor 'fills' folder.
   (more-stats recs subtypes)
   Returns a map with values of average dwell stats for each compo, percent substitutions, percent demand
   satisfied, and the percent of substitutions accounted for by each SRC demandtype grouping in subtypes."
  [recs & {:keys [subtypes] :or {subtypes bcts}}]
  (let [genuinefills (util/filter-by-vals :DemandGroup not= ["NotUtilized"] recs)
        filldays (group-by :DemandType recs)]
  (->> (group-by (juxt :DemandType :SRC) recs)
                 (map (fn [[demand filler] xs] 
                              (let [
                                    realfills (util/filter-by-vals :fill-type = ["Filled"] xs)
                                    subs (util/filter-by-vals :FillType = ["Substitute"] realfills)
                                    [rf sub gf] (map #(weighted-sample-by % :deltat :quantity) [realfills subs genuinefills])]
            (merge (dwell-stats2 xs)
            {:percsubs  (if (not (zero? rf)) (double (/ sub rf))) :subdays sub :filldays rf  :demandays gf
            :percsat   (if (not (zero? gf)) (double (/ rf gf)) 0)
            :DemandType demand})))))
               ;   I think I stopped revamping around here.              
                                

                          
     ;(sub-stats dmdtypes subtypes sub) 
           ))

(defn fill-table2
  [t statsf grpkey]
    (->>  (for [[tr xs] (group-by grpkey (tbl/table-records t))]
            (map (fn [r] (assoc r grpkey tr)) (statsf xs))) ;returns a bunch of records now
      (reduce concat) 
      (tbl/records->table)))


(defn get-dmdtype-stats [name-paths destpath]
  (let [stable (-> (build-fill-tables name-paths)  ;;if take out BCTs, get an out of memory error with all interests
                 (fill-table2 more-stats :name))]
    (spit-fill-stats (tbl/order-fields-by more-stats-order stable) 
      ;stable
                     destpath :out "fillstats2.txt")))

