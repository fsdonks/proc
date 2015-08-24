;;This is a sample of stuff we can do using the post processor.
(ns proc.example
  (:require [proc.stacked :as stacked]
            [spork.util.table :as tbl]
            [spork.util.excel.core :as xl]
            [clojure.core.reducers :as r]
            [proc.schemas :as schemas]
            [proc.sporkpatches]
            [clojure.pprint :as ppr])
  (:use [proc.core]
        [incanter.core]
        [incanter.io]
        [incanter.charts]))

;;(def test-path "C:/Users/heather.a.jackson/Desktop/SRM/SRM SSW Policy/")
;; (def test-deployments 
;;   "C:/Users/heather.a.jackson/Desktop/SRM/SRM SSW Policy/AUDIT_Deployments.txt")

(def test-path "C:/Users/thomas.spoon/Documents/SRM Comparison/FORSCOM/")
(def test-deployments   (str test-path "AUDIT_Deployments.txt"))

;;This dumps out our fills and sandtrends for the interesting srcs.
(defn run-sample! 
  ([path]
     (only-by-interest [:TRUCK :ENG :CSSB :DIV :GSAB :SAPPER :MP :CAB :ATTACK :BCT :CA :All] 
        (sandtrends-from path)))
  ([] (run-sample! test-path)))

;;One function to make the files for the charts and display the charts at the same time
(defn charts-from-unproc-run
  ([path]
    (do (run-sample! path)
        (do-charts-from path)))
  ([] (charts-from-unproc-run test-path)))
  
(defn deployments 
  ([int]
    (view (deployment-plot test-deployments (get interests int))))
  ([] (view-deployments :BCT)))

(defn deployment-chart
  ([int]
     (deployment-plot test-deployments (get interests int)))
  ([] (deployment-chart  :BCT)))

(defn txt? [^String nm] (.contains nm ".txt"))

(def fillr (into {} (for [[k v] schemas/fillrecord] [ (str k ) v])))

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
                                 (map (fn [interest] [(str root (interest fill-names)) interest]) interests))
                :when (txt? fill-file)]
          (let [hdrs (clojure.string/join \tab (map (fn [x] (subs x 1)) (proc.util/raw-headers fill-file)))
                fd (with-open [rdr (clojure.java.io/reader fill-file)]
                     (spork.util.table/lines->table (cons hdrs (rest (line-seq rdr))) :schema fillr))
                trend (clojure.string/replace tr ".txt" "")
                _ (println trend)]
            (->>  (spork.util.table/table-records fd)
                  (map (fn [r] (assoc r :name nm :trend trend))))))
       (reduce concat)
       (spork.util.table/records->table)
        ))

  
(def runs {"FORSCOM" "C:/Users/thomas.spoon/Documents/SRM Comparison/FORSCOM/fills"
           "SSW"     "C:/Users/thomas.spoon/Documents/SRM Comparison/SSW/fills"})

(def doneruns "V:/Branch - Institutional Processes/SE7/SS Analysis V2/Myles Run V2/Done Runs/")

(def behruns {"Adapt 7+3.1" (str doneruns "Adapt ARFORGEN SC 7 & 3.1/fills")
              "Adapt 3.1"   (str doneruns "Adapt ARFORGEN_SC31/fills")})     

(defn spit-fill-stats [t root]
  (with-open [out (clojure.java.io/writer (str root "fillstats.txt"))]
    (doseq [ln (proc.util/table->lines t)]
      (proc.util/writeln! out ln))))
  
(defn fill-stats [xs]
  (let [missed   (reduce + (map (fn [x] (* (inc (:duration x)) (:quantity x))) 
                                (filter (fn [x] (= (:compo x) "Ghost")) xs)))
        total   (reduce + (map (fn [x] (* (inc (:duration x)) (:quantity x))) 
                                xs))]
    {:missed  missed
     :total    total
     :proportion (float (/ missed total))}))

;;What?  Why named filter...  
(defn Filter [header oper values xs]
  "(Filter header oper values xs)
   Takes a sequence of records, xs, and returns a subset of those records where header is the field name.
   If oper is =, returns the records where the header value is equal to at least one value in values.
   If oper is not=, returns the records where the header is not= to all values in values."
  (if (= oper =)
    (filter (fn [rec] (contains? (set values) (header rec))) xs)
    (reduce (fn [recs val] (filter (fn [rec] (oper (header rec) val)) recs)) xs values)))

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
  (->> (Filter :DemandGroup not= ["NotUtilized"] recs)
    (Filter :FollowOn = [false "false"])))
  
(defn dwell-stats
    "This stats function is intended for records pulled from post processor 'fills' folder.
     (dwell-stats recs compos)
     Returns a map where the keys are :compo+avgdwell and the values are the average dwells for each
     compo."
  [recs & {:keys [compos] :or {compos ["AC" "NG"]}}]
  (let [compfs (group-by :compo (get-gen-fills recs))]      
    (zipmap (map #(keyword (str % "avgdwell")) compos) (->> (map #(get compfs %) compos)
                                                         (map #(Filter :dwell-plot? = [true "true"] %))
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
        subs (Filter :FillType = ["Substitute"] realfills)
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

(defn read-records [path] 
  (let [t  (tbl/tabdelimited->table 
            (slurp path) 
            :keywordize-fields? false 
            :schema fillr)            
        flds (mapv (fn [kw] (keyword (subs kw 1))) (tbl/table-fields t))]
  (-> t                  
    (assoc :fields flds)
    (tbl/table-records))))

(def more-stats-order [:name :demandays :filldays :subdays :percsat :percsubs	
                       :IBCTsubdays :percIBCTsubs :SBCTsubdays :percSBCTsubs 
                       :ABCTsubdays :percABCTsubs	:ACavgdwell	:NGavgdwell])

(defn get-stats-from [name-paths destpath]
  (let [stable (-> (build-fill-tables name-paths :BCT)  ;;if take out BCTs, get an out of memory error with all interests
                 (fill-table more-stats :name))]
    (spit-fill-stats (tbl/order-fields-by more-stats-order stable) destpath)))
    
        
(defn map-on-map 
  "Didn't need this.  Will it ever be useful?
   (map-on map & {:keys [fkeys fvals]})
   Takes a map as an argument and returns a map where fkeys was applied to the keys and fvals was applied to 
   the values."
  [map & {:keys [fkeys fvals] :or {fkeys #(identity %) fvals #(identity %)}}]
  (zipmap (for [k (keys map)] (fkeys k)) (for [v (vals map)] (fvals v))))

(def fpath "V:/Branch - Institutional Processes/SE7/SS Analysis V2/Myles Run V2/Done Runs/Adapt ARFORGEN SC 7 & 3.1/fills/BCT.txt")
(def fdir "V:/Branch - Institutional Processes/SE7/SS Analysis V2/Myles Run V2/Done Runs/Adapt ARFORGEN SC 7 & 3.1/")

(defn lines-demo 
  "We can make line graphs instead of stacked area charts, but need to uncompress x labels, and use fewer groups..."
  [fpath]
  (let [dset (proc.stacked/roll-sand (proc.util/as-dataset fpath) 
                                   :cols [:FillType :DemandType] :cat-function proc.stacked/suff-cat-fill-subs )]
    (view (line-chart :start :quantity :group-by :Category :legend true :data dset))))
    
                              
                              


             



    


  

        

