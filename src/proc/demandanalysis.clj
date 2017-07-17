
;namespace for Marathon input analysis of demand records.  Can do a linegraph of demand signal and get some peak demands here.

(ns proc.demandanalysis
  (:require 
            [proc.util :as util]
            [clojure.string :as str])
  (:use [incanter.core]
        [incanter.charts]))


;Idea was to compute the deltas in the demand signal by computing a net gain of demand quantity each time we have events starting
;or ending.
;Could also use a sampler for this, but this was done before I learned of the sampler
(defn add-deltas 
  "takes incanter dataset.  Adds a row for the end day of each demand with a negative quantity.  Groups by :StartDay"
  [ds] 
  (let [negs (assoc ds :rows (map (fn [row] (assoc row :Quantity (- (:Quantity row)) 
                                                     :StartDay (+ (:StartDay row) (:Duration row)))) (:rows ds)))
         ]
    ($group-by [:StartDay] (conj-rows negs ds))))

(defn quant-by-time 
  "returns two vetors within a vector where the first vector is times and the second vector is quantities between the lowest time
and highest time"
  [daymap]  
  (let [daymkeys (map #(:StartDay %) (keys daymap))
        deltars (map (fn [ds] (:rows ds)) (vals daymap))
        deltas (map (fn [rows] (apply + (map #(:Quantity %) rows))) deltars)
        deltmap (zipmap daymkeys deltas)
        max (apply max daymkeys)]
    (loop [curr (apply min daymkeys) ;can add between delta numbers with repeat instead of +1
           times [curr]
           quants [(get deltmap curr)]
           currquant (get deltmap curr)]
      (if (= curr max)
        [times quants]
        (let [dval (get deltmap (+ curr 1))]
          (recur (+ curr 1) (conj times (+ curr 1)) (if dval (conj quants (+ dval currquant)) (conj quants currquant))
                 (if dval (+ currquant dval) currquant)))))))

(defn add-derived-cols
  "Will add each column with add-derived-column given args of size 3 vectors containing the three args required 
  for add-derived-column"
  ([data]
    data)
  ([data & args]
    (reduce (fn [acc [column-name from-columns f]] (add-derived-column column-name from-columns f acc)) data args)))

(defn get-peak-demands 
  "tds is a dataset.  Defaults to enabled is true and false.  try :enabled true for only enabled records
Returns a map where {:SRC SRC :DemandGroup DemandGroup} are keys and value are integers.
group-bys is an alternative vector of column labels for grouping.   "
  [root & {:keys [enabled group-bys] :or {enabled true group-bys [:SRC :DemandGroup]}}]
  (let [tds (util/as-dataset (str root "AUDIT_DemandRecords.txt"))
        bigmap (->> (if enabled ($where ($fn [Enabled] (= (str/upper-case Enabled) "TRUE")) tds) tds)
                ($group-by group-bys))]
    (reduce-kv (fn [acc k v] (assoc acc k (apply max (second (quant-by-time (add-deltas v)))))) {} bigmap)))
 
(defn build-samples [[times quants]]
  (for [i (range (count times))]
    [(nth times i) (nth quants i)]))
  
(defn get-peak-demands2 
  "this one will include all times where we have peak demand.
  tds is a dataset.  Defaults to enabled is true and false.  try :enabled true for only enabled records
Returns a map where {:SRC SRC :DemandGroup DemandGroup} are keys and value are integers.
group-bys is an alternative vector of column labels for grouping.   "
  [root & {:keys [pred group-bys] :or {pred ($fn [Enabled] (= Enabled "True")) group-bys [:SRC :DemandGroup]}}]
  (let [tds (util/as-dataset (str root "AUDIT_DemandRecords.txt"))
        bigmap (->> ($where pred tds)
                ($group-by group-bys))
        get-peaks (fn [acc k v] 
                    (let [[times quants] (quant-by-time (add-deltas v))
                          peak (apply max quants)
                          samples (build-samples [times quants])
                          peakts (filter (fn [[t quant]] (= quant peak)) samples)]                         
                    (conj acc (assoc k :peak peak :times (map first peakts)))))]
    (reduce-kv  get-peaks [] bigmap)))

(defn get-peak-demands3 
  "want to pull out peak demand by period"
  []
)
  
;(filter (fn [[k v]] (= (:DemandGroup k) "Scenario9")) peaks) ;where peaks is the result of get-peak-demands        

;(view (stacked-area-chart ["a" "a" "b" "b" "c" "c" ] [10 20 30 10 40 20] :legend true :group-by ["I" "II" "I" "II" "I" "II"]))

;(view (xy-plot (first qbt) (second qbt)))


(defn graph-demand 
  "these need to smoothed I think"
  [root & {:keys [pred] :or {pred ($fn [SRC DemandGroup] true)}}]
  (let [ds (->> (util/as-dataset (str root "AUDIT_DemandRecords.txt"))
             ($where pred))
        [times quants] (quant-by-time (add-deltas ds))]
       (view (xy-plot times quants))))
        

