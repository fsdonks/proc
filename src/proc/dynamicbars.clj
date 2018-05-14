;At one point, we were creating these "dynamic sufficiency bar charts" in Excel.
;For multiple SRCs and multiple policies, these charts show the maximum number of deployed units as a % of peak demand.
(ns proc.dynamicbars
  (:require [spork.util.table :as tbl]
            [proc.interests :as ints]
            [proc.schemas :as schemas]
            [clojure.string :as str]
            [spork.util.temporal :as temp]
            [proc.util :as util])
  (:use [incanter.charts]))


(defn srcs-ints 
  "returns a map of {'SRC' :interest...}."
  [ints] ;still expecting the SRCS to be unique in the interests here.
  (reduce-kv (fn [m k [name srcs]] (merge m (apply assoc {} (concat (interpose k srcs) [k])))) {} ints))

(defn peaks-by-interest 
  "Returns the last max peak found for each interest"
  [root interests & {:keys [groupfn]}]
  (let [drecs (util/enabled-demand root)
        rev-ints (srcs-ints interests)
        groupfn  (if groupfn groupfn (fn [{:keys [SRC DemandGroup]}] (rev-ints SRC)))
        peakfn (fn [{:keys [actives]}] (apply + (map :Quantity actives)))]
    (temp/peaks-by groupfn drecs :start-func :StartDay :duration-func :Duration :peak-function peakfn))) 

(defn get-peak-info
  "Returns '[times (peakfn record)]'"
  [peak-acts peakfn]
  [(map (fn [[t r]] t) peak-acts) (peakfn (second (first peak-acts)))])

(defn max-fill-during [trendrecs ts] ;assume that trendrecs are for one interest
  (reduce max (map (fn [t] (reduce + (map :Deployed (filter (fn [r] (= (:t r) t)) trendrecs)))) ts)))

(defn dmdrecs->maxstats 
  "Returns '[maxfillduringpeakdmd peakdmd]'"
  [dmdrecs trendrecs rev-ints] ;assume dmdrecs and trendrecs are for one interest
  (let [groupfn (fn [{:keys [SRC]}] (rev-ints SRC))
        peakfn (fn [{:keys [actives]}] (reduce + (map :Quantity actives)))
        [ts peakdmd] (-> (temp/peak-activities dmdrecs :start-func :StartDay :duration-func :Duration :peak-function peakfn)
                       (get-peak-info peakfn))]
    [(max-fill-during trendrecs ts) peakdmd]))
    
(defn peak-info-by-int [root interests]
  "Returns a map of {interest1 [maxfillduringpeakdmd1 peakdmd1] interest2 [maxfillduringpeakdmd2 peakdmd2]...}"
   (let [dmdrecs (->> 
                (tbl/tabdelimited->table (slurp (str root "AUDIT_DemandRecords.txt")) :parsemode :noscience 
                                         :schema schemas/drecordschema)
                (tbl/table-records)
                (filter (fn [{:keys [Enabled]}] Enabled)))
        trendrecs (tbl/table-records (tbl/tabdelimited->table (slurp (str root "DemandTrends.txt"))
                                   :schema schemas/dschema :parsemode :noscience)) 
        rev-ints (srcs-ints interests)
        groupfn (fn [{:keys [SRC]}] (rev-ints SRC))
        dmdrecsbyints (group-by groupfn dmdrecs)
        trendrecsbyints (group-by groupfn trendrecs)
        bothrecs (merge-with (fn [drecs trecs] [drecs trecs]) dmdrecsbyints trendrecsbyints)
        stats-by-int (reduce-kv (fn [m k [drecs trecs]] (assoc m k (dmdrecs->maxstats drecs trecs rev-ints))) {} bothrecs)]
     stats-by-int))

;________testing:

(defn get-bar-perc 
  "To compare to charts we've already done"
  [peakmap]
  (reduce-kv (fn [m k [fill dmd]] (assoc m k (/ (float fill) dmd))) {} peakmap))
  
;groupnames should be ordered the same way as roots
(defn dyn-suff-chart [ints roots & {:keys [groupnames] :or {groupnames (map (fn [root] 
                                                                              (util/next-last (str/split root #"/"))) roots)}}]
  (let [interests (into (sorted-map-by (fn [key1 key2] (compare (str key1) (str key2)))) ints) ;order ints alphabetically
        peakmaps (map (fn [root] (peak-info-by-int root interests)) roots)
        categories (mapcat (fn [int] (repeat (count roots) (subs (str int) 1))) (keys interests))
        groupby (reduce concat (repeat (count interests) groupnames))
        values (reduce concat (for [i (keys interests)] (map i peakmaps)))
        percvals (map (fn [[fill dmd]] (* 100 (/ (float fill) dmd))) values)]
  (bar-chart categories 
                   percvals
                     :legend true
                     :group-by groupby
                     :x-label "SRC Group" 
                     :y-label "Max % of Peak Demand Filled") ;default-bar-colors
        ))
               
(def actcolors54 [(java.awt.Color. 0 176 80) (java.awt.Color. 255 192 0) (java.awt.Color. 255 0 0)] )

(defn get-stroke-args [colors]
  (for [c (range (count colors))] [(nth colors c) :series c]))

(defn set-chart-colors [chart allargs]
  (reduce (fn [acc args] (apply (partial set-stroke-color acc) args)) chart allargs))

(defn color-chart [chart colors]
  (set-chart-colors chart (get-stroke-args colors)))
             
(defn removeBarGaps [barchart] ;mutates the char since we're setting stuff
  (let [tplot (.getPlot barchart)
        tren (.getRenderer tplot)]
    (do (.setItemMargin tren 0.0))))

(defn addMarkerAt [chart value] ;mutates the chart since we're setting and adding stuff 
  (let [marker (new org.jfree.chart.plot.ValueMarker value)
        tplot (.getPlot chart)]
    (do (.setStroke marker (new java.awt.BasicStroke 0.5))
      (.setPaint marker java.awt.Color/black) ;using the static field red
      (.addRangeMarker tplot marker))))

(def roots54 ["root1path"
              "root2path"
              "root3path"
              ])

; try (view tchart) after loading the below code in order to view the chart.
(comment (def tchart (let [chart (dyn-suff-chart (dissoc ints/bctscabs :ECAB :CABS) roots54)]
              (do (color-chart chart actcolors54)
              (removeBarGaps chart)
              (addMarkerAt chart 100)
              chart)))
         )
