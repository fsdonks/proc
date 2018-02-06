
;namespace for Marathon input analysis of demand records.  Can do a linegraph of demand signal and get some peak demands here.

(ns proc.demandanalysis
  (:require 
            [proc.util :as util]
            [proc.schemas :as schemas]
            [proc.interests :as ints]
            [clojure.string :as str]
            [spork.util.table :as tbl]
            [spork.util.temporal :as temp]
            [proc.dynamicbars :refer [enabled-demand]]
            [proc.core :as c]
            [proc.supply :as supply]
            [proc.stacked :as stacked])
  (:use [incanter.core]
        [incanter.charts])
  (:import [org.jfree.chart.annotations XYLineAnnotation]
           [ java.awt.Color]))


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
  [root & {:keys [pred group-bys] :or {pred ($fn [Enabled] (= (str/upper-case Enabled) "TRUE")) group-bys [:SRC :DemandGroup]}}]
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
        

;;___________________________________

;;spark charts

(defn key->str
  "Take a keyword, remove the : and return a string"
  [keyw]
  (last (clojure.string/split (str keyw) #":")))

(defn keymap->strmap 
  "proc.charts/interests->src-map returns a map of src to a set of interest keywords.
  This will turn those interest keywords to strings."
  [m]
  (reduce-kv (fn [acc k v] (assoc acc k (set (map key->str v))))  {} m))

(defn src->int
  "This will return a set of interests for each src instead of one interest per
  src like ints/src->int"
  [interests]
  (->> (proc.charts/interests->src-map interests :interest-name? true)
       ;(keymap->strmap)
       ))
  
(defn met-by-time
  "Demand satisfaction by group-fn and time from demandtrends.txt.
  group-fn operates on the SRC string and can be (src->int interests), identity for by src, or (fn [s] 'All') for everything"
  [path & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  ;allow for multiple interests per SRC.
    (->> (tbl/tabdelimited->records path :pasemode :noscience :schema schemas/dschema)
         (into [])
         (map (fn [{:keys [t SRC TotalRequired Overlapping TotalFilled]}]
                    {:SRC SRC :t t :req (+ TotalRequired Overlapping) :filled TotalFilled}))
         (util/separate-by (fn [r] (group-fn (:SRC r))))
         (reduce-kv (fn [acc interest v]
                      ;demandtrends should be sorted by time, so this is okay.
                      (let [parts (partition-by :t v)]
                        (conj acc
                        [interest
                         (map :t (map first parts))
                         (map (fn [p] (* 100 (/ (reduce + (map :filled p)) (reduce + (map :req p))))) parts)]))) [])))

(defn smooth
  "Used when plotting x and y values.  When the xs are sparse, this
  returns new xs and ys so that there are 90 degree angles between
  each delta"
  [xs ys]
  (let [spliced (->> (for [x (range 1 (count xs))]
                       [[(- (nth xs x) 0.00001) (nth xs x)] [(nth ys (- x 1)) (nth ys x)]])
                     (concat [[[(first xs)] [(first ys)]]]))
        agg (fn [f parts] (mapcat f parts))]
    [(agg first spliced) (agg second spliced)]))

(def curr (atom nil))

(defn peak-times
  "Returns a map with the peak of the activity profile along with 2 item vectors containing the start and end of each period of peak as defined the peak-function."
  [activities  peak-function]
  (let [_ (reset! curr nil)
        deltas (remove (fn [[t r]] ((constantly (= @curr (peak-function r)))
                                    (reset! curr (peak-function r)))) activities)
        peak (apply max (map (fn [[t r]] (peak-function r)) deltas))
        _ (reset! curr nil)
        peaks (reduce (fn [acc [t r]] (if @curr
                                        ((constantly (conj acc [@curr (- t 1)])) (reset! curr nil))
                                        (if (= (peak-function r) peak)
                                          (do (reset! curr t) acc)
                                          acc))) [] deltas)
        last-rec (last (:actives (last (last activities))))]
                                        ;If the peak is 0, it's not really a peak.
    (assoc {:peak peak} :intervals (if (= peak 0) [] peaks))))

(defn peak-times-by
  "Groups xs by a key function, f, and for each group, returns a map with the peak, 2 item
  vectors containing the start and end of each period of peak as defined by an optional peak-function and result of applying
  f to the records.  Defaults to the number of active records in an activity sample as the peak."
  [f xs & {:keys [start-func duration-func peak-function] 
           :or   {start-func :Start duration-func :Duration
                  peak-function (fn [r] (:count r))}}]
        (for [[k recs] (group-by f xs)]
          (let [activities (temp/activity-profile recs :start-func start-func 
                                        :duration-func duration-func)]
            (assoc (peak-times activities peak-function) :group k))))

(defn peak-times-by-period
  "Groups xs by a key function, f, and for each group, returns the peak along with 2 item
  vectors containing the start and end of each period of peak as defined by an optional peak-function.
Defaults to the number of active records in an activity sample as the peak."
  [f xs period-recs start-func duration-func peak-function]
  (for [[k recs] (util/separate-by f xs)
        :let [activities (temp/activity-profile recs :start-func start-func :duration-func duration-func)]
        {:keys [FromDay ToDay Name] :as r} period-recs
        :let [during (filter (fn [[t m]] (and (>= t FromDay) (<= t ToDay))) activities)
                                        ;this will be nil if there were no activities before
              before (last (take-while (fn [[t m]] (< t FromDay)) activities))
                                        ;if the period ended on a peak, stop the last interval at the end of the period
                                        ;if the period started on a peak, begin the first interval at the beginning of the period
              acts (concat [[FromDay (second before)]] during [[(+ ToDay 1) {}]])]]
      (assoc (peak-times acts peak-function) :group k :period Name)))
  
(defn peaks-from
  "Given the path to a Marathon audit trail, compute peak-times-by-period for the enabled
  demand records. Supply an optional demand-filter for the demand records."
  [path & {:keys [group-fn demand-filter] :or {group-fn (fn [s] "All") demand-filter (fn [r] true)}}]
  (let [demands (->> (enabled-demand path)
                     (filter demand-filter))
        peakfn (fn [{:keys [actives]}] (apply + (map :Quantity actives)))
        periods (util/load-periods path)]
    (peak-times-by-period (fn [r] (group-fn (:SRC r))) demands periods :StartDay :Duration peakfn)))

(defn peak-lines
  "given the path to a marathon audit trail, compute a sequence of [[x1 y1] [x2 y2]] vectors for each interest in order to draw
  horizontal lines whenever there is peak demand."
  [path & {:keys [group-fn demand-filter] :or {group-fn (fn [s] "All") demand-filter (fn [r] true)}}]
  ;demand-filter should work on supply-filter too
  (let [capacities (supply/capacity-by path :group-fn group-fn :supply-filter demand-filter)]
  (->> (peaks-from path :group-fn group-fn :demand-filter demand-filter)
       (remove (fn [r] (= (:peak r) 0)))
       (group-by :group)
       (map (fn [[group rs]]
              [group (reduce (fn [acc {:keys [intervals peak period]}]
                               (concat acc (map (fn [[x1 x2]] (let [static-met (* 100 (/ (capacities [group period]) peak))]
                                                                [[x1 static-met] [x2 static-met]])) intervals))) [] rs)]))
       (into {}))))

(def tatom (atom nil))

(defn draw-line
  "Takes the plot and adds a line to it.  Like add-polygon, but the stroke and paint are specified via
  the options"
  [plt [[x1 y1] [x2 y2]] & {:keys [paint stroke]}]
  (let [annotation (new XYLineAnnotation x1 y1 x2 y2 (new java.awt.BasicStroke 3) java.awt.Color/blue)]
    (.addAnnotation (.getXYPlot plt) annotation)))

;;this isn't working.  probably need to change the renderer for the plot?
(defn set-legend-stroke
  "Takes the plot and sets the stroke width of the nth item in the LegendCollection to stroke-width."
  [plt n stroke-width]
  (let [i-legend (-> (.getXYPlot plt)
            (.getLegendItems)
            (.get n))]
    (.setLineStroke i-legend (new java.awt.BasicStroke 3)) ))

(defn spark-charts
  "make a spark chart for each group.  Once it's added, see met-by-time for an explanation of
  group-fn"
  [path & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (let [inscopes (c/inscope-srcs (str path "AUDIT_InScope.txt"))
        inscope? (fn [src] (not (nil? (inscopes src))))
        lines (peak-lines path :group-fn group-fn :demand-filter (fn [r] (inscope? (:SRC r))))]
  (doseq [[group ts ys] (met-by-time (str path "DemandTrends.txt") :group-fn group-fn)]
    (let [[sts sys] (smooth ts ys)
          plt (xy-plot sts sys :legend true
                       :x-label "Time (days)"
                       :y-label "Percentage of Demand Met"
                       :series-label "Simulation")
          _ (reset! tatom plt)]
      (doseq [i (lines group)] (draw-line plt i))
      (.setTitle plt (str group " TADMUDI and Simulation Results"))
      ;add the TADMUDI entry to the legend.
      (add-lines plt [] [] :series-label "TADMUDI and Peak Demand Periods")
      ;change the stroke of the new TADMUDI entry to match the stroke of draw-line
      ;(set-legend-stroke plt 1 3)
      (proc.core/phases-to-chart plt path)
      (doto (new org.jfree.chart.ChartFrame group plt)                   
        (.setSize 500 400)
        (.setVisible true))))))

;(view (xy-plot [1 2 3 4 5] [5 8 2 9 5))

;(add-polygon ap [[15 225] [35 225]])
