
;namespace for Marathon input analysis of demand records.  Can do a linegraph of demand signal and get some peak demands here.

(ns proc.demandanalysis
  (:require 
            [proc.util :as util]
            [proc.schemas :as schemas]
            [proc.interests :as ints]
            [clojure.string :as str]
            [spork.util.table :as tbl]
            [spork.util.temporal :as temp]
            [proc.core :as c]
            [proc.supply :as supply]
            [proc.stacked :as stacked]
            [proc.charts :as charts]
            [proc.risk :as risk])
  (:use [incanter.core]
        [incanter.charts])
  (:import [org.jfree.chart.annotations XYLineAnnotation]
           [org.jfree.chart LegendItemCollection]
           [java.awt Color]
           [java.lang Math]))

;;Idea was to compute the deltas in the demand signal by computing a net gain of
;;demand quantity each time we have events starting or ending.

;;Could also use a sampler for this, but this was done before I learned of the
;;sampler
(defn add-deltas
  "takes DemandRecords. Adds a row for the end day of each demand with a negative
  quantity. Groups by :StartDay. Can supply a function that operates
  on a demand record to return a response other than :Quantity like
  (fn [r] (* (:Quantity r) (:Strength r)))."
  [recs & {:keys [response] :or {response :Quantity}}]
  (let [recs (if (= response :Quantity)
               recs
               (map (fn [r] (assoc r :Quantity (response r))) recs))
        negs (for [row recs]
               (assoc row :Quantity (- (:Quantity row))
                          :StartDay (+ (:StartDay row) (:Duration row))))]
    (group-by :StartDay (concat recs negs))))

(defn quant-by-time
  "returns two vetors within a vector where the first vector is times and the
  second vector is quantities between the lowest time and highest time"
  [daymap]
  (let [daymkeys (keys daymap)
        deltars (vals daymap)
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
          (recur (+ curr 1)
                 (conj times (+ curr 1))
                 (if dval (conj quants (+ dval currquant)) (conj quants currquant))
                 (if dval (+ currquant dval) currquant)))))))

;;Wat?
(defn add-derived-cols
  "Will add each column with add-derived-column given args of size 3 vectors
  containing the three args required for add-derived-column"
  ([data]  data)
  ([data & args]
   (reduce (fn [acc [column-name from-columns f]]
             (add-derived-column column-name from-columns f acc)) data args)))

(defn enabled? [{:keys [Enabled]}]
  (= (str/upper-case Enabled) "TRUE"))

(def src-group "Function of :: record -> [SRC DemandGroup]"
  (juxt :SRC :DemandGroup))

;;Make Generic.
(defn filtered-demand
  [root & {:keys [pred] :or {pred enabled?}}]
  (->> (tbl/tabdelimited->records (str root "AUDIT_DemandRecords.txt")
                                  :paresmode :noscience :schema schemas/drecordschema)
       (into [] (filter pred))))

;;Make Generic.
(defn load-time-map
  [root & {:keys [pred groupf] :or {pred   enabled?
                                    groupf src-group}}]
  (->> (filtered-demand root :pred pred)
       (group-by groupf)))

(defn demand-quantities
  "calls this on a sequence of demand records, returns the result of
  quant-by-time."
  [recs & {:keys [response] :or {response :Quantity}}]
  (quant-by-time (add-deltas recs :response response)))

(defn get-peak-demands
  "tds is a dataset. Defaults to enabled is true and false.
   try :enabled true for only enabled records Returns a map where
   {:SRC SRC :DemandGroup DemandGroup} are keys and value are integers.
   group-bys is an alternative vector of column labels for grouping."
  [root & {:keys [pred groupf] :or {pred enabled?
                                    groupf src-group}}]
  (let [bigmap (load-time-map root :pred pred :groupf groupf)]
    (reduce-kv (fn [acc k v]
                 (assoc acc k
                        (apply max (second (demand-quantities v)))))
               {} bigmap)))

;;document
(defn build-samples [[times quants]]
  (for [i (range (count times))]
    [(nth times i) (nth quants i)]))

(defn get-peak-demands2
  "this one will include all times where we have peak demand.
  tds is a dataset.
  Defaults to enabled is true and false.
  try :enabled true for only enabled records Returns a map where
  {:SRC SRC :DemandGroup DemandGroup} are keys and value are integers.
  group-bys is an alternative vector of column labels for grouping."
  [root & {:keys [pred groupf] :or {pred enabled?  groupf src-group}}]
  (let [bigmap (load-time-map root :pred pred :groupf groupf)
        get-peaks (fn [acc k v]
                    (let [[times quants] (quant-by-time (add-deltas v))
                          peak (apply max quants)
                          samples (build-samples [times quants])
                          peakts (filter (fn [[t quant]] (= quant peak)) samples)]
                      (conj acc {:group k :peak peak :times (map first peakts)})))]
    (reduce-kv  get-peaks [] bigmap)))

;;(filter (fn [[k v]] (= (:DemandGroup k) "Scenario9")) peaks)
;;where peaks is the result of get-peak-demands

;;(view (stacked-area-chart ["a" "a" "b" "b" "c" "c" ] [10 20 30 10 40
;;20] :legend true :group-by ["I" "II" "I" "II" "I" "II"]))

;;(view (xy-plot (first qbt) (second qbt)))

;;Auxillary fn for plotting, likely redundant (proc.stacked)
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

;;Generic?
(defn graph-demand
  "these need to smoothed I think"
  [root & {:keys [pred] :or {pred ($fn [SRC DemandGroup] true)}}]
  (let [recs (filtered-demand root :pred pred)
        [times quants] (quant-by-time (add-deltas recs))]
    (xy-plot times quants)))

;;generic?
(defn graph-demand-for
  "Given a sequence of srcs, graph-demand for each. Set xlow and xmax for the x
   axis."
  [root srcs xlow xmax]
  (let [cs (for [s srcs]
             (doto (graph-demand root :pred
                      ($fn [SRC Enabled]
                           (and (= (clojure.string/upper-case Enabled) "TRUE")
                                (= SRC s))))
               (.setTitle (str s " Demand vs Time"))
               (util/set-bounds :x-axis :lower xlow :upper xmax)))
        _ (doseq [c cs] (.setLabel (.getDomainAxis (.getPlot c)) "Time (days)")
                 (.setLabel (.getRangeAxis (.getPlot c)) "Number of Units Required"))]
    (util/sync-scales cs)
    (doseq [c cs] (view c) cs)))

(defn sand-demands
  "Draw a sand chart of the demands grouped by k keyword called on
  each demand record.  p specifies a path to AUDIT_DemandRecords.txt.
  reponse is an alternate function for the vertical axis besides
  :Quantity."
  [p k & {:keys [response schema] :or {response :Quantity
                                       schema schemas/drecordschema}}]
  (let [xytable
        (->> (tbl/tabdelimited->records p :schema schema)
             (group-by k)
             ((fn [m]
                (for [[group recs] m
                      :let [[xs ys] (apply smooth
                                           (demand-quantities recs
                                                              :response response))]
                      i (range (count xs))]
                  {:x (nth xs i) :y (nth ys i) :group group})))
             (clojure.core.matrix.dataset/dataset)
             (stacked/xy-table :x :y :group-by :group :data))]
    (stacked/stacked-areaxy-chart2* xytable)))

;;example usage to view a sandchart of demand records by :vignette
;;where the y axis is total strength.
(comment
  (binding [stacked/*trend-info* stacked/d-info]
    (view (sand-demands path
                        :Vignette
                        :response (fn [r] (* (:Strength r) (:Quantity
                                                            r)))
                        :schema (assoc schemas/drecordschema
                                       "Strength" :int))))
  )
                                                        
;;___________________________________

;;spark charts
(defn keymap->strmap
  "proc.charts/interests->src-map returns a map of src to a set of interest keywords.
  This will turn those interest keywords to strings."
  [m]
  (reduce-kv (fn [acc k v] (assoc acc k (set (map name v))))  {} m))

(defn src->int
  "This will return a set of interests for each src instead of one interest per
  src like ints/src->int"
  [interests]
  (proc.charts/interests->src-map interests :interest-name? true))

(defn partition-trends
  "Partitions the sorted-map of demandtrends where the keys are time and the vals
  are records into a partitioned sequence of records. A new partition is made
  each time there is a period of no demand."
  [sorted-trends]
  (->> (map second sorted-trends)
       (partition 2 1)
       (partition-by (fn [[[{deltat1 :deltat t1 :t}] [{t2 :t}]]]
                       ;(assert (<= (+ deltat1 t1) t2) "demand samples should never overlap")
                       (< (+ deltat1 t1) t2)))
       (map (fn [parts] (map (fn [[left right]] left) parts)))
       ))

;;Generic...
(defn percent-satisfaction
  "compute the demand satisfaction from a sequence of demantrend records."
  [recs]
  (let [denominator (reduce + (map :req recs))]
    (if (= denominator 0) 0
        (* 100 (/ (reduce + (map :filled recs)) denominator)))))

(defn satisfaction
  "compute a map of demand satisfaction, deltat, and time. "
  [[{:keys [deltat t] } :as part]]
  {:percent (percent-satisfaction part)
   :deltat deltat
   :t t})

(defn lastday
  "compute the lastday for a demand given the start time and duration. lastday and
  t are inclusive."
  [t duration]
  (- (+ t duration) 1))

;;need to account for deltat between gaps so decided to use loop recur expect no
;;last recorded on.
;;Handle that at end of loop deltat for all demands on a single day should be
;;the same.
;;Can test this assumption later or assert in met-by-time at a performance cost.
(defn part-trends
  "Partitions the sorted-map of demandtrends where the keys are time and the
   vals are records into a partitioned sequence of records.
   A new partition is made each time there is a period of no demand.
   Returns [ts ys]"
  [sorted-trends]
  (loop [[[{firstt :t firstdt :deltat :as firstrend}
           :as trend]
          :as trends] (into [] (rest (map second sorted-trends)))
         gapped-trends []
         current [(satisfaction (first (map second sorted-trends)))]]
    (let [{:keys [t percent deltat] :as lastcurr} (last current)]
      (if (empty? trends)
        (conj gapped-trends (conj current (assoc lastcurr :t (lastday t deltat))))
        (if (= (+ 1 (lastday t deltat)) firstt)
          (recur (rest trends) gapped-trends (conj current (satisfaction trend)) )
          ;;duration of last demand before a gap is always 1
          (recur (rest trends) (conj gapped-trends current) [(satisfaction trend)])
          )))))

(def ^:dynamic demandfilter (fn [r] true))

(defn condense-trends
  "Shrink each demandtrend record to the information we really need.
   Adds overlapping to totalrequired."
  [trends]
  (->> trends
  (map (fn [{:keys [t SRC TotalRequired Overlapping TotalFilled deltaT
  Vignette]}]
         {:SRC SRC
          :t t
          :req (+ TotalRequired Overlapping)
          :filled TotalFilled
          :deltat deltaT
          :vignette Vignette}))
  (filter demandfilter)))

(defn satisfaction-with-overlap
  "use demantrend records to get the percent demand satisfied."
  [recs]
  ((comp percent-satisfaction condense-trends) recs))


;;Post - DemandTrends
;;TODO: Allow group-fn to act as an exclusionary filter if
;;group is nil?
(defn simple-trends
  "compute a subset of demand trend information and group the records acording to a group-fn.
  group-fn operates on the SRC string and can be (src->int interests), identity
  for by src, or (fn [s] 'All') for everything."
  [xs & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (let [recs (if (coll? xs) xs (util/load-trends xs))]
    (->> recs ;;(into [] recs)
         (condense-trends)
         (util/separate-by (fn [r] (group-fn (:SRC r)))))))

;;Post - DemandTrends.
;;deltat = duration
(defn met-by-time
  "Demand satisfaction by group-fn and time from demandtrends.txt.
   xs is the path to a marathon audit trail directory or the demandtrend records.
   Demand satistfaction is partitioned each time there is no demand."
  [xs & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (->> (simple-trends xs :group-fn group-fn)
       (map (fn [[interest v]]
              (let [parts (into (sorted-map) (group-by :t v))
                    partitions (part-trends parts)]
                [interest
                 (map (fn [part] (map :t part)) partitions)
                 (map (fn [part] (map :percent part)) partitions)])))))

(def curr (atom nil))

;;TODO - eliminate curr references
(defn peak-times
  "Returns a map with the peak of the activity profile along with 2 item vectors
  containing the start and end of each period of peak as defined the
  peak-function."
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
    ;;If the peak is 0, it's not really a peak.
    {:peak peak :intervals (if (zero? peak) [] peaks)}))

(defn peak-times-by
  "Groups xs by a key function, f, and for each group, returns a map with
   the peak, 2 item vectors containing the start and end of each period of peak as
   defined by an optional peak-function and result of applying f to the records.
   Defaults to the number of active records in an activity sample as the peak."
  [f xs & {:keys [start-func duration-func peak-function]
           :or   {start-func :Start duration-func :Duration
                  peak-function (fn [r] (:count r))}}]
        (for [[k recs] (group-by f xs)]
          (let [activities (temp/activity-profile recs :start-func start-func
                                        :duration-func duration-func)]
            (assoc (peak-times activities peak-function) :group k))))

(defn peak-times-by-period
  "Groups xs by a key function, f, and for each group, returns the peak along with
  2 item vectors containing the start and end of each period of peak as defined
  by an optional peak-function. Defaults to the number of active records in an
  activity sample as the peak."
  [f xs period-recs start-func duration-func peak-function]
  (for [[k recs] (util/separate-by f xs)
        :let [activities (temp/activity-profile recs :start-func start-func
                                                :duration-func duration-func)]
        {:keys [FromDay ToDay Name] :as r} period-recs
        :let [during (filter (fn [[t m]] (and (>= t FromDay) (<= t ToDay))) activities)
              ;;this will be nil if there were no activities before
              before (last (take-while (fn [[t m]] (< t FromDay)) activities))
              ;;if there is an activity on the first day of the period, we don't want a before
              initial (if (= (ffirst during) FromDay) [] [[FromDay (second before)]])
;;if the period ended on a peak, stop the last interval at the end of the period
;;if the period started on a peak, begin the first interval at the beginning of the period.
              acts (concat initial during [[(+ ToDay 1) {}]])]]
      (assoc (peak-times acts peak-function) :group k :period Name)))

;;Static
(defn peaks-from
  "Given the path to a Marathon audit trail, compute peak-times-by-period for the
   enabled demand records.
   Supply an optional demand-filter for the demand records.
   group-fn accepts an SRC string as the argument and groups the records by this
   function in addition to by period."
  [path & {:keys [group-fn demand-filter periods]
           :or {group-fn (fn [s] "All")
                demand-filter (fn [r] true)
                }}]
  (let [periods (if (nil? periods) (util/load-periods path) periods)
        demands (->> (util/demand-records path)
                     (filter demand-filter))
        peakfn (fn [{:keys [actives]}] (apply + (map :Quantity actives)))]
    (peak-times-by-period (fn [r] (group-fn (:SRC r)))
                          demands periods :StartDay :Duration peakfn)))

(defn capacity-vector [capacities group period peak [x1 x2]]
  (let [static-met (* 100 (/ (capacities [group period]) peak))]
    [[x1 static-met] [x2 static-met]]))

  #_(fn [[x1 x2]]
      (let [static-met (* 100 (/ (capacities [group period]) peak))]
        [[x1 static-met] [x2 static-met]]))

;;Static / Generic
(defn peak-lines
  "given the path to a marathon audit trail, compute a sequence of
  [[x1 y1] [x2 y2]] vectors for each interest in order to draw horizontal lines whenever
  there is peak demand."
  [path & {:keys [group-fn demand-filter]
           :or {group-fn (fn [s] "All") demand-filter (fn [r] true)}}]
  ;demand-filter should work on supply-filter too
  (let [capacities (supply/capacity-by path :group-fn group-fn :supply-filter demand-filter)]
    (->> (peaks-from path :group-fn group-fn :demand-filter demand-filter)
         (remove (fn [r] (= (:peak r) 0)))
         (group-by :group)
         (map (fn [[group rs]]
                [group (reduce (fn [acc {:keys [intervals peak period]}]
                                 (concat acc (map (capacity-vector capacities
                                                      group period peak)
                                                   intervals))) [] rs)]))
         (into {}))))

;;Charting
(defn draw-line
  "Takes the plot and adds a line to it. Like add-polygon, but the stroke and
  paint are specified via the options"
  [plt [[x1 y1] [x2 y2]] & {:keys [paint stroke]}]
  (let [color (new java.awt.Color (float 0) (float 0) (float 1) (float 0.37))
        annotation (new XYLineAnnotation x1 y1 x2 y2 (new java.awt.BasicStroke 3) color)]
    (.addAnnotation (.getXYPlot plt) annotation)))

;;Charting
;;this isn't working.  probably need to change the renderer for the plot?
(defn set-legend-stroke
  "Takes the plot and sets the stroke width of the nth item in the
  LegendCollection to stroke-width."
  [plt n stroke-width]
  (let [i-legend (-> (.getXYPlot plt)
            (.getLegendItems)
            (.get n))]
    (.setLineStroke i-legend (new java.awt.BasicStroke 3)) ))

;;Charting
(defn extend-bounds
  "Makes a progress listener for a jfreechart.
   Listens for when the plot is done drawing and then extends the y axis beyond
   mx.
   Also adds phase lines to the chart based on the marathon audit trail path."
  [cht mx path phase-lines?]
  (reify org.jfree.chart.event.ChartProgressListener
    (chartProgress [this e]
      (when (= (.getType e) org.jfree.chart.event.ChartProgressEvent/DRAWING_FINISHED)
        (let [tickunit (-> cht (.getXYPlot) (.getRangeAxis) (.getTickUnit) (.getSize))
              above (if (zero? (rem mx tickunit))
                      (+ mx (/ tickunit 2))
                      (+ (- mx (rem mx tickunit)) tickunit))
         ;;need to bump the upper bound up a bit in this case. Should probably
         ;;be a function of draw-line
         ;;stroke width but this is okay for now after some testing.
              above (if (<= (/ (- above mx) tickunit) 0.075) (+ above (/ tickunit 2)) above)]
          (util/set-bounds cht :y-axis :upper above)
          (when phase-lines? (c/phases-to-chart cht path))
          (.removeProgressListener cht this)
          )))))

;;Charting.
(defn remove-extra-legend-items
  "An extra item is added to the legend every time add-lines is called. Only keep
  the first item that was added to the legend."
  [plot]
  (let [plt (.getPlot plot)
        legendItemsOld (.getLegendItems plt)
        legendItemsNew (new LegendItemCollection)]
    (.add legendItemsNew (.get legendItemsOld 0))
    (.add legendItemsNew (.get legendItemsOld (- (.getItemCount legendItemsOld) 1)))
    (.setFixedLegendItems plt legendItemsNew)))

;;Charting
(defn plot-separate-lines
  "Can create an xy-plot with gaps. xs and ys are sequences of partitioned x and y
  values for each line segment."
  [xs ys]
  (let [[sts sys] (smooth (first xs) (first ys))
        plt (xy-plot sts sys :legend true
                       :x-label "Time (days)"
                       :y-label "Percentage of Demand Met"
                       :series-label "Simulation")]
  (doseq [x (range 1 (count xs))]
    (add-lines plt (nth xs x) (nth ys x))
    (set-stroke-color plt java.awt.Color/red :dataset x))
  plt))

(defn intersect?
  "Given two line segments ([x11 x12] and [x21 x22]), determine if they intersect
   at all."
  [x11 x12 x21 x22]
  (assert (<= x11 x12)
  "The second value of the first line segment needs to be greater than  the first value.")
  (assert (<= x21 x22)
  "The second value of the second line segment needs to be greater than the first value.")
  (if (< x11 x21)
    (>= x12 x21)
    (<= x11 x22)))

(defn within?
  "test if x is contained within the interval from low to high inclusive."
  [x low high]
  (and (>= x low) (<= x high)))

(defn max-within
  "Given a sequence of line seqments in 2-d like [[x1 y1] [x2 y2]] where the y
   value of the line is specified by y1, finds the maximum value of the lines
   between xlow and xhigh."
  [xlow xhigh xs]
  (->> xs
       (filter (fn [[[x1 y1] [x2 y2]]]
                 (if (and x1 x2) (intersect? x1 x2 xlow xhigh)
                     ;;probably don't need this.  Should always have x1 and x2
                     (within? x1 xlow xhigh))))
  (map (fn [[[x1 y1] [x2 y2]]] y1))))

;;account for grouped mots and mys
(defn find-max
  "returns the maximum percent demand met of tadmudi lines and marathon lines
   between low and high inclusive times."
  [[low high] mts mys tadlines] 
  (->> (map (fn [ts ys] (->> (map #(conj [%1] %2) ts ys)
                             (partition 2 1)
                             ;;second t is the next record.
                             (map (fn [[[x1 y1] [x2 y2]]] 

                                    [[x1 y1] [(if (> x2 x1) (dec x2) x2) y2]]))
                             (max-within low high)))
            mts mys)
       (reduce concat)
       (concat (max-within low  high tadlines))
       ((fn [xs] (if (empty? xs) 0 (reduce max xs))))))


;;Generic
(defn spark-charts
  "make a spark chart for each group. Once it's added, see met-by-time for an
  explanation of group-fn. set the x bounds with a vector of [lowbound
  highbound]. Y axis max will be determined based on these bounds."
  [path & {:keys [group-fn show-charts? phase-lines? bounds]
           :or {group-fn (fn [s] "All") show-charts? false
                phase-lines? true       bounds nil}}]
  (let [inscopes (util/inscope-srcs (str path "AUDIT_InScope.txt"))
        inscope? (fn [src] (not (nil? (inscopes src))))
        lines (peak-lines path :group-fn group-fn :demand-filter (fn [r] (inscope? (:SRC r))))
        drecs (util/load-trends path)
        {finalt :t finaldt :deltaT} (last drecs)
        {firstday :t} (first drecs)
        ;;automatic x axis max does not account for tadmudi lines.
        ;;Let's assume that last day we care about is
        xbound (if bounds (second bounds) (lastday finalt finaldt))]
  (for [[group ts ys] (met-by-time drecs :group-fn group-fn)]
    (let [plt (plot-separate-lines ts ys)
          maxpercent (if bounds (find-max bounds ts ys (lines group))
                         (reduce max (concat (reduce concat ys)
                                             (map (fn [[[x y] []]] y) (lines group)))))]
         ;;this is odd. can probably just use the false branch of this if
         ;;tried this first but then the bar at maxpercent is sometimes cut off.
        ;(do (util/set-bounds plt :y-axis :upper maxpercent)
            ;(.addProgressListener plt (extend-bounds plt maxpercent path phase-lines?)))
      (do (util/set-bounds plt :y-axis :upper (+ maxpercent 5))
        (when phase-lines? (c/phases-to-chart plt path)));)
      (util/set-bounds plt :x-axis :upper xbound :lower (if bounds (first bounds) firstday))
      (doseq [i (lines group)] (draw-line plt i))
      (.setTitle plt (str group " TADMUDI and Simulation Results"))
      ;add the TADMUDI entry to the legend.
      (add-lines plt [] [] :series-label "TADMUDI and Peak Demand Periods")
      (remove-extra-legend-items plt)
      ;change the stroke of the new TADMUDI entry to match the stroke of draw-line
      ;(set-legend-stroke plt 1 3)
      (doto (new org.jfree.chart.ChartFrame group plt)
        (.setSize 500 400)
        (.setVisible show-charts?))
      [group plt]))))

;;Generic?
(defn dump-spark-charts
  "Load spark charts from a marathon audit trail directory specified by in and
  save all spark charts to the out directory. Calling an optional function f on
  each chart before saving."
  [in out & {:keys [group-fn f phase-lines? bounds]
             :or {group-fn (fn [s] "All") f identity
                  phase-lines? true bounds nil}}]
  (doseq [[group chart] (spark-charts in :group-fn group-fn
                                      :phase-lines? phase-lines? :bounds bounds)]
;;Create the out dir if it doesn't exist
    (clojure.java.io/make-parents (str out "."))
    (charts/simple-save-jfree-chart (f chart) (str out group ".png"))))

;;For now, removing srcs that aren't found across runs and data functions. If
;;you wanted to include those, every time you hit new run data, add all srcs,
;;and then later, add missing data string values. When computing delta's, if
;;either value isn't numeric, you can make an olddata->newdata string instead.
;;Generic?
(defn dump-sparks-for
  "Save the spark charts for the period named by period"
  [in out period group-fn]
  (let [[{:keys [Name FromDay ToDay]} :as periods]
        (filter (fn [{:keys [Name]}] (= Name period)) (util/load-periods in))]
    (when (not= (count periods) 1)
      (throw (ex-info (str "There needs to be only one period with that name.  There are "
                           (count periods) " with that name.") {})))
    (dump-spark-charts in out :group-fn group-fn
     :f (fn [cht] ;(util/set-bounds cht :x-axis :lower FromDay :upper ToDay)
          (.setTitle cht (str (.getText (.getTitle cht)) "\nTime Period: " Name))  cht )
     :phase-lines? false :bounds [FromDay ToDay])))


;(view (xy-plot [1 2 3 4 5] [5 8 2 9 5]))

;(add-polygon ap [[15 225] [35 225]])


;;___________________________________________________
;;comparing demand run statistics
;;there are differences between multiple marathon runs. we would like to know
;;whether the difference is cause by demand or supply
;;there is a similar namespace collected stats from runs in proc.stats, but instead of figuring out
;;how to re-use that, I'm writing this with that I've used here recently.

;;Generic.
;this doesn't account for deltat
(defn demand-sat-by
  "returns a map of {group percentmet} given a path to a marathon audit trail. Use group-fn to define the group as a function
  of the src string"
  [path & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (->> (simple-trends (util/load-trends path) :group-fn group-fn)
       (map (fn [[group recs]] [group (percent-satisfaction recs)]))
       (into {})))

;;Generic
(defn demand-sat-by-period
 "returns a map of {group percentmet} given a path to a marathon audit trail. Use group-fn to define the group as a function
  of the src string.  Use filter-fn to filter out demandtrend records. Supply a map of {Periodname [tstart tfinal]}, and
  results will be returned in a map of {[group Periodname] demandsat}."
  [path & {:keys [group-fn periods] :or {group-fn (fn [s] "All")}}]
  (let [trends (util/separate-by (fn [r] (group-fn (:SRC r))) (util/load-trends path))]
    ;;mapcat fn returns a seq of [periodname dematsat] or ["totaltime" demandsat]
    (mapcat (fn [[group xs]]
              (if periods
                (let [samples (proc.core/sample-demand-trends-correct xs)]
                  (for [[p [tstart tfinal]] periods]
                    [[group p]
                     (satisfaction-with-overlap (mapcat (fn [t] (map second (c/samples-at samples t))
                                                                     ) (range tstart (+ tfinal 1))))]
                    ))
               [[[group "alltimes"] (satisfaction-with-overlap xs)]]
                )) trends)))

;;Generic.
(defn demand-sat-from
 "given the path to a marathon audit trail, returns a map of the groups defined by group-fn to the percent of demand
satisfied.  "
  [path & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (demand-sat-by-period path :group-fn group-fn :periods (util/period-map-from path)))

;;Generic.
(defn demand-sats-for
  "demand-sat-from wrapper for audit-stats. Returns surge period only"
  [root & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  [(assoc (->> (demand-sat-from root :group-fn group-fn)
       (filter (fn [[[group period] _]] (= period "Surge")))
       (map (fn [[[group period] res]] [group (java.lang.Math/round (float res))]))
       (into {}))
          :fname "%surgemet")])

(defn demand-sats-prepostall
  "demand-sat-from wrapper for audit-stats.  Includes fill for presurge,
  surge, and presurge+surge periods."
  [root & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (let [{PreSurge "PreSurge" Surge "Surge" :as period-map}
        (util/period-map-from root)
        fill (demand-sat-by-period root :group-fn group-fn :periods
                                   (assoc period-map "entire" [(first PreSurge) (second Surge)]))
        group-map (fn [recs p sname]
                    (assoc
                     (->> fill
                          (filter (fn [[[group period] _]] (= period p)))
                          (map (fn [[[group period] res]] [group
                                                           (java.lang.Math/round (float res))]))
                          (into {}))
                     :fname sname))]
    [(group-map fill "PreSurge" "%presurgemet")
     (group-map fill "Surge" "%surgemet")
     (group-map fill "entire" "entire%met")]))

(defn average
  "compute the average of a sequence of numbers."
  [xs]
  (float (/ (reduce + xs) (count xs))))

;;might want to include non-deploying population in the future...
(defn avg-dwell
  "compute average dwell before deployment for deploying population
  only. Used for audit-sats."
  [root & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  [(assoc (->> (tbl/tabdelimited->records
                (str root "AUDIT_Deployments.txt") :schema
                schemas/deprecordschema)
               (into [])
               (remove (fn [{:keys [FollowOn DemandGroup]}]
                         (or FollowOn (= DemandGroup
                                         "RC_nonBOG-war"))))
               (group-by (fn [r] (group-fn (:UnitType r))))
               (map (fn [[group recs]] [group (average (map
                                                        :DwellYearsBeforeDeploy
                                                        recs))]))
               (into {}))
          :fname "avg_dwell")])

;;Generic
(defn inventories-for
  "by-compo-supply-map-groupf wrapper for audit-stats."
  [root & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (->> (for [[group {ac "AC" ng "NG" rc "RC"  :or {ac 0 ng 0 rc 0} :as invs}] (supply/by-compo-supply-map-groupf root :group-fn group-fn)
        [compo inv] {"AC" ac "NG" ng "RC" rc}]
         {group inv :fname (str compo "supply")})
       (group-by :fname)
       (map (fn [[fnm recs]] (reduce merge recs)))))

;;Generic.
;peak ;intervals ;group ;period
(defn surge-peaks
  "Uses peaks-from to return the surge period peaks."
  [root & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  [(assoc (->> (peaks-from root :periods (filter (fn [r] (= "Surge" (:Name r)))
                                                 (util/load-periods root))
                           :group-fn group-fn)
       (map (fn [r] [(:group r) (:peak r)]))
       (into {}))
       :fname "surge_peak"
  )])

;;when we add fns for audit-stats.  Keep track of the name of the statistics here.
(def statfs
  {demand-sats-for ["%surgemet"]
   inventories-for ["ACsupply" "NGsupply" "RCsupply"]
   surge-peaks ["surge_peak"]
   avg-dwell ["avg_dwell"]
   demand-sats-prepostall ["%presurgemet" "%surgemet" "entire%met"]})

(defn audit-stats
  "Compute statistics given a sequences of data functions called datafs.
   datafs operate on marathon audit trail directories.
   this fn returns records of
  {:data {'group1' 'datafres1'} :path 'blah' :dataname 'blah'}.

   dataf returns a seq of records like {group datafres :fname '%met'} where
   fname is a string name for the type of data.

   Each dataf should also accept the same group-fn"
  [root datafs & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (for [dataf datafs
        {:keys [fname] :as d} (dataf root :group-fn group-fn)]
    {:data (dissoc d :fname)
     :path root
     :dataname fname}))

(defn search-evolutions
  "The group (or SRC) might have changed over time. Track changes from one SRC to
  another in a map of {oldGroup newGroup}, but we should continue searching the
  map for multiple evolutions. Searches until a matching group is found in the
  data or there are no more evolutions. Arguments are the evolution map, the
  initial group, and a data map containing the keys of the groups. Returns the
  first val found in the data map or nil if no matches were found."
  [group evolutions data]
  (loop [grp group]
    (if-let [res (data grp)]
      res
      (when-let [newgroup (evolutions grp)]
        (recur newgroup)))))

(comment
  (def e {:a 1 :b 2 :c 3 3 2})
  (def d {:a 3 3 "a"})
  (search-evolutions :c e d)
  "a"
  (search-evolutions :a e d)
  3)

(defn stats-from
  "Given a sequence of dataf functions and a sequence of m4 [path pathname] pairs,
   compute each dataf for each path and return a sequence of records like
   {'group' 'blah' 'dataheader_pathname' 'blah'}.
   Only groups that exist in each result of dataf will be kept.
   In the case of SRC/group evolutions, you can also supply a map of
   {oldgroup newgroup}."
  [path-pairs datafs & {:keys [group-fn evolutions]
                        :or {group-fn (fn [s] "All") evolutions {}}}]
  (let [evolutions (if (nil? evolutions) {} evolutions)
        xs (for [[p pathname] path-pairs
                   stats (audit-stats p datafs :group-fn group-fn)]
               (assoc stats :pathname pathname))]
    (reduce (fn [acc {:keys [data path dataname pathname]}]
              (reduce-kv (fn [a g r]
                           (if-let [foundata (search-evolutions g evolutions data)]
                             (assoc a g (assoc r (str dataname "_" pathname) foundata))
                             (dissoc a g)))
  {} acc)) (zipmap (keys (:data (first xs))) (repeat (count (:data (first xs))) {})) xs)))

(defn compute-deltas
  "given the results of stats-from and the same inputs path-pairs and datafs,
  compute the dataf difference between the last path and the first path and
  assoc the deltas into the records with keys like 'datafstring_delta'."
  [statsfrom path-pairs datafs]
  (let [statnames (mapcat statfs datafs)
        firstpath (second (first path-pairs))
        lastpath  (second (last path-pairs))]
  (for [[group r] statsfrom]
    (assoc (reduce (fn [acc sname]
                     (assoc acc (str sname "_delta") (- (acc (str sname "_" lastpath))
                                                        (acc (str sname "_" firstpath)))))
                   r statnames) "group" group))))

(defn stat-order
  "given path-pairs and datafs, compute an ordered sequence of header names in
  order to write these records to a text file."
  [path-pairs datafs]
  (let [pathnames (map second path-pairs)]
    (reduce concat (for [s (mapcat statfs datafs)
                         p pathnames
                         :let [header (str s "_" p)]]
                     (if (= p (last pathnames))
                       [header (str s "_delta")]
                       [header])))))

(defn supply-delta-string
  "given a computed-delta record, compute a string which contains the delta for
   each component."
  [r]
  (reduce (fn [acc k]
            (let [res (r k)
                  write-delta (fn [sign] (str acc (if (= acc "") "" ", ")
                                              sign res " " (subs k 0 2)))]
              (if (= res 0)
                acc
                (if (pos? res)
                  (write-delta "+")
                  (write-delta "")))))
          ""
          ["ACsupply_delta" "NGsupply_delta" "RCsupply_delta"]))

(defn farb-comparison-2125
  "given the results of compute-deltas using inventories-for and demand-sats-for,
  generate the same records of data Jim used for requirements analysis. SRCs
  only for the group here since mapping SRC to oi title."
  [computedeltas oititles]
  (for [{src "group"
         taa2024 "%surgemet_taa2024"
         taa2125 "%surgemet_taa2125"
         delta "%surgemet_delta" :as r}  computedeltas
        :let [deltas (supply-delta-string r)]]
    {"High Interest Force Element" (str (oititles src)
                                        (if (= deltas "") "" " ") deltas )
     "m4 TAA 20-24 % Surge Demand Met" taa2024
     "m4 TAA 21-25 % Surge Demand Met" taa2125
     "change" delta}))

(def splus [demand-sats-for inventories-for])
(def peaks-only [surge-peaks])

(defn stats-with-deltas
  "returns a sequence of statistics with deltas."
  [path-pairs & {:keys [evolutions fns group-fn]
                 :or {fns splus group-fn (fn [s] "All")}}]
  (-> (stats-from path-pairs fns :group-fn group-fn :evolutions evolutions)
      (compute-deltas path-pairs fns)))

(defn spit-2125-comparison
  "spit a 2125-farb-comparison table to out."
  [out path-pairs & {:keys [evolutions fns] :or {fns splus}}]
  (-> (stats-with-deltas path-pairs :evolutions evolutions :fns fns)
      ;use the first path instead of last path since that will be the src we're
      ;keeping if there are evolutions.
      (farb-comparison-2125 (supply/oi-titles (first (first path-pairs))))
      (tbl/records->file out
           :field-order ["High Interest Force Element"
                         "m4 TAA 20-24 % Surge Demand Met"
                         "m4 TAA 21-25 % Surge Demand Met"
                         "change"])))

(def prefixes2226 ["ACsupply" "NGsupply" "RCsupply" "entire%met"
                   "%presurgemet" "%surgemet" "avg_dwell"])
(def postfixes2226 ["non-bog" "no-non-bog" "delta"])
(def order2 (concat ["group"] (for [prefix prefixes2226 postfix postfixes2226] (str
                                                              prefix "_" postfix))))

(defn spit-stats
  "spits the stats table to out."
  [out path-pairs & {:keys [evolutions fns group-fn field-order]
                     :or {fns splus group-fn (fn [s] "All")
                          field-order []}}]
  (tbl/records->file (stats-with-deltas path-pairs :evolutions evolutions
                                        :fns fns :group-fn group-fn)
                     out
                     :field-order field-order))

(defn stats-2226
  "Spit out the table of run statistics used to verify and produce
  results for TAA2226."
  [root]
  (binding [demandfilter (fn [{:keys [vignette]}] (not= vignette
                                                       "RC_NonBOG-War"))]
    (spit-stats (str root "stats.txt") [[(str root "m4book-NonBOG\\")
                                         "non-bog"]
                                        [(str root
                                              "m4book-No-NonBOG\\") "no-non-bog"]]
                :fns [demand-sats-prepostall inventories-for
                      avg-dwell]
                :group-fn (fn [s] s)
                :field-order order2)))

;;testing
(comment
(def p [["C:\\Users\\craig.j.flewelling\\Desktop\\snapchart\\testdata-v6early\\" "early"]])
(def s [demand-sats-for])

(def pplus [["C:\\Users\\craig.j.flewelling\\Desktop\\snapchart\\testdata-v6\\" "taa2024"] ["C:\\Users\\craig.j.flewelling\\Desktop\\snapchart\\testdata-v6early\\" "taa2125"]])

(def evolutions-example {"77302R000" "77202K000"})

(def out "C:\\Users\\craig.j.flewelling\\Desktop\\snapchart\\taa_comparison.txt")
(spit-2125-comparison out pplus splus)

(spit-stats "C:\\Users\\craig.j.flewelling\\Desktop\\snapchart\\med-stats.txt"
            pplus :fns peaks-only)

)

;;;_____________________medreform work

;;if there are no intervals, remove that record because there was no demand.
(defn first-peak-day
  "given the result of peaks-from, returns a sequence of records
  containing :peak :group :firstday :period"
  [p-from]
  (->> (remove (fn [{:keys [intervals] :as r}] (empty? intervals)) p-from)
       (map (fn [{:keys [intervals] :as r}]
              (let [first-day (ffirst intervals)]
                (assoc (dissoc r :intervals) :firstday first-day))))))

;;TODO seems like we already have this....
(defn activity-at
  "given an activity profile, returns the active records at time t."
  [activities t]
  (last (take-while (fn [[day m]] (<= day t)) activities)))

(defn quantities-by-demandgroup
  "given a map of {:actives [] :count}, where actives is a sequence of demand
  records, returns a map of :demand-group-key to quantity."
  [m & {:keys [demand-group-key] :or {demand-group-key :DemandGroup}}]
  (->> (:actives m)
       (group-by (fn [r] (r demand-group-key)))
       (map (fn [[g xs]]  [g (reduce + (map :Quantity xs))]))
       (into {})))

;;Generic?
(defn peak-parts
  "For each group of demand records defined by group-fn which operates
  on the src string and a demand-group-key that groups demands in the
  first group, returns a map of :group :period :peak
  :demand-group-key-value1 :demand-group-key-value2 etc. for the first day of peak demand in each
  period."
  [path & {:keys [group-fn demand-filter periods demand-group-key] :or
           {group-fn (fn [src] "All")
            demand-filter (fn [r]
                            true)
            demand-group-key :DemandGroup
            }}]
  (let [periods (if (nil? periods) (util/load-periods path) periods)
        sample-days (first-peak-day
                     (peaks-from path :group-fn group-fn
                                 :demand-filter demand-filter :periods
                                 periods))
        drecs (util/demand-records path)
        activity-map (->> (util/separate-by (fn [{:keys [SRC]}] (group-fn SRC)) drecs)
                          (map (fn [[g xs]] [g (temp/activity-profile xs :start-func :StartDay)]))
                          (into {}))]
    (for [{:keys [peak group firstday period] :as r} sample-days]
      (merge (->> firstday
           (activity-at (activity-map group))
           ((fn [x] (quantities-by-demandgroup (second x) demand-group-key)))) r))))

(defn columns->records
  "Given a sequence of records, keep all fields specified by a flds sequence and put remaining
  field values into a new column name by k and each remaining field key into a column named by fld.
  Asume that each record has the same fields."
  [recs flds k fld]
  (mapcat (fn [r] (let [baser (select-keys r flds)
                        nfs (apply dissoc r (keys baser))]
                    (if (> (count nfs) 0)
                      (for [[key v] nfs]
                        (assoc baser k v
                               fld (name key)))
                      [r]))) recs))

(defn peak-parts-less-fields
  "Returns records of peak-parts where the three fields are :group :period :peak :demandgroup."
  [path & {:keys [group-fn demand-filter] :or {group-fn (fn [s] "All")
                                               demand-filter (fn [r] true)}}]
  (columns->records (map (fn [r] (dissoc r :firstday))
                         (peak-parts path :group-fn group-fn :demand-filter demand-filter))
                    [:group :period] :peak :demandgroup))

(defn get-strength
  "retrns a map of the group defined by group-fn to total strength or number of
  personnel. Pull strength from a column in Audit_SupplyRecords called Strength.
  If no Strength is present, return 'nostrength' Assume strenth is the same for
  every src."
  [path & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (let [groups (->> (util/supply-records path)
                    (map (fn [r] (select-keys r [:SRC :Strength])))
                    (set)
                   (util/separate-by (fn [r] (group-fn (:SRC r)))))]
    (into {} (for [[g recs] groups]
               [g (if (:Strength (first recs)) (reduce + (map :Strength recs))
                      "nostrength")]))))

(def policies
  {"1:0" risk/c10
   "1:2" risk/c12
   "1:8" risk/c18})

(def policies-90-bog
  {"1:0" {:bog 3
          :cyclelength 3
          :overlap 1
          :mob 0}
   "1:2" {:bog 3
          :cyclelength 36
          :overlap 1
          :mob 0}
   "1:8" {:bog 3
          :cyclelength 108
          :overlap 1
          :mob 0}})

(defn policy-map
  "given a map of string names to policy definitions from proc.risk for both the rc and ac
  along with a map of comonent inventories, and a demand value returns a map of the concatentated
  string names for ac and rc to the % demand met."
  [acpolicies rcpolicies {ac "AC" ng "NG" rc "RC"} demand]
  (into {} (reduce concat (for [[anm apolicy] acpolicies
                                [rnm rpolicy] rcpolicies
                                :let [nm (str anm "/" rnm)
                                      performance (risk/performance demand
                                                                    {:ac-supply (if ac ac 0)
                                                                     :ac-policy apolicy
                                                                     :ng-supply (if ng ng 0)
                                                                     :ng-policy rpolicy
                                                                     :rc-supply (if rc rc 0)
                                                                     :rc-policy rpolicy})]]
             [[(str nm "-expected") (:total-supply performance)] [nm (supply/round-to 0 (* 100
                                   (:fill
                                    performance)))]]))))

;;need to make sure have all demand groups
(defn ensure-fields [recs]
  (let [keymap (into {} (for [k (into #{} (reduce concat (map (fn [r] (keys r)) recs)))]
                          [k nil]))]
    (for [r recs] (merge keymap r))))

;;as it stands, you will get a record for every grouped enabled demand
;;regardless of if that group is enabled in supplyrecords.

;;Then, inventory by component will be 0 if the group isn't enabled in the
;;supplyrecords.
(defn peak-records
  "returns data used to generate charts in Excel for static analysis.
   fields are SRC, RA, NG, AR (inventories), PAX, demandgroup1, demandgroup2,
   etc.
   (quantity of units demanded on the first day of peak demand), policy1,
   policy2, etc. (% demand met given total peak), peak"
  [path & {:keys [group-fn demand-filter rcpolicies acpolicies]
           :or {group-fn (fn [s] "All")
                demand-filter (fn [r] true)
                rcpolicies policies
                acpolicies {"1:0" risk/c10}}}]
  (let [inventory (supply/by-compo-supply-map-groupf path :group-fn group-fn)
        recs (->> (peak-parts path :group-fn group-fn :demand-filter demand-filter)
                  (filter (fn [{:keys [period]}] (= period "Surge"))))
        strengths (get-strength path :group-fn group-fn)]
    (for [{:keys [group peak] :as r} recs
          :let [{ac "AC" rc "RC" ng "NG" :or {ac 0 rc 0 ng 0}} (inventory group)]]
      (assoc (merge r {"AC" ac "RC" rc "NG" ng}
                    (policy-map acpolicies
                                rcpolicies
                                (inventory group)
                                peak)) :pax (strengths group)))))

(defn peak-records-to
  "spits the results of peak-records to a file"
  [path & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (tbl/records->file (ensure-fields (peak-records path :group-fn group-fn))
                     (str path "chartdata.txt")))

;_______________________________________________________
