;;A namespace to patch in stackedareaXY charts into incanter.
(ns proc.stacked
  (:require  [proc.util :as util]
             [incanter.core   :refer :all]
             [incanter.io     :refer :all]
             [incanter.charts :refer :all]
             [clojure.core.reducers :as r]
             [spork.util.table :as tbl]
             [spork.util.general :as gen]
             [spork.events.observe :as obs]
             [spork.util.temporal :as temp]
             [clojure.string :as str])
   (:import  [org.jfree.data.xy DefaultTableXYDataset 
                                XYSeries XYSeriesCollection 
              XYDataItem]
             [org.jfree.data.general Series AbstractDataset]
             java.awt.Color))

;;This is an ugly hack to ditch errors.
(defmacro do! [expr]
  `(try ~expr 
        (catch ~'Exception ~'hidden nil)))

(defmacro find-method [m class] 
  `(.getDeclaredMethod ~class  ~(str m) (into-array ~'Class [])))

(defmacro invoke! [klass obj meth & args] 
  `(let [meth# (doto (find-method ~meth ~klass)
                 (.setAccessible true))]
     (.invoke meth# ~obj (into-array ~klass [~@args]))))

(defn fire-dataset-changed! [obj]
  (do  (invoke! org.jfree.data.general.AbstractDataset 
                obj 
                fireDatasetChanged)
       obj))
           

;;Note: 
;;if we implement the TableXYDataSet interface, our data should
;;work fine with the stacked aread renderer...

;;imports a private function we "need" 
(def data-as-list #'incanter.charts/data-as-list)
(comment 



(def sand      "rootpath")
(def sand-ds   (util/read-keyed sand))
)

(defn set-xticks [plot tick]
  (.setTickUnit (.getDomainAxis (.getPlot plot)) (org.jfree.chart.axis.NumberTickUnit. tick)))
;; (defn hide-labels [plot]
;;   (.setVisible (.getDomainAxis (.getCategoryPlot plot)) false))

(defn parse-cat-fill [^String cat ^String fill]   
  (case fill  
    "Filled" (cond (.contains cat "Committed") "Committed"
                   (.contains cat "MissionFilled")   "Mission"
                   (.contains cat "Mission_NotDeployable") "Mission_NotDeployable"
                   (.contains cat "Mission") "Mission"
                   (= cat "Rotational") "Mission"
                   :else (clojure.string/replace cat "Filled" ""))
    "Unmet"  (cond (.contains cat "Committed")       "CommittedUnmet"
                   (.contains cat "Mission")   "MissionUnmet"
                   (= cat "Rotational")  "MissionUnmet"
                   :else (clojure.string/replace cat "Filled" ""))
        cat))

(defn parse-arfor-fill [^String cat ^String fill]   
  (case fill  
    "Filled" (cond (= cat "Rotational") "Deployed" ;better word than rotational?
                   (= cat "Prepare") "Demobilization" ;Is this always true? Could use :location field to capture this
               ;and use cat for Rotational only.
               :else cat)
    "Unmet"  fill
        cat))


  
;;This should take a fill-record and break it out into 
;; [:category :SRC :name :operation :start :duration :fill-type :unitid :quantity :end :location :compo 
;;  :Unit :DemandGroup :FillType :FollowOn :Component :DeploymentID :DwellYearsBeforeDeploy :DeployDate 
;;  :FollowOnCount :AtomicPolicy :Category :DeployInterval :FillPath :Period :Demand :PathLength :OITitle 
;;  :BogBudget :CycleTime :DeploymentCount :DemandType :FillCount :Location :DwellBeforeDeploy :Policy :deltat]

;;We should get 
;; {"Met Demand" :green  ;deployed from available, some criteria (dwell before deploy/location)
;;  "Not Fully Trained" :amber ;(deployed not from available, some criteria   (dwell before deploy/location)
;;  "ARNG on EPP from TR1 / TR2" :pink  ;(ARNG deployed from ready1/ready2) 
;;  "Extended Force" :red               ;Extended deployment to 15 mo?
;;  "Unmet Demand" :black               ;category = unmet
;;  }


;;unmet demands are missed demands..
(defn sufficiency-cat-fill 
  [acthresh rcthresh fill-type DwellBeforeDeploy Component DwellYearsBeforeDeploy]
  (cond (=  fill-type "Unmet")   "Unmet Demand"
        (< DwellBeforeDeploy acthresh) "Extended Force" ;This should probably fall under cases below now. Also,
       ;DwellBeforeDeploy is in days and acthresh is in years so this doesn't really make sense. well, maybe
        ;it does.  we are keeping it now. acthresh is probably .99something
  :else 
  (case Component
    "AC" (if (>= DwellYearsBeforeDeploy acthresh) "Met Demand" "Dwell: AC<1yr RC<4yr")
    (if (>= DwellYearsBeforeDeploy rcthresh) "Met Demand" "Dwell: AC<1yr RC<4yr"))))

(defn cat-by-demandgroup
  [acthresh rcthresh fill-type DwellBeforeDeploy Component DwellYearsBeforeDeploy DemandGroup]
  (cond (=  fill-type "Unmet")   (str DemandGroup "-Unmet Demand")
        (< DwellBeforeDeploy acthresh) (str DemandGroup "-Extended Force") ;This should probably fall under cases below now. Also,
       ;DwellBeforeDeploy is in days and acthresh is in years so this doesn't really make sense. well, maybe
        ;it does.  we are keeping it now. acthresh is probably .99something
  :else 
  (case Component
    "AC" (if (>= DwellYearsBeforeDeploy acthresh) (str DemandGroup "-Met Demand") (str DemandGroup "-Dwell: AC<1yr RC<4yr"))
    (if (>= DwellYearsBeforeDeploy rcthresh) (str DemandGroup "-Met Demand") (str DemandGroup "-Dwell: AC<1yr RC<4yr")))))

(defn suff-cat-fill-subs
  [FillType DemandType]
  (case FillType
    "Sub" "Unmet Demand"
    "Substitute" (case DemandType
                "77302R500" "IBCT sub"
                "77302R600" "IBCT sub"          
                "47112R000" "SBCT sub" 
                "87312R000" "ABCT sub")
    "Primary" "Primary Fill"))

;:dense meaning we have samples for every t as in sandtrends.  For sparse samping, use :sparse
;if we have sparse sampling, we'll use :duration and :sampled fields found in the fills data.  We'll remove records with :sampled true
;from the fills before generating the data for our sand chart.
(def ^:dynamic *sample-freq* :dense) 

(defn roll-sand [ds & {:keys [cols cat-function phase] 
                       :or   {cols [:category :fill-type] 
                              cat-function parse-cat-fill}}]
  (let [sparse? (= *sample-freq* :sparse)
        pred ($fn [DemandGroup Period sampled] (and (not= DemandGroup "NotUtilized") 
                                            (if sparse? (= sampled "false") true)
                                            (if phase (= Period phase) true)))] 
    
    ;;get categories first
    (->> ds 
      ($where pred) ;filtered out non-utilizers
      (add-derived-column :Category cols cat-function)
      ($rollup :sum :quantity (if sparse? [:Category :start :duration]
                                [:Category :start]))
      ($order :start :asc))))


;;Added to account for off-by-one errors in the reporting.  We have 
;;ac and guard that are following policies that get 364 vs. 365 days.
(def ^:constant +normal-year+ 0.99726)

;;alternative categorization
;;We want to convert the fills into a different set of criteria
(defn roll-fills [ds phase & {:keys [acthresh rcthresh]
                              :or   {acthresh +normal-year+
                                     rcthresh (* 4 +normal-year+)}}]
  (roll-sand ds :phase phase :cols [:fill-type :DwellBeforeDeploy :Component :DwellYearsBeforeDeploy]
                                              :cat-function (partial sufficiency-cat-fill acthresh rcthresh)))

(defn fills-by-demandgroup [ds phase & {:keys [acthresh rcthresh]
                              :or   {acthresh +normal-year+
                                     rcthresh (* 4 +normal-year+)}}]
  (roll-sand ds :phase phase :cols [:fill-type :DwellBeforeDeploy :Component :DwellYearsBeforeDeploy :DemandGroup]
                                              :cat-function (partial cat-by-demandgroup acthresh rcthresh)))
;;;;__________                

(defn sparse-samples [xs]
  (for [[t temp-map] (temp/activity-profile xs :start-func :start :duration-func :duration)
        [c recs] (let [groups (group-by :Category (:actives temp-map))]
                   (if (empty? groups)
                     [["no activities"]]
                     groups)) ] ; this is going to skip over the empty categories
    {:Category c :start t :quantity (if recs
                                      (reduce + (map :quantity recs))
                                      0)}))

;since fill data has sparse samples AND we are showing fill and unmet only, we can't assume fills are active until the next time sample,
;we can use an activity profile to find out when 
;changes happen.  To make sure our fills charts has nearly
;vertical lines for changes in quantity, we need to replicate some of the data a very small distance before any changes.
(defn daily-samples [xs] ;xs includes :duration this time
  (let [sparse? (= *sample-freq* :sparse)
        end (atom nil)
        xs (if sparse? (sparse-samples xs) xs)
        bounds (partition 2 1 (gen/clumps :start xs))
        ;dont need these samples
        clean-bounds (if sparse? (filter (fn [[[tl xs] [tr ys]]] (not= (:Category (first xs)) "no activities")) bounds) bounds) ]     
    (as->   
             (reduce (fn [acc [[tl xs] [tr ys]]]   
                       (do (when (and sparse? (= (:Category (first @end)) "no activities")) 
                             ;we know we just came from 0 and want to smooth things out before xs start
                             (reduce conj! acc (r/map (fn [r] (assoc r :start (- tl 0.001) :quantity 0)) xs))) 
                         ;we only have to do this at the end I think-not for each partition (but we don't know when we're at end)
                         (reset! end ys) 
                         (loop [n (unchecked-inc tl)
                                inner (reduce conj! acc xs)]
                           (if (== n tr) ;inner
                             (do (when (and sparse? (= (:Category (first ys)) "no activities"))
                                 (reduce conj! inner 
                                         (r/map (fn [r] (assoc r :start n :quantity 0)) xs))) ;first sample of no activities
                               (reduce conj! inner 
                                       (r/map (fn [r] (assoc r :start (- n 0.001))) xs))) ;smoothing sample before next xs
                             (recur (unchecked-inc n)
                                    (reduce conj! inner 
                                            (r/map (fn [r] (assoc r :start n)) xs)))))))
                     (transient []) clean-bounds)
           v
           (persistent! (if sparse? v (reduce conj! v @end)) ;If sparse?, the end will always be "no activities" now
                        ))))
  
;; (def xytbl (DefaultTableXYDataset.))

;;So, the easiest way to get a dataset into multiple xy-series, is to 
;;use group-by on the categories, and then for each group, and define
;;the x/y keys to collect by. 

;;this is kinda weak.
;; (defn xy-by 
;;   ([xkey ykey ds] ($ [xkey ykey] ds))
;;   ([xkey ykey group-key ds] 
;;      (->> ds
;;           ($ [xkey ykey])
;;           ($group-by group-key))))


;;One dumb way to do this is to build up the series manually, and just 
;;live in JFreeChart.
;;We'll do that...

;;In clojure/incanter, we're computing the categories by 
;;grouping...
;;then for each 


;;So, the problem we have is this: 
;;Incanter provides a nice, idiomatic wrapper around JFreeChart's 
;;stackedArea chart.  It defaults to using a categorical axis, 
;;which is great for some things.  However, in our work, we 
;;want to plot timeseries data in a stacked area fashion.  So, 
;;with thousands of samples, the categorical axis gets boffed and 
;;cannot display the information (it can't even draw really). 
;;The solution to this is to do 


;;The first task is to create a DefaultTableXYDataset, which 
;;is composed of XYSeries via addSeries. 

;;Each series is a category.


(comment 
;;THis is different then incanter...it uses a categorical series by default.
(defn stacked-areaxy-chart*
  ([categories values & options]
    (let [opts        (when options (apply assoc {} options))
          data        (or (:data opts) $data)
          _values     (data-as-list values data)
          _categories (data-as-list categories data) ;;only difference
          ;;is that categories are now series...
          title       (or (:title opts) "")
          theme       (or (:theme opts) :default)
          _group-by   (when (:group-by opts)
                        (data-as-list (:group-by opts) data))
          ;;group-by is now the series....
          x-label      (or (:x-label opts) "time")
          y-label      (or (:y-label opts) "Values")
          series-label (:series-label opts)
          vertical?    (if (false? (:vertical opts)) false true)
          legend?      (true? (:legend opts))
          dataset      (org.jfree.data.xy.DefaultXYDataset.)
          chart   (org.jfree.chart.ChartFactory/createStackedAreaXYChart
                     title
                     x-label
                     y-label
                     dataset
                     (if vertical?
                       org.jfree.chart.plot.PlotOrientation/VERTICAL
                       org.jfree.chart.plot.PlotOrientation/HORIZONTAL)
                     legend?
                     true
                     false)]
      (do
        (doseq [i (range 0 (count _values))]
          (.addValue dataset
                     (nth _values i)
                     (cond
                      _group-by
                        (nth _group-by i)
                      series-label
                        series-label
                      :else
                        (str 'values))
                     (nth _categories i)))
          (set-theme chart theme)
          chart))))
)



;;It kind of sucks that we don't have a reduction in incanter....

;;like group-by, except we compute a jfreechart XYSeries object for
;;each unique value of series-key.  The whole point of this is to get 
;;something JFreechart likes...we might just extend the interfaces to
;;incanter datasets or something.  For now, it works.
(defn series-by [x-key y-key series-key xs]
  (let [order      (atom [])
        get-series! (fn get-series! ^XYSeries [series k] 
                      (if-let [res (get @series k)]
                        res
                        (let [s (XYSeries. k true false)
                              _ (swap! series assoc k s)
                              _ (swap! order (fn [xs] (conj xs [(count xs) k])))]
                          s)))]
    (with-meta   (->> xs
                      (reduce (fn [acc r]
                                (let [^XYSeries s (get-series! acc (series-key r))]
                                  (do (.add s ^double (x-key r) ^double (y-key r))
                                      acc))) (atom {}))
                      (deref))
      (let [os @order]
           {:order {:idx->key   (into {} os)
                    :key->order (into {} (map (fn [[l r]] [r l]) os))}}))))


;;we need to wrap the dataset..
(defn ^org.jfree.data.xy.DefaultTableXYDataset  
  build-datatable [series-map & {:keys [order-by]}]
  (let [{:keys [idx->key key->order]}   (get (meta series-map) :order)
        idxs   (range (count idx->key))
        trends (keys   key->order)
        order  (if order-by 
                 (sort-by order-by trends)  
                 (sort-by (fn [k] (key->order k)) trends))
        addf   (fn [^DefaultTableXYDataset ds ^XYSeries ser]                 
                 (doto ds (.addSeries ser)))]
    (reduce (fn [^DefaultTableXYDataset ds nm] 
              (let [^XYSeries series (get series-map nm) ]
                (addf ds series)))
            (org.jfree.data.xy.DefaultTableXYDataset.)
            order)))

(defn get-color [k]
  (let [[r g b] (get spork.graphics2d.canvas/color-specs k)]
    (java.awt.Color. r g b)))

(defn faded 
  ([clr alpha]
     (let [clr (get-color clr)]
       (java.awt.Color. 
        (spork.graphics2d.canvas/get-r clr)
        (spork.graphics2d.canvas/get-g clr)
        (spork.graphics2d.canvas/get-b clr)
        alpha)))
  ([clr] (faded clr 100)))
             
             

(def ^:dynamic *pallete*
  {:orange         (java.awt.Color. 255 204 51)
   :light-orange   (java.awt.Color. 255 134 36)
   :faded-orange   (java.awt.Color. 255 134 36 100)
   :blue           (java.awt.Color. 51  51  255)
   :light-blue     (java.awt.Color. 102 204 255)
   :yellow         (java.awt.Color. 255 255 0)
   :faded-blue     (java.awt.Color. 102 204 255 100)
   :green          (java.awt.Color. 102 204 0)
   :light-green    (java.awt.Color. 204 255 204)
   :pink           (get-color :pink)
   :red            (get-color :red)
   :black          (get-color :black)
   :amber          (java.awt.Color. 255 204 0)})
  
;;Allows us to have some trend colors faded..
(defmacro with-faded [& expr]
  `(binding [~'*pallete* (merge ~'*pallete* {:light-blue :faded-blue
                                             :light-orange :faded-orange})] 
     ~@expr))
  
;;our default coloring 
(def default-colors 
  {:unmet-committed  (java.awt.Color. 255 134 36)
   :unmet-mission    (java.awt.Color. 102 204 255)
   :mission          (java.awt.Color. 51  51  255)
   :committed        (java.awt.Color. 255 204 51)
   :transition-ready (java.awt.Color. 204 255 204)
   :prepare          (java.awt.Color. 255 255 0)
   :ready            (java.awt.Color. 102 204 0)})

(def default-order ["Reset"
                    "PrepareFilled" 
                    "Prepare"                     
                    "TransitionReadyFilled" 
                    "TransitionReady" 
                    "ReadyFilled"
                    "Ready"
                    "Available"
                    "Mission_NotDeployable"
                    "Mission"
                    "MissionFilled"
                    "CommittedFilled" 
                    "Committed" 
                    "MissionUnmet"
                    "CommittedUnmet"
                    "Primary Fill"
                    "IBCT sub"
                    "SBCT sub"
                    "ABCT sub"
                    "Met Demand"
                    "Not Fully Trained"
                    "Dwell: AC<1yr RC<4yr"
                    "ARNG on EPP from TR1 / TR2"
                    "Extended Force"
                    "Unmet Demand"])


;; {"Met Demand" :green  ;deployed from available, some criteria (dwell before deploy/location)
;;  "Not Fully Trained" :amber ;(deployed not from available, some criteria   (dwell before deploy/location)
;;  "ARNG on EPP from TR1 / TR2" :pink  ;(ARNG deployed from ready1/ready2) 
;;  "Extended Force" :red               ;Extended deployment to 15 mo?
;;  "Unmet Demand" :black               ;category = unmet
;;  }

(def srm-color {"PrepareFilled" :yellow
                "Prepare"       :yellow
                "TransitionReadyFilled" :light-green
                "TransitionReady"      :light-green
                "ReadyFilled"      :green
                "Ready"            :green
                "CommittedFilled"  :orange
                "Committed"        :orange
                "CommittedUnmet"   :faded-orange
                "Available"        :green
                "MissionFilled"    :blue
                "Mission"          :blue
                "Mission_NotDeployable" :light-blue
                "MissionUnmet"     :faded-blue
                "IBCT sub" :orange
                "SBCT sub" :blue
                "ABCT sub" :red
                "Primary Fill" :green
                "Met Demand"         :green
                "Not Fully Trained"  :amber 
                "Dwell: AC<1yr RC<4yr" :amber
                "ARNG on EPP from TR1 / TR2" :pink
                "Extended Force"     :red
                "Unmet Demand"       :black})

(def arforgen-color-2 {"Demobilization" :maroon
                       "Reset"       :red
                       "Reset_Deployable" :light-red
                       "Train"       :orange                     
                       "Ready"       :yellow
                       "Available"   :light-green
                       "Available_Deployable" :green
                       "Deployed"    :olive-drab
                       "Unmet" :black})

(def arforgen-order-2 ["Demobilization"
                       "Reset"       
                       "Reset_Deployable" 
                       "Train"                            
                       "Ready"       
                       "Available"   
                       "Available_Deployable" 
                       "Deployed"    
                       "Unmet"])

(def arforgen-color {"Reset"       :red 
                     "Train"       :red
                     "Reset/Train" :orange                      
                     "Ready"       :yellow
                     "Recovery"    :light-green
                     "Available"   :green 
                     "Deployed"    :blue})



(def deployment-colors {"AC"     :blue 
                        "NG"     :red 
                        "RC"     :red
                        "Ghost"  :black})

(def default-color srm-color)

(def cyan (java.awt.Color. 128 250 250))

(def green (java.awt.Color. 128 250 128))

(def blue (java.awt.Color. 127 127 250))

(def red (java.awt.Color. 250 127 127))

(def d-color {"Disrupt" red
              "Defend" blue
              "Deter" green
              "Defeat" cyan})

(def d-order ["Disrupt" "Deter" "Defend" "Defeat"])

(def d-info {:color d-color :order d-order})


;;Dynamic var that lets us choose how to stylize and order our trends,
;;so we're consistent.  Ideally, all a use has to do is set
;;with-trend-info,  and as we create charts, we use said trend information.
(def ^:dynamic *trend-info* {:color default-color 
                             :order default-order})

;;Why don't we just create the table as needed?  We don't really need 
;;to store it, other than to trigger change notifications....maybe 
;;we do it lazily....once we have a series map, we can create the 
;;dataset.  The dataset allows us to add data (by [x {trend y}] pairs)
;;to the dataset.
(defprotocol IXYData
  (^DefaultTableXYDataset as-tablexy [obj])
  (^XYSeries series [obj nm])
  (series-seq       [obj])
  (get-bounds       [obj]))

(defprotocol IOrderedSeries (order-series-by [obj f]))
(defprotocol IChartable (as-chart [obj opts]))
(defprotocol IColorable     (set-colors [obj colors]))
(defprotocol IReactiveData  
  (set-notify [obj v])
  
  (add-samples [plt samples]))


(defn chart [obj] (as-chart obj {}))


(defn vector-ordering [xs]
  (reduce-kv (fn [acc k v]
               (assoc acc v k)) {} xs))

(defn as-order-function [f]
  (cond (vector? f) (let [m (vector-ordering f) bound (inc (count f))] (fn [kv] (get m (first kv) bound))) ;order is implied by position
        (map?    f) (fn [kv] (f (first kv)))   ;order is implied by mapping of series->order
        (fn?     f) f
        :else (throw (Exception. (str "unknown ordering function " f)))))

(defn order-series-by! [^DefaultTableXYDataset tab order-func]
  (let [ofunc (as-order-function order-func)]
        (reduce (fn [^DefaultTableXYDataset acc [nm ^XYSeries ser]]
                  (doto acc (.addSeries ser)))
                (DefaultTableXYDataset.)    
                (sort-by ofunc (series-seq tab)))))

(defn derive-order [tab]
    (reduce (fn [{:keys [idx->key key->order]} [k ^XYSeries ser]]
              {:idx->key (assoc idx->key (count idx->key) k)
               :key->order (assoc key->order k (count idx->key))})
            {:idx->key {}
             :key->order {}}
            (series-seq tab)))


(declare       stacked-areaxy-chart2*)

;;there's some weirdness with the duplicate value error, I'm just
;;going to trap it with a try-catch,finally and ignore it.

(extend-type   DefaultTableXYDataset
  IOrderedSeries
  (order-series-by [obj f] (order-series-by! obj f))
  IXYData
  (as-tablexy [obj] obj)
  (series     [obj nm]  (reduce (fn [acc idx] 
                                  (let [^XYSeries ser (.getSeries obj idx)
                                        k (.getKey ser)]
                                    (if (= k nm)
                                      (reduced ser)
                                      acc)))
                                nil 
                                (range (.getSeriesCount obj))))
  (series-seq [obj] (let [cnt (.getSeriesCount obj)]
                      (map (fn [idx] (let [s (.getSeries obj idx)]
                                       [(.getKey s) s]))
                           (range cnt))))
  (get-bounds [obj] (let [^XYSeries s (.getSeries obj 0)]
                      [(.getMinX s) (.getMaxX s)]))
  IReactiveData
  (set-notify [obj v] (let [v (boolean v)]
                        (doseq [[_ ^XYSeries ser] (series-seq obj)]
                          (.setNotify ser v))
                        obj))
  (add-samples [obj samples]
    (let [series-map (into {} (series-seq obj))]
      (do (set-notify obj false)
          (doseq [s samples]
            (doseq [[nm ^double x ^double y] s]
              (.addOrUpdate ^XYSeries (get series-map nm) x y)))  ;;use addOrUpdate to get around false dupes.
          (do! (set-notify obj true))
          (fire-dataset-changed! obj)
          ))))

 
(extend-type   XYSeriesCollection
  IOrderedSeries
  (order-series-by [obj order-func]
    (let [ofunc (as-order-function order-func)]
      (reduce (fn [^XYSeriesCollection acc  [nm ^XYSeries ser]]
                (doto acc (.addSeries ser)))
              (XYSeriesCollection.)    
              (sort-by ofunc (series-seq obj)))))
  IXYData
  (as-tablexy [obj] obj)
  (series     [obj nm]  (.getSeries obj nm))
  (series-seq [obj]     (map (fn [^XYSeries s] [(.getKey s) s]) (seq (.getSeries obj))))
  (get-bounds [obj]    (let [^XYSeries s (.getSeries obj 0)]
                         [(.getMinX s) (.getMaxX s)]))
  IReactiveData
  (set-notify [obj v] (let [v (boolean v)]
                        (doseq [[_ ^XYSeries ser] (series-seq obj)]
                          (.setNotify ser v))
                        obj))
  (add-samples [obj samples]
    (let [series-map (into {} (series-seq obj))]
      (do (set-notify obj false)
          (doseq [s samples]
            (doseq [[nm ^double x ^double y] s]
              (.addOrUpdate ^XYSeries (get series-map nm) x y)))  ;;use addOrUpdate to get around false dupes.
          (do! (set-notify obj true))
          (fire-dataset-changed! obj)
          ))))

;(add-samples xyt  (for [[t rs] (gen/clumps :start simple-samples) r rs]  [(:Category r)  t (:quantity r)]))

;;If we want to construct a series incrementally, it'd be nice
;;if we could conj a data point [x {series1 y series2 y ....}] 
;;and then redraw. 
;;This would be equivalent to adding "slices" to the stacked 
;;area over time.  This should allow us to animate 
;;the chart over time, and add data at regular intervals in 
;;a general fashion,  Similarly, we can copy this technique 
;;for other datasets, and get animated charts pretty easily.
;;So...maybe we have a container for our series, and for 
;;the dataset.  Once we have a handle on the series, we 
;;can get a series map.  Adding points is as simple as 
;;traversing the series map and adding values to each, 
;;not notifying the series change until the end.  After
;;we add all the points, we trigger a DataSetChanged event.
;;Really, all the dataset does is serve as an interface to 
;;jfree...
(defrecord xydataset [^DefaultTableXYDataset table seriesmap order] ;this is like a java class
  ;this implements these protocols. A record is just a hash-map record with some fancy java stuff....
  ;Deftype if you don't care if it's a hashmap.
  ;reify and produce a bunch of protocols-Anoymous object that implements a bunch of protocols.
  IOrderedSeries
  (order-series-by [obj f]
    (let [tab  (order-series-by! table f)]
      (xydataset. tab seriesmap (derive-order tab))))
  IXYData
  (as-tablexy [obj] table)
  (series     [obj nm]  (get seriesmap nm))
  (series-seq [obj]     (series-seq table))
  (get-bounds [obj]   (let [^XYSeries s (first (vals seriesmap))]
                           [(.getMinX s) (.getMaxX s)]))
  IChartable 
  (as-chart [obj  opts] (apply stacked-areaxy-chart2* obj (if (map? opts) 
                                                            (flatten (seq opts))
                                                            opts)))
  IReactiveData
  (set-notify [obj v] (do (set-notify table v) obj))
  (add-samples [obj samples] (do (add-samples table samples) obj)))


(defn clear-series [xyd] 
  
  (do (doseq [[nm ^XYSeries ser] (series-seq xyd)]
        (do! (.clear ser)))
      (fire-dataset-changed! (:table xyd))
      xyd))

(defn color? [c]  (instance?  java.awt.Paint c))
(defn get-color! [c] 
  (if (color? c) c
      (if-let [c (get *pallete* c)]
        (if (keyword? c) (get-color! c)
            c)
        (if-let [c (get spork.graphics2d.canvas/color-specs c)]
          (java.awt.Color. (int (first c)) (int (second c)) (int (nth c 2)))
          (throw (Exception. (str "unknown color " c)))))))        

(extend-type org.jfree.chart.JFreeChart  ;extend-protocol opposite. or extend-type
  IColorable 
  (set-colors [obj colors]
    (let [^org.jfree.chart.plot.XYPlot plot (.getXYPlot ^org.jfree.chart.JFreeChart obj)]      
      (doseq [n  (range (.getDatasetCount plot))]
        (let [ds (.getDataset plot (int n))
              ^org.jfree.chart.renderer.xy.XYItemRenderer xyr (.getRendererForDataset plot ds)]
          (doseq  [ [idx [nm ^XYSeries ser]] (map-indexed vector (series-seq ds))]
            (when-let [c (get colors nm)]
              (.setSeriesPaint xyr  (int idx) (get-color! c))))))))
  IXYData
  (as-tablexy [obj] (.getDataset (.getXYPlot obj)))
  (series     [obj nm]  (series (.getDataset (.getXYPlot obj)) nm))
  (series-seq [obj]     (series-seq (.getDataset (.getXYPlot obj))))
  (get-bounds [obj]   (let [^XYSeries s (second (first (series-seq obj)))]
                           [(.getMinX s) (.getMaxX s)])))
;add new protocol called IChart new get-bounds (call it something else),
;set bounds, set range, and set domain, and extend this protocol here.


(defn xycoords [^XYSeries ser]
  (map (fn [^XYDataItem xy]
         [(.getX xy) (.getY xy)])
       (.getItems ser)))
  

(defn set-domain! [^org.jfree.chart.JFreeChart obj min max]
  (let [^org.jfree.chart.plot.XYPlot plot (.getXYPlot ^org.jfree.chart.JFreeChart obj) ;xyplot is main plot obj
        ax (.getDomainAxis plot)]
    (do (.setRange ax min max)))) ;return obj at the end. same with range

(defn set-range! [^org.jfree.chart.JFreeChart obj min max]
  (let [^org.jfree.chart.plot.XYPlot plot (.getXYPlot ^org.jfree.chart.JFreeChart obj)
        ax (.getRangeAxis plot)]
    (do (.setRange ax min max))))

(defn copy-axes! [^org.jfree.chart.JFreeChart l ^org.jfree.chart.JFreeChart r]
  (let [rx (.getDomainAxis (.getXYPlot r)) 
        ry (.getRangeAxis  (.getXYPlot r))]
    (do (.setRange rx (.getRange (.getDomainAxis (.getXYPlot l)) ))
        (.setRange ry (.getRange (.getRangeAxis  (.getXYPlot l)))))))

(defn get-axis [chart axis]
  (case axis
    :x (.getDomainAxis (.getXYPlot chart))
    :y (.getRangeAxis (.getXYPlot chart))
    (throw (ex-info "unknown axis!, expected :x or :y" {:axis axis}))))

;;shady hack to cope with new jfreechart API.
;;http://www.jfree.org/forum/viewtopic.php?t=117509
(defn set-axis-decimals! [chart axis decimals]
  (let [num-axis (get-axis chart axis)
        decimals (long decimals)]
    (if (zero?  decimals)
      (.setStandardTickUnits num-axis (org.jfree.chart.axis.NumberAxis/createIntegerTickUnits))
      (let [num-form (java.text.NumberFormat/getNumberInstance)
            _ (.setMaximumFractionDigits num-form decimals)
            _ (.setNumberFormatOverride num-axis num-form)]))))

;;Added to fix a problem in test output, where tiny values on the
;;y-axis were producing duplicates instead of an equally tiny
;;autorange.
;;http://www.jfree.org/forum/viewtopic.php?t=117649
(defn set-autorange-minimum-size! [chart axis size]
    (let [^org.jfree.chart.axis.ValueAxis num-axis (get-axis chart axis)
          ]
      (.setAutoRangeMinimumSize num-axis (double size))
      chart))

;;if we wanted to order the xydataset, how would we? 
;;we'd have to removeall series....
;;then add them in the prescribed order.

;;The series wouldn't change, but the datatable would...

;;Produces a wrapper around the data table from jfree, allows us to 
;;access it from clojure.
(defn xy-table
  [xkey ykey & options]
    (let [opts         (when options (apply assoc {} options))
          data         (or (:data opts) $data)
          _group-by    (when (:group-by opts) (:group-by opts)) ; (data-as-list (:group-by opts) data)) 
          ;;new
          series-map   (series-by xkey ykey (or _group-by (fn [_] "Series")) (util/records #_:rows data))
          ;;new
          order        (get (meta series-map) :order)
          dtbl         (build-datatable   series-map)]
      ;;the difference between the regular area and this guy is that
      ;;we have a category, defined by group-by, and the xs and ys....
      ;;I'll rename _categories to xs at some point and values to
      ;;ys......
      (->xydataset dtbl series-map order)))

;;given an ordering, and a map of series->color, we hav the ability
;;to manually set out colors.  I'll also use this to extend the
;;dotplots.  Maybe have a dynamic var that binds series ordering and
;;coloring, so we have a consistent theme.  The challenge here is that 
;;we don't have a map of name->series-index, so we have to pack it as 
;;we build the dataset.  Since we're doing it manually here, that's
;;fine, except we may have to change it for the scatter plots and
;;other stuff.
(defn stacked-areaxy-chart*
  [xkey ykey  & options]
    (let [opts         (when options (apply assoc {} options))
          data         (or (:data opts) $data)
          ;; _values     (data-as-list values data) ;old
          ;; _categories (data-as-list categories data) ;;only difference
          ;;is that categories are now series...
          title        (or   (:title opts)     "")
          theme        (or   (:theme opts)     :default)
          _group-by    (when (:group-by opts) (:group-by opts)) ; (data-as-list (:group-by opts) data)) 
                      ;;this doesn't actually "group" anything...
          _order-by    (if (:order-by opts) (:order-by opts))
          ;;new 
          _color-by    (when (:color-by opts) (:color-by opts))
          ;;group-by is now the series....
          x-label      (or (:x-label opts) "time")
          y-label      (or (:y-label opts) "quantity")
          series-label (:series-label opts)
          vertical?    (if (false? (:vertical opts)) false true)
          legend?      (true? (:legend opts))
          ;;new
          series-map   (series-by xkey ykey (or _group-by (fn [_] "Series")) (util/records #_:rows data))
          ;;new
          order        (get (meta series-map) :order)
          dtbl         (build-datatable series-map)
          chart        (org.jfree.chart.ChartFactory/createStackedXYAreaChart
                        title
                        x-label
                        y-label
                        dtbl
                        (if vertical?
                          org.jfree.chart.plot.PlotOrientation/VERTICAL
                          org.jfree.chart.plot.PlotOrientation/HORIZONTAL)
                        legend?
                        true
                        false)]
      ;;the difference between the regular area and this guy is that
      ;;we have a category, defined by group-by, and the xs and ys....
      ;;I'll rename _categories to xs at some point and values to ys......
      (do
        (set-theme     chart theme)
        (.setAntiAlias chart false)
        chart)))

(defn stacked-areaxy-chart2*
  [^xydataset xytable & options]
  (let [opts         (if options (apply assoc {:legend true :aa false} options)
                         {:legend true  :aa false})
        ;; _values     (data-as-list values data) ;old
        ;; _categories (data-as-list categories data) ;;only difference
        ;;is that categories are now series...
        title        (or   (:title opts)        "")
        theme        (or   (:theme opts)  :default)
        ;;new 
        _color-by    (or (:color-by opts)
                         (get *trend-info* :color))
        ;;group-by is now the series....
        x-label      (or (:x-label opts) "time (days)")
        y-label      (or (:y-label opts) "quantity required (units)")
        y-decimals   (or (:y-decimals opts) 0)
        series-label (:series-label opts)
        vertical?    (if (false? (:vertical opts)) false true)
        legend?      (true? (:legend opts))
        _order-by    (or (:order-by opts) (get *trend-info* :order))
        tickwidth    (when  (:tickwidth opts) (:tickwidth opts))
        ^xydataset 
        xytable      (order-series-by xytable _order-by)
        chart        (org.jfree.chart.ChartFactory/createStackedXYAreaChart
                      title
                      x-label
                      y-label
                      (.table xytable)
                      (if vertical?
                        org.jfree.chart.plot.PlotOrientation/VERTICAL
                        org.jfree.chart.plot.PlotOrientation/HORIZONTAL)
                      legend?
                      true
                      false)]
    ;;the difference between the regular area and this guy is that
    ;;we have a category, defined by group-by, and the xs and ys...
    ;;formatting bs that changed in the newer jfree dependency.
    ;;this will now hose the chart if we don't use the approved
    ;;path for creating IntegerTickUnits.
    ;;http://www.jfree.org/forum/viewtopic.php?t=117509
    (do (set-axis-decimals! chart :y 0)
        (.setAntiAlias chart  (get opts :aa))
        (set-colors    chart _color-by)
        (when tickwidth (set-xticks chart tickwidth))
        (set-theme     chart  theme)
        chart)))

(defn sand-chart [ds & {:as opts}]
  (->> ds 
       (roll-sand)
       (stacked-areaxy-chart2* :start
                               :quantity
                               :legend true
                               :group-by :Category :data)))

(def ^:dynamic *sampling* :daily)

;;Since incanter 1.9.1 uses a new dataset implementation, we
;;really don't want to mess with the internals like we used to.
;;So, accessing rows the old way is a nono.
(defn expand-samples [ds & {:keys [samplefn] :or {samplefn daily-samples}}]
  (if (= *sampling* :daily)
    ;(assoc ds :rows (samplefn (:rows ds)))
    (incanter.core/dataset (incanter.core/col-names ds)
                           (samplefn (util/records ds)))

    ds))
  

(defn sand-data [ds]
  (->>  (util/as-dataset ds)
        (roll-sand)
        (expand-samples)
        (xy-table :start :quantity :group-by 
                  :Category :data)))

(def ^:dynamic *by-demandgroup?* false)

(defn trend-info-by-dmdgroup [rolled-fills]
  (let [group-stack-height (fn [dgroup] (let [caps (str/upper-case dgroup)]
                                          (case caps "UNGROUPED" 0 "HLD" 1 (abs (hash caps)))))
        cat-stack-height (fn [cat] (case cat "Met Demand" 0 "Dwell: AC<1yr RC<4yr" 1 "Extended Force" 2 "Unmet Demand" 3))   
        cats (->> (util/records #_:rows rolled-fills)
               (map :Category)
               (set)
               (map (fn [s] (let [[dgroup cat]  (str/split s #"-")]
                              {:CompositeCat s
                               :DemandGroup dgroup 
                               :Category cat 
                               :GroupLevel (group-stack-height dgroup) 
                               :CatLevel (cat-stack-height cat)})))  
               (sort-by (juxt :GroupLevel :CatLevel)))
        order (map :CompositeCat cats)
        colors (into {} (map (fn [r] [(:CompositeCat r) (get srm-color (:Category r))]) cats))]
    {:color colors :order (vec order)}))
  
(defn fill-data [ds phase subs]
  (binding [*sample-freq* :sparse] 
    (let [rolled-fills (if subs
                         (roll-sand (util/as-dataset ds) :phase phase :cols [:FillType :DemandType] :cat-function suff-cat-fill-subs)
                         (if *by-demandgroup?* 
                           (fills-by-demandgroup (util/as-dataset ds) phase)
                           (roll-fills (util/as-dataset ds) phase)))
          expanded (expand-samples rolled-fills :samplefn daily-samples)]
        [(xy-table :start :quantity :group-by :Category :data expanded) (if *by-demandgroup?*
                                                                          (trend-info-by-dmdgroup rolled-fills)
                                                                          *trend-info*)])))

(defmethod view proc.stacked.xydataset [xyt & options]
  (view (as-chart  xyt  options)))  


(defn trend-samples [trend-xys]
  (for [[t xys] trend-xys
        [x y] xys]
    [t x y]))
  
(defn sampletrends [n & {:keys [l r] :or {l 0 r 10}}]
  (let [trends '("CommittedFilled" "MissionFilled" "ReadyFilled" 
                 "PrepareFilled" 
                 "TransitionReadyFilled" 
                 "MissionUnmet" 
                 "CommittedUnmet")]
      (for [x (range n)] 
                (mapv (fn [t] [t x (+ l (rand-int (- r l)))])
                      trends))))
(defn walk-trends [xyt] 
  (let [svec  (mapv (fn [[nm ^XYSeries xys]] [nm (.getItems xys)])
                    (series-seq xyt))
        bound (count (second (first svec)))]
    (for [i (range bound)]
      (mapv (fn [[k ^XYSeries data]]           
              (let [^XYDataItem xy (nth data i)]
                [k (.getX xy) (or (.getY xy) 0.0)]))
            svec))))     
                  
;;we need to clear out the xydt and fill a new one...
(defn empty-clone [xyd]
  (let [^DefaultTableXYDataset tab (:table xyd)
        new-table                  (DefaultTableXYDataset.)
        sm (atom {})
        _ (doseq [[nm ^XYSeries series] (series-seq xyd)]
            (let [cnt (unchecked-dec (.getItemCount series))
                  ser ;(XYSeries. (.getKey series) true false)

                      (doto (.createCopy series 0 cnt) (.clear))]
              (do   (do! (.addSeries new-table ser))
                    (swap! sm assoc (.getKey ser) ser))))]
    (->xydataset new-table
                 (:seriesmap @sm)
                 (:order xyd))))

;; (defn zero-trends [xyd]
;;   (let [ks (map first (series-seq xyd))]
;;     (add-samples xyd
;;                  [(mapv (fn [k] [k 0.0 0.0]) ks)])))
      

;;animate the chart fill...
;;Basically, just pump data to a handler that updates the chart over
;;time.  The handler processes events by sampling (crawling the
;;samples basically), collating them, and updating the dataset, 
;;and calling a draw call on the chart. 
;;Assumes samples are ordered by timefunc.
(defn animated-data  [interval xydt]
  (let [unfilled  (empty-clone xydt)
        full-chrt (as-chart xydt {})
;        [min max] (get-bounds xydt)                
        chrt      (as-chart unfilled {})
;        _         (set-domain! chrt min max)
        _         (copy-axes! full-chrt chrt)
        dat       (as-tablexy chrt)
        _         (view chrt)]
    (future 
      (doseq [xs (walk-trends xydt)]
        (do  (add-samples dat [xs])
             (Thread/sleep interval))))    
    chrt))


;; (def number-stream (->> (obs/make-observable)
;;                         (obs/sequential-obs)))

;; (def number-spewer (agent {:state :spew}))
;; (defn spew-numbers [{:keys [state]}]
;;   (case state
;;     :spew  (do (obs/notify! number-stream (rand-int 100))
;;              (send *agent* spew-numbers)
;;              (Thread/sleep 100)
;;              {:state state})
;;     :stop-spewing  
;;           (pprint "Agent stopped spewing!")))

;; (defn sample-numbers [n ]
;;   (let [res  (obs/take-observations n number-stream)]
;;     (do (send number-spewer spew-numbers)
;;         (deref res)
;;         (send number-spewer 
;;           (fn [s] (assoc s :state :stop-spewing)))
;;         @res)))

  


;;in order for us to consistently render, we need to tie the color to
;;the data.
;;Since jfreecharts operate on datasets, which have number-labeled
;;series, we can apply the 

;; (defprotocol IReactivePlot
;;   (get-data    [plt])
;;   (data-series [plt])
;;   (get-series  [plt series])
;;   (add-data    [plt series x y]))




;;we can use proxies to extend the tabledataset class...
;;this will allow us to wrap the xydataset in some clojure interfaces 
;;like map and seq....good stuff...

  ;; clojure.lang.ILookup
  ;; ; valAt gives (get pm key) and (get pm key not-found) behavior
  ;; (valAt [this k] )
  ;; (valAt [this k not-found] )  
  ;; clojure.lang.IPersistentMap
  ;; (count [this] )
  ;; (assoc [this k v] )     ;;revisit          
  ;; (empty [this] )  
  ;; ;cons defines conj behavior
  ;; (cons [this e]   )
  ;; (equiv [this o]  )  
  ;; (hashCode [this] )
  ;; (equals [this o] )  
  ;; ;containsKey implements (contains? pm k) behavior
  ;; (containsKey [this k] )
  ;; (entryAt [this k]     )
  ;; (seq [this])  
  ;; ;without implements (dissoc pm k) behavior
  ;; (without [this k])    
  ;; clojure.lang.Indexed
  ;; (nth [this i])
  ;; (nth [this i not-found])


;;creates a jfreechart xyseries proxy that allows us to do idiomatic 
;;clojure interop
;; (defn ->xy-series  []
;;     (proxy [XYSeries clojure.lang.ILookup clojure.lang.IPersistentMap] []       
;;       ;valAt gives (get pm key) and (get pm key not-found) behavior
;;       (valAt [this k]  )
;;       (valAt [this k not-found] )   
;;       (count [this] )
;;       (assoc [this k v] )     ;;revisit          
;;       (empty [this] )  
;;       ;cons defines conj behavior
;;       (cons [this e]   )
;;       (equiv [this o]  )  
;;       (hashCode [this] )
;;       (equals [this o] )  
;;       ;containsKey implements (contains? pm k) behavior
;;       (containsKey [this k] )
;;       (entryAt [this k]     )
;;       (seq [this])  
;;       ;without implements (dissoc pm k) behavior
;;       (without [this k])    
;;       clojure.lang.Indexed
;;       (nth [this i])
;;       (nth [this i not-found])))

;; (defn ->xy-dataset []
;;   (proxy [DefaultTableXYDataset clojure.lang.ILookup clojure.lang.IPersistentMap]        
;;       ;valAt gives (get pm key) and (get pm key not-found) behavior
;;       (valAt [this k] )
;;       (valAt [this k not-found] )   
;;       (count [this] )
;;       (assoc [this k v] )     ;;revisit          
;;       (empty [this] )  
;;       ;cons defines conj behavior
;;       (cons [this e]   )
;;       (equiv [this o]  )  
;;       (hashCode [this] )
;;       (equals [this o] )  
;;       ;containsKey implements (contains? pm k) behavior
;;       (containsKey [this k] )
;;       (entryAt [this k]     )
;;       (seq [this])  
;;       ;without implements (dissoc pm k) behavior
;;       (without [this k])    
;;       clojure.lang.Indexed
;;       (nth [this i])
;;       (nth [this i not-found])))

  
;; (defrecord interactive-plot [chrt data series->idx series-order]
;;   IReactivePlot
;;   (get-data    [plt]             (.getDataset (.getXYPlot chrt)))
;;   (data-series [plt]             (.getSeries  (.get-data plt)))
;;   (add-data    [plt series x y]  (.add (data-series plt) x y)))

;; (extend-type org.jfree.chart.JFreeChart
;;   IReactivePlot
;;   (get-data    [plt]             (.getDataset (.getXYPlot chrt)))
;;   (data-series [plt]             (.getSeries  (.get-data plt)))
;;   (add-data    [plt series x y]  (.add (data-series plt) x y)))

;;This is really a map of x->y, the way we're implementing it....
;;It corresponds to what jfree refers to as a table dataset, where 
;;every series shares the same x coords.
(defn ->xy-series [nm]
  (proxy [XYSeries 
          clojure.lang.ILookup 
          clojure.lang.IPersistentMap
          clojure.lang.IMapEntry] 
      [nm]    
       
;   clojure.lang.ILookup
   ; valAt gives (get pm key) and (get pm key not-found) behavior
   (valAt ([k] (.getDataItem ^XYSeries this k))
          ([k not-found] (if-let [res (.getDataItem ^XYSeries this k)]
                               res 
                               not-found)))
;   clojure.lang.IPersistentMap
  (count [] (.getItemCount ^XYSeries this))
  (assoc [ k v]  (doto ^XYSeries this (.add (double k) (double v))))     ;;revisit          
  (empty [] nil)  
   ;cons defines conj behavior
  (cons [ e]   (doto ^XYSeries this (.add (double (first e)) (double (second e)))))
  (equiv [ o]  (.equals ^XYSeries this o))
  (hashCode [] (.hashCode ^XYSeries this))
  (equals [ o] (.equals ^XYSeries this o))
  ;; ;containsKey implements (contains? pm k) behavior
  (containsKey [ k] (pos? (.indexOf ^XYSeries this  (double k))))
  (entryAt [ k]  (let [idx (.indexOf ^XYSeries this (double k))]
                       (.getDataItem ^XYSeries this idx)
                       nil))
  (seq [] (map (fn [^XYDataItem xy] 
                 (clojure.lang.MapEntry. (.getX xy) (.getY xy))) 
               (iterator-seq (.iterator ^java.util.List (.getItems ^XYSeries this)))))
  ;; ;without implements (dissoc pm k) behavior
  (without [ k] (doto ^XYSeries this (.remove (int k))))
  ;; clojure.lang.Indexed
  (nth ([ i] (.get ^java.util.List (.getItems ^XYSeries this) i))
       ([ i not-found] (.get ^java.util.List (.getItems ^XYSeries this) i)))
  (key [] (.getKey ^Series this))
  (val [] (seq  this))
  ))

(comment 
(def xy (into (->xy-series "Howdy!") (map vector (range 10) (map #(* 2 %) (range 10)))))
)

;;Another option is to just hook up a listener to the xy dataseries...

;;All we need for an interactive plot is to have a set of data coming
;;in, and subscribers to said data. 
;;As data changes, the subscribers perform some action. 

;;If we have absolute names for trends, for instance, then as data 
;;is added to a trend, chart-subscribers that care about the trends 
;;can modify themselves correspondingly...


;;We'll use this pub/sub model to do some cool stuff.

;;We can define a protocol that covers data series....
;;

;;If we have more than one series, it's a data table...
;;In stacked area charts, the series data are all synchronized, 
;;that is, they share the same x axis.


