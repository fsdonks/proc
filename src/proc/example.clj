;;This is a sample of stuff we can do using the post processor.
(ns proc.example
  (:require [proc.stacked :as stacked]
            [proc.util :as util]
            [proc.interests :as ints])
  (:use [proc.core]
        [incanter.charts]
        [incanter.core]
        [proc.supply])) 

(def constants-look ["K:/Divisions/FS/dev/constants.clj" "V:/dev/constants.clj"])

(load-file (util/path! constants-look))

;;This dumps out our fills and sandtrends for the interesting srcs.
(defn run-sample!   "Call with eachsrc true if you want the fills for each SRC individually, or if you want to group the fills by interest,
call with ints set to a symbol from proc.interests.
call with :byDemandType? true if you want fills files by demandtype instead of the default by SRC. NotUtilized and Unmet records 
will always be found in a fills file associated with the respective SRC or interest.  If you don't see a fills file for an SRC, is it
in your interests?"
  [path & {:keys [interests subints eachsrc byDemandType?] :or {interests ints/defaults byDemandType? true}}]
  (let [subs (if subints subints (keys interests))]
    (binding [proc.core/*byDemandType?* byDemandType?
              proc.core/*run-path* path] 
      (if eachsrc
        (binding [*last-split-key* (if byDemandType? :DemandType :SRC)]
          (sandtrends-from path))
        (only-by-interest subs interests (sandtrends-from path))))))

;Can do trac-like charts by demand group by doing (binding [proc.stacked/*by-demandgroup?* true] 
;                                                        (do-charts-from root :interests blah))

(def marathons (atom {})) ;;Stores history of all marathon runs where key is title and val is the chart objs vector
(def chart-info {:title 0 :dwell 1 :fill 2}) ;; used to pull specific peices out of chart vector

(defn all-chart-types [key & col] ;;given a key and vector of components, returns the compentent specified by key
  (let [c (if col (first col) (vals @marathons))] ;;default collection in global history of marathon runs
    (map #(nth % (get chart-info key)) c)))

(defn get-chart-title [chart] ;; returns the title of the chart in form [run]-[interest]
  (let [htmlstr (.getText (first (all-chart-types :title [chart])))
        s clojure.string/split
        ttl (fn [key] (cond (= :run key) (first (s (last (s htmlstr #"Run: ")) #"<br>"))
                            (= :interest key) (first (s (last (s htmlstr #"Interest: ")) #"</b>"))))]
    (str (ttl :run) "-" (ttl :interest))))

(defn sync-chart-scales [coll axis] ;;syncs the axies of the given collection of charts
  (if (= axis :all)
    (do (proc.util/sync-scales coll :axis :x-axis)
        (proc.util/sync-scales coll :axis :y-axis))
    (proc.util/sync-scales coll :axis axis)) nil)

(defn show-chart [chart] ;;makes the chart visible given the collection on components
  (let [frame (show-stack chart)
        title (get-chart-title chart)]
    (.setTitle frame title)))
    ;;(.setIconImage frame (spork.graphics2d.image/read-buffered-image "C:\\Users\\michael.pavlak\\Desktop\\icon.JPG"))))

    

;; saves the jfree chart given the chart vector of chart objs, the title of the chart, and key (:dwell or :fill)
(defn save-jfree-chart [chart title key & {:keys [width height full-title] :or {width 700 height 420 full-title true}}] 
  (let [t (fn [ntitle] (.setTitle (first (all-chart-types key [chart])) ntitle))] 
    (when full-title ;; When full-title, change the title of the dwell or fill chart to include run + intereset
      (let [ftitle (get-chart-title chart)]
        (when (= key (keyword "fill")) (t (str ftitle " - Fill")))
        (when (= key (keyword "dwell")) (t (str ftitle " - Dwell")))))
    (org.jfree.chart.ChartUtilities/saveChartAsPNG
     (java.io.File. title) (first (all-chart-types key [chart])) width height)
    (when (= key (keyword "fill")) (t "Fill")) ;; Change title of chart back to short title 
    (when (= key (keyword "dwell")) (t "Dwell"))))
   
(defn get-theme [font-name] ;; Changes font to larger size of default ChartFactory theme
  (let [theme (org.jfree.chart.ChartFactory/getChartTheme)] 
    (doto theme
      (.setExtraLargeFont (java.awt.Font. font-name (java.awt.Font/BOLD) 26))
      (.setLargeFont (java.awt.Font. font-name (java.awt.Font/BOLD) 22))
      (.setRegularFont (java.awt.Font. font-name (java.awt.Font/PLAIN) 14))
      (.setSmallFont (java.awt.Font. font-name (java.awt.Font/PLAIN) 12))
      )theme))


;Do-charts-from creates stacked dwell and fill charts for each interest using AUDIT_deployments.txt and
;the fills files produced from proc.fillsfils/run-sample!.
;You need to call run-sample! in order to make a folder for fills files within the Marathon run folder before calling do-charts-from
(defn do-charts-from ;do we have too many arguments here?
  "Pass in your own interests if you'd like.  See examples of interests in proc.interests
:group-key defaults to :DemandType for dwell-before-deployment plot.  Can set :group-key to :UnitType as well.
Call with :sync false in order to not sync the x and y axis across these charts.  :sync defaults to true.
Call with :phases set to a sequence of phases defined in the run in order to see separate charts for each phase.  Will throw
a null pointer exception if one of your phases doesn't exist in the run.
Call with :fillbnds {:fxlow val0 :fxhigh val1 :fylow val2 :fyhigh val3} and/or 
:dwellbnds {:dxlow val4 :dxhigh val5 :dylow val6 :dyhigh val7} to set the bounds for axes."
  [root &
   {:keys [subs phases interests subints group-key syncys phase-lines fillbnds dwellbnds vis save-fill save-dwell]
    :or {subs false phases [nil] syncys false interests ints/defaults group-key :DemandType phase-lines true vis true
         save-fill false save-dwell false fillbnds {:fxlow 0 :fylow 0} dwellbnds {:dxlow 0}}}]
  (org.jfree.chart.ChartFactory/setChartTheme (get-theme "Calibri"))
  (let [ints (if subints subints (keys interests))
        charts (only-by-interest ints interests
                                 (remove nil? (for [int ints phase phases] ;remove nil printlns
                                                (binding [dep-group-key group-key] 
                                                  (dwell-over-fill root (get interests int) subs phase)))))
        dwells (remove #(nil? (.getDataset (.getPlot %))) (all-chart-types :dwell charts))
        fills  (all-chart-types :fill charts)
        phstarts (phase-starts root)

        {:keys [fxlow fxhigh fylow fyhigh]} fillbnds
        {:keys [dxlow dxhigh dylow dyhigh]} dwellbnds
        chart-bounds (fn [coll]
                       (doseq [chart coll :let [sb (fn [axis l h] (proc.util/set-bounds chart axis :lower l :upper h))]]
                         (sb :x-axis dxlow dxhigh) (sb :y-axis dylow dyhigh)))]
    
    (doseq [chart charts] (swap! marathons assoc (get-chart-title chart) chart)) ;;adds new charts to global history
    
    (doseq [k [:dwell :fill]] ;; sets chart boundaries and syncs scales  
      (chart-bounds (all-chart-types k))
      (let [a (if syncys :all :x-axis)]
        (sync-chart-scales (all-chart-types k) a)))
    (sync-chart-scales (concat dwells fills) :x-axis)
    
    (doseq [dwell dwells] (add-trend-lines! dwell :bnds (:x (xy-bounds dwell)))) ;;adds trend lines
    (when phase-lines ;; adds phase-lines
      (doseq [fill (all-chart-types :fill)
              :let [b (:y-axis (util/state fill)) l (fn [] (.getLowerBound b)) u (fn [] (.getUpperBound b))]]
        (add-phase-lines phstarts (l) (u) fill)))

    ;;makes charts visable, default true
    (when vis (doseq [chart charts] (show-chart chart)))

    (when (or save-dwell save-fill) ;;saves dwell/fill charts 
      (doseq [chart charts :let [title (get-chart-title chart)]]
        (when save-fill (save-jfree-chart chart (str root title "-fill.png") :fill))
        (when save-dwell (save-jfree-chart chart (str root title "-dwell.png") :dwell))))))

(defn do-multiple-charts-from ;; calls do-charts-from for each root in roots using the same options for each
 [roots &
  {:keys [subs phases interests subints group-key syncys phase-lines fillbnds dwellbnds vis save-fill save-dwell]
   :or {subs false phases [nil] syncys false interests ints/defaults group-key :DemandType phase-lines true vis true
        save-fill false save-dwell false fillbnds {:fxlow 0 :fylow 0} dwellbnds {:dxlow 0}}}]
  (doseq [root roots]
   (do-charts-from root
                   :subs subs :phases phases :interests interests :subints subints :group-key group-key
                   :syncys syncys :phase-lines phase-lines :fillbnds fillbnds :dwellbnds dwellbnds
                   :vis vis :save-fill save-fill :save-dwell save-dwell)))

;; saves all marathons that are in the global history to the specified director with optional widths and heights
(defn save-all-marathons [dir & {:keys [width height] :or {width 700 height 420}}]
 (let [charts (vals @marathons)]
  (doseq [chart charts :let [title (get-chart-title chart)]]
   (doseq [k ["fill" "dwell"]] (save-jfree-chart chart (str dir title k ".png") (keyword k) :width width :height height)))))
 
 



;; dwells (->> (map (fn [[pane dwell fill]] dwell) charts)
;;             (remove (fn [chart] (nil? (.getDataset (.getPlot chart))))));had to remove no deployment data charts
;;fills (map (fn [[pane dwell fill]] fill) charts)
;;(proc.util/set-bounds chart :x-axis :lower dxlow :upper dxhigh)
;;(proc.util/set-bounds chart :y-axis :lower dylow :upper dyhigh)))
;;(sync-chart-scales (concat dwells fills) :x-axis)
;;(when syncys (do (sync-chart-scales dwells :y-axis) (sync-chart-scales fills :y-axis)))    
;; (when phase-lines
;;   (let [fills (map #(nth % 2) (vals @marathons))]
;;     (doseq [fill fills] (add-phase-lines phstarts
;;                                          (.getLowerBound (:y-axis (util/state fill)))
;;                                          (.getUpperBound (:y-axis (util/state fill))) fill))))
;; _ (doseq [fill fills] (Image.IO/write fill "jpg" "v:/"))
;;(proc.util/sync-scales (concat dwells fills) :axis :x-axis)
;;(when syncys (do
;;               (proc.util/sync-scales dwells :axis :y-axis)
;;               (proc.util/sync-scales fills :axis :y-axis)))
;; _ (doseq [chart dwells] (do (proc.util/set-bounds chart :x-axis :lower dxlow :upper dxhigh)
;;                             (proc.util/set-bounds chart :y-axis :lower dylow :upper dyhigh)))
;; _ (doseq [chart fills] (do (proc.util/set-bounds chart :x-axis :lower fxlow :upper fxhigh)
;;                            (proc.util/set-bounds chart :y-axis :lower fylow :upper fyhigh)))
;; _ (proc.util/sync-scales (concat dwells fills) :axis :x-axis)   ;added for taa. want all charts same x
;; _ (when syncys (do  ;it might be useful if our y-axes all match, too
;;                  (proc.util/sync-scales dwells :axis :y-axis)
;;                  (proc.util/sync-scales fills :axis :y-axis)))
;; _  (doseq [dwell dwells] (add-trend-lines! dwell :bnds (:x (xy-bounds dwell)))) ;need to add trend lines after syncing charts

;;  (when :save-fills 
;;    (org.jfree.chart.ChartUtilities/saveChartAsPNG (File. (str title "-fill.png")) fill 800 800))
;;  (when :save-dwells
;;    (org.jfree.chart.ChartUtilities/saveChartAsPNG (File. (str title "-dwell.png")) dwell 800 800))
;; (org.jfree.chart.ChartUtilities/saveChartAsPNG (File. "file.png" (last x) 100 100))






;;One function to make the files for the charts and display the charts at the same time
(defn charts-from-unproc-run
  ([path]
    (do (run-sample! path)
        (do-charts-from path))))

(defn view-deployments 
  ([int interests deploymentspath]
    (let [dwell-plot (deployment-plot deploymentspath (get interests int) nil)
          _ (add-trend-lines! dwell-plot :bnds (:x (xy-bounds dwell-plot)))]
      (view dwell-plot)))
  ([deploymentspath] (view-deployments :BCTS ints/cints19-23 deploymentspath)))    

; pipeline copied from sand-chart2
(defn unit-sand-from [path interest] ;oops.  Need to do daily samples here
  (let [ds (util/as-dataset (str path "sand/" interest ".txt"))]
    (-> (->> (-> (util/as-dataset ds)
           (stacked/roll-sand :cat-function stacked/parse-arfor-fill)
           (stacked/expand-samples)) ; this smooths out the picture but what is it doing?
      (stacked/xy-table :start :quantity :group-by :Category :data))
      (stacked/stacked-areaxy-chart2* :legend true :color-by stacked/arforgen-color-2 :order-by stacked/arforgen-order-2)
      (view))))

(defn lines-demo 
  "We can make lines instead of stacked area charts, but need to uncompress x labels, and use fewer groups..."
  [fpath]
  (let [dset (stacked/roll-sand (util/as-dataset fpath) 
                                   :cols [:FillType :DemandType] :cat-function stacked/suff-cat-fill-subs )]
    (view (line-chart :start :quantity :group-by :Category :legend true :data dset))))


;given a parent directory, return us all paths of the run folders.... identify
;a run folder by some marathon file
(defn run-names-from [rootsloc]) 


  

 






