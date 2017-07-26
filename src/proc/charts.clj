(ns proc.charts
  (:require [proc.stacked :as stacked]
            [proc.util :as util]
            [proc.interests :as ints])
  (:use [proc.core]
        [incanter.charts]
        [incanter.core]
        [proc.supply]))

(def chart-info {:title 0 :dwell 1 :fill 2})

(defn all-chart-types
  "Takes a key of either :dwell :fill or :title and returns the chart type from vector of chart components"
  [key coll]
  (map #(nth % (get chart-info key)) coll))

(defn get-chart-title
  "Given a chart vector, returns the title of the chart"
  [chart]
  (let [htmlstr (.getText (first (all-chart-types :title [chart])))
        s clojure.string/split
        ttl (fn [key] (cond
                        (= :run key) (first (s (last (s htmlstr #"Run: ")) #"<br>"))
                        (= :interest key) (first (s (last (s htmlstr #"Interest: ")) #"<"))))]
    (str (ttl :run) "-" (ttl :interest))))

(defn get-chart-interest
  "Given a chart vector, returns the interest of the chart"
  [chart]
  (let [htmlstr (.getText (first (all-chart-types :title [chart])))
        s clojure.string/split]
    (first (s (last (s htmlstr #"Interest: ")) #"</b>"))))

(defn inv-to-nums
  "Converts an inventory string to numbers"
  [inv]
  (for [i (filter #(not= "" %) (clojure.string/split inv #"[\\d]*/"))]
    (read-string i)))

(defn add-all-invs
  "Given a collection of inventory strings, adds inventories together and returns sum as a inventroy string"
  [invs]
  (let [p (apply map + (for [i invs] (inv-to-nums i)))]
    (str (first p) "/" (nth p 1) "/" (nth p 2) "//" (last p))))

(defn get-total-inventory
  "returns the total inventory as a string given the chart, interest, and root dir"
  [chart interest root]
  (let [chart-int (keyword (get-chart-interest chart))
        srcs (last (get interest chart-int))
        supply (proc.supply/supply-by-compo root)
        inv (filter #(not (not %)) (map #(get supply %) srcs))]
    (add-all-invs inv)))

(defn add-inventory-to-chart
  "Adds the inventory information to chart"
  [chart interest root]
  (let [htmlstr (.getText (first (all-chart-types :title [chart])))
        info (first (clojure.string/split (last (clojure.string/split htmlstr #"<b>")) #"</b>"))
        inv (get-total-inventory chart interest root)
        new-str (str "<html><center><b><font size=6>"
                     info "<br>Inventory: " inv "</font></b></center></html>")]
    (.setText (first (all-chart-types :title [chart])) new-str)))

(defn sync-chart-scales
  "Given a collection of charts, takes keys :x-axis, :y-axis, or all, and syncs the given axis on all charts in coll"
  [coll axis] ;;syncs the axies of the given collection of charts
  (if (= axis :all)
    (do (proc.util/sync-scales coll :axis :x-axis)
        (proc.util/sync-scales coll :axis :y-axis))
    (proc.util/sync-scales coll :axis axis)) nil)

(defn show-chart
  "Sets chart as visible"
  [chart] ;;makes the chart visible given the collection on components
  (let [frame (show-stack chart)
        title (get-chart-title chart)]
    (.setTitle frame title)))

;; saves the jfree chart given the chart vector of chart objs, the title of the chart, and key (:dwell or :fill)
(defn save-jfree-chart
  "Saves chart as image"
  [chart title key & {:keys [width height full-title] :or {width 700 height 420 full-title true}}] 
  (let [t (fn [ntitle] (.setTitle (first (all-chart-types key [chart])) ntitle))] 
    (when full-title ;; When full-title, change the title of the dwell or fill chart to include run + intereset
      (let [ftitle (get-chart-title chart)]
        (when (= key (keyword "fill")) (t (str ftitle " - Fill")))
        (when (= key (keyword "dwell")) (t (str ftitle " - Dwell")))))
    (org.jfree.chart.ChartUtilities/saveChartAsPNG
     (java.io.File. title) (first (all-chart-types key [chart])) width height)
    (when (= key (keyword "fill")) (t "Fill")) ;; Change title of chart back to short title 
    (when (= key (keyword "dwell")) (t "Dwell"))))

(defn get-theme
  "Changes default ChartFactory theme to have larger font size and to use supplied font from font name"
  [font-name] ;; Changes font to larger size of default ChartFactory theme
  (let [theme (org.jfree.chart.ChartFactory/getChartTheme)] 
    (doto theme
      (.setExtraLargeFont (java.awt.Font. font-name (java.awt.Font/BOLD) 26))
      (.setLargeFont (java.awt.Font. font-name (java.awt.Font/BOLD) 22))
      (.setRegularFont (java.awt.Font. font-name (java.awt.Font/PLAIN) 14))
      (.setSmallFont (java.awt.Font. font-name (java.awt.Font/PLAIN) 12))
      )theme))

;; ===== HELPER FUNCTIONS FOR BUILDING CHARTS/DO-CHARTS-FROM ======================

(defn root->charts
  "Creates charts given root, interest, phases, group-key, subs and subints."
  [root interests phases group-key subs subints]
  (org.jfree.chart.ChartFactory/setChartTheme (get-theme "Calibri"))
  (let [ints (if subints subints (keys interests))]
    (only-by-interest ints interests
                      (remove nil? (for [int ints phase phases]
                                     (binding [dep-group-key group-key]
                                       (dwell-over-fill root (get interests int) subs phase)))))))

(defn charts->dwells
  "Given a vector of chart objects, returns the dwells plot"
  [charts]
  (remove #(nil? (.getDataset (.getPlot %))) (all-chart-types :dwell charts)))

(defn charts->fills
  "Given a vector of chart objects, returns the fill plot"
  [charts]
  (all-chart-types :fill charts))

(defn ->set-bounds
  "Sets the bounds on all charts in charts to supplied bounds. Returns nil"
  [charts xlow xhigh ylow yhigh]
  (doseq [chart charts :let [sb (fn [axis l h] (proc.util/set-bounds chart axis :lower l :upper h))]]
    (sb :x-axis xlow xhigh) (sb :y-axis ylow yhigh)))

(defn ->sync
  "Syncronizes axis/axies on all charts and sets bounds. Returns nil" 
  [charts syncys xlow xhigh ylow yhigh]
  (doseq [k [:dwell :fill]]
    (->set-bounds (all-chart-types k charts) xlow xhigh ylow yhigh)
    (let [a (if syncys :all :x-axis)]
      (sync-chart-scales (all-chart-types k charts) a)))
  (sync-chart-scales (concat (all-chart-types :dwell charts) (all-chart-types :fill charts)) :x-axis))

(defn ->lines
  "Draws/redraws phase lines for all charts in chart given phstarts. Returns nil"
  [charts phase-lines phstarts]
  (doseq [dwell (all-chart-types :dwell charts)]
    (add-trend-lines! dwell :bnds (:x (xy-bounds dwell))))
  (when phase-lines
    (doseq [fill (all-chart-types :fill charts) :let [b (:y-axis (util/state fill))]]
      (add-phase-lines phstarts (.getLowerBound b) (.getUpperBound b) fill))))

(defn ->save-dwell-fill
  "Given root, charts, and booleans for save-dwell/save-fill, saves charts as images. Returns nil"
  [root charts save-dwell save-fill]
  (when (or save-dwell save-fill) 
    (doseq [chart charts :let [title (get-chart-title chart)]]
      ;;(add-inventory-to-chart chart interest root)
      (when save-fill (save-jfree-chart chart (str root title "-fill.png") :fill))
      (when save-dwell (save-jfree-chart chart (str root title "-dwell.png") :dwell)))))

(defn ->vis
  "Sets charts to visible. Returns nil"
  [charts root interests vis]
  (when vis
    (doseq [chart charts]
      ;;(add-inventory-to-chart chart interests root)
      (show-chart chart))))
  
