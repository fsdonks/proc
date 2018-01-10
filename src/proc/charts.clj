(ns proc.charts
  (:require [proc.stacked :as stacked]
            [proc.util :as util]
            [proc.interests :as ints]
            [proc.powerpoint :as ppt]
            [proc.clipboard :as clip]
            [spork.util.table :as tbl]
            [spork.util [io :refer [fname]]]
            [clojure.string :as str])
  (:use [proc.core]
        [incanter.charts]
        [incanter.core]
        [proc.supply])
  (:import [javax.swing JFrame]))


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

(defn set-chart-title [chart title]
  (.setTitle (first (all-chart-types :title [chart])) title))
  

(defn get-chart-interest-string
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
    (if (= '() invs) 
      (str "No inventory found") ;;when invs is empty
      (str (first p) "/" (nth p 1) "/" (nth p 2) "//" (last p)))))

(defn get-total-inventory
  "returns the total inventory as a string given the chart, interest, and root dir"
  [chart interests root]
  (let [chart-int (get-chart-interest-string chart)
        srcs (get (proc.interests/str-interests->srcs interests) chart-int)
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

(defn get-inventory-title 
  [chart interest root]
  (let [htmlstr (.getText (first (all-chart-types :title [chart])))
        info (first (clojure.string/split (last (clojure.string/split htmlstr #"<b>")) #"</b>"))
        inv (get-total-inventory chart interest root)
        new-str (str "<html><center><b><font size=6>"
                     info "<br>Inventory: " inv "</font></b></center></html>")]
    (str "Run: " (.trim (first (clojure.string/split (last (clojure.string/split new-str #"Run:")) #"<br>"))) "   "
      "Interest: " (.trim (first (clojure.string/split (last (clojure.string/split new-str #"Interest:")) #"<"))) "   "
      "Inventory: " (.trim (first (clojure.string/split (last (clojure.string/split new-str #"Inventory:")) #"<"))))))
  

(defn sync-chart-scales
  "Given a collection of charts, takes keys :x-axis, :y-axis, or all, and syncs the given axis on all charts in coll"
  [coll axis] ;;syncs the axies of the given collection of charts
  (if (= axis :all)
    (do (proc.util/sync-scales coll :axis :x-axis)
        (proc.util/sync-scales coll :axis :y-axis))
    (proc.util/sync-scales coll :axis axis)) nil)

;; saves the jfree chart given the chart vector of chart objs, the title of the chart, and key (:dwell or :fill)
(defn save-jfree-chart
  "Saves chart as image"
  [chart title key & {:keys [width height full-title root interests] :or {width 700 height 420 full-title true}}] 
  (let [t (fn [ntitle] (.setTitle (first (all-chart-types key [chart])) ntitle))] 
    (when full-title ;; When full-title, change the title of the dwell or fill chart to include run + intereset
      (let [ftitle (get-chart-title chart) new-ttl (get-inventory-title chart interests root)]
        (when (= key (keyword "fill")) (t new-ttl))
        (when (= key (keyword "dwell")) (t new-ttl))))
    (org.jfree.chart.ChartUtilities/saveChartAsPNG
     (java.io.File. title) (first (all-chart-types key [chart])) width height)
    (when (= key (keyword "fill")) (t "Fill")) ;; Change title of chart back to short title 
    (when (= key (keyword "dwell")) (t "Dwell"))))

(defn listen-for-keys [root chart title frame] 
  (.addKeyListener frame (proxy [java.awt.event.KeyListener] []
                           (keyPressed [e]
                             (when (and (.isControlDown e) (= java.awt.event.KeyEvent/VK_C (.getKeyCode e)))
                               ;;(println "KEY PRESSED")
                               ;;(println (str root title))
                               (save-jfree-chart chart (str root title "-fill.png") :fill)
                               (save-jfree-chart chart (str root title "-dwell.png") :dwell)
                               (clip/copy-file-to-clip
                                [(str root title "-fill.png") (str root title "-dwell.png")]))))))                             
                               ;;(println "KEY PRESSED"))))))

(defn show-chart
  "Sets chart as visible"
  [root chart] ;;makes the chart visible given the collection on components
  (let [frame (show-stack chart)
        title (get-chart-title chart)]
    (listen-for-keys root chart title frame)
    (listen-for-keys root chart title (first chart))
    (.setTitle frame title)))

(defn get-theme
  "Changes default ChartFactory theme to have larger font size and to use supplied font from font name"
  [font-name] ;; Changes font to larger size of default ChartFactory theme
  (let [theme (org.jfree.chart.ChartFactory/getChartTheme)] 
    (doto theme
      (.setExtraLargeFont (java.awt.Font. font-name (java.awt.Font/BOLD) 26))
      (.setLargeFont (java.awt.Font. font-name (java.awt.Font/BOLD) 22))
      (.setRegularFont (java.awt.Font. font-name (java.awt.Font/PLAIN) 14))
      (.setSmallFont (java.awt.Font. font-name (java.awt.Font/PLAIN) 12)))
    theme))

;; ===== HELPER FUNCTIONS FOR BUILDING CHARTS/DO-CHARTS-FROM ======================
(defn scope-to-deployments [root src-set]
  (let [path (str root "AUDIT_Deployments.txt")
        deps (tbl/tabdelimited->table path :schema util/deploy-schema)]
    (tbl/select  :from deps :where
                (fn [{:keys [UnitType DemandType]}]
                    (or (src-set UnitType)
                        (src-set DemandType))))))

(defn interests->src-map [ints]
  (reduce (fn [acc [k v]]
            (assoc acc k
              (conj (get acc k #{}) v)))
          {}
          (apply concat
                 (for [[nm intspec] ints]
                   (let [[lbl srcs] intspec]
                     (for [src srcs]
                       [src nm]))))))

(defn src-map->src-set [sm] (set (keys sm)))

(defn root->charts
  "Creates charts given root, interest, phases, group-key, subs and subints."
  [root interests phases group-key subs subints]
  (org.jfree.chart.ChartFactory/setChartTheme (get-theme "Calibri"))
  (let [ints (if subints subints (keys interests))
        src-map          (interests->src-map interests)
        src-set (src-map->src-set src-map)
        scoped-deployments (scope-to-deployments root
                                                 src-set)
        active-srcs (reduce (fn [acc {:keys [UnitType DemandType]}]
                              (conj acc UnitType DemandType))
                            #{} scoped-deployments)
        active-interests  (reduce clojure.set/union #{}
                                  (vals (select-keys src-map active-srcs)))]
     (doall (remove nil?
               (for [int ints phase phases]
                 (if (active-interests int)
                   (binding [dep-group-key group-key]
                     (dwell-over-fill root (get interests int)
                                      subs phase :deployments
                                      scoped-deployments))
                   (println [:ignoring int :for-charting :no-deployments!])))))))

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
  (doseq [chart charts
          :let [sb (fn [axis l h]
                     (proc.util/set-bounds chart axis :lower l :upper h))]]
                
    (sb :x-axis xlow xhigh)
    (sb :y-axis ylow yhigh)))
  
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
  [root charts interests save-dwell save-fill]
  (when (or save-dwell save-fill) 
    (doseq [chart charts :let [title (get-chart-title chart)]]
      (when save-fill (save-jfree-chart chart (str root title "-fill.png") :fill :root root :interests interests))
      (when save-dwell (save-jfree-chart chart (str root title "-dwell.png") :dwell :root root :interests interests)))))

(defn ->vis
  "Sets charts to visible. Returns nil"
  [charts root interests vis]
  (when vis
    (doseq [chart charts]
      (add-inventory-to-chart chart interests root)
      (show-chart root chart))))

(defn find-images
  "get a sequence of .png filepaths from each root"
  [roots]
  (apply concat (for [root roots] (map #(str root %) (ppt/find-images root)))))

(defn get-interest [png-filepath]
  (-> (str/replace  png-filepath #"-dwell.png|-fill.png" "")
      (str/split #"-")
      (last)))

(defn run-name [png-filepath]
  (let [int (get-interest png-filepath)]
    (first (str/split png-filepath (re-pattern (str "-" int "-"))))))
         
(defn sorted-images
  "returns a sorted sequence of image filepaths.  Sorted according to
  the order of roots and int-order"
  [int-order roots]
  (->> (find-images roots)
       (util/sorted-by-vals [#(get-interest (fname %)) #(run-name (fname %))] (concat int-order roots))))

(defn prep-images
  "get a sequence of image filepaths from multiple directories"
  [roots num-per-slide img-finder] 
  (let [images (img-finder roots)
        r (- (count images) (rem (count images) num-per-slide))]
    (if (zero? r) (partition num-per-slide images) (conj (partition num-per-slide images) (drop r images)))))

(defn charts->ppt
  "take images and put them in a powerpoint presentation"
  [roots filename template num-per-slide & {:keys [img-finder] :or {img-finder find-images}}]
  (let [pptx (ppt/->pptx template)]
    (doseq [ps (prep-images roots num-per-slide img-finder)]
      (ppt/slide-with-images pptx ps))
    (ppt/save-ppt pptx filename)))

(defn charts->ppt-with-layout [roots filename template layout & {:keys [img-finder] :or {img-finder find-images}}]
  (ppt/save-ppt (ppt/add-images-to-ppt (ppt/->pptx template) (img-finder roots) layout) filename))
 
;example
(comment
 (def t1 "C:\\Users\\craig.j.flewelling\\Desktop\\t\\testdata-v6\\")

 (def t2 "C:\\Users\\craig.j.flewelling\\Desktop\\t\\testdata-v6 - Copy\\")

 (def test-order ["QM" "CAB"])

 (def i-finder (partial sorted-images test-order))

 (charts->ppt  [t1 t2] "C:\\Users\\craig.j.flewelling\\Desktop\\t\\test.pptx" (str t1 "\\template.pptx") 4 :img-finder i-finder)

 (def ppt-settings {:filename (str t1 "dwell-over-fill.pptx") :template (str t1 "template.pptx") :layout (map #(get ppt/rectangles %) (map #(get ppt/positions %) ["Top Left" "Bottom Right"]))})

 (charts->ppt-with-layout [t1 t2] (:filename ppt-settings) (:template ppt-settings) (:layout ppt-settings) :img-finder i-finder)
 )
