;;A port of the patch-chart/laydown visualization from squirm. 
;;We need to establish a portable format for generating these dudes.
(ns proc.patches
  (:require [spork.sketch :as sketch]
            [spork.geometry.shapes :as s]
            [spork.graphics2d.canvas :as canv]
            [clojure.core.reducers :as r]))
            
(def ^:dynamic *attributes*   {"Committed" {:SurgeType "Committed"}
                               "FCCommitted"  {:SurgeType "Committed"}
                               "FCMission" {:SurgeType "Mission"}
                               "Mission"   {:SurgeType "Mission"}
                               "Demand1"          {:SurgeType "Committed"}
                               "Demand2"          {:SurgeType "Mission"}
                               "Ready_Deployable" {:SurgeType "Ready"}})

(def ^:constant +chunk-width+  150)
(def ^:constant +chunk-height+ 14)
(def ^:dynamic *chunk-txt*)

;; (canv/color-by (canv/gradient-right l r)
;;                (s/->rectangle :white 0 0 +chunk-width+ +chunk-height+)))    

;; (defn ->transition [l r]        
;;   (canv/color-by (canv/gradient-right l r)
;;                  (s/->rectangle :white 0 0 +chunk-width+ +chunk-height+)))


;; (defn ->added-label  [txt label-color color x y w h]
;;   (let [r          (s/->rectangle  color x y w h)
;;         half-dur   (/ w 2.0)
;;         centerx    (+ x 1)
;;         scalex     1.0 
;;         centery    0]
;;      (sketch/scale scalex 1.0 (sketch/uncartesian 
;;                                (sketch/->label txt centerx centery :color label-color)))))


;; (defn ->transition 
;;   ([l r] (->transition l r "Transition"))
;;   ([l r lbl]
;;      (canv/color-by (canv/gradient-right l r)
;;                     (sketch/->labeled-box lbl :black :white 0 0 +cunk-width+ +chunk-height+))))

(defn ->transition 
  ([l r]      (canv/color-by (canv/gradient-right l r)
                             (s/->rectangle :white 0 0 +chunk-width+ +chunk-height+)))
  ([l r lbl]
     (canv/color-by (canv/gradient-right l r)
                    (sketch/->labeled-box lbl :black :white 0 0 +chunk-width+ +chunk-height+))))

(defn transition-type [^String loc]
  (if (.contains loc "Transition")
      (clojure.string/replace loc "Transition" "")))

(defn loc->color 
  ([loc location->attributes]
     (if (vector? loc)  (or (loc->color (first loc)) (loc->color (second loc)))
         (case loc
           "Label"   :grey
           "Ready"   :green
           "Prepare" :yellow
           "TransitionReady" [:light-sky-blue :green]
           "TransitionReady_NotDeployable" [:light-sky-blue :green]
           "TransitionPrepare" [:orange :yellow]
           "Mission_NotDeployable" :light-sky-blue
           "Mission_Deployable" :light-sky-blue
           (if (.contains ^String loc "Transition")
             :transition
             (case     (:SurgeType (location->attributes loc))
               "Committed" :orange
               "Mission"    :light-sky-blue
               "Mission_Deployable" :light-sky-blue
               "Ready"     :green
               "Prepare"   :yellow        
               (throw (Exception. (str "unknown mission type: " loc))))))))
  ([loc] (loc->color loc #(get *attributes* %))))
       
(defn id-transitions [xs]
  (loop [acc  []
         prev nil
         xs   xs]
    (if (empty? xs) acc
        (let [x (first xs)]
          (case x 
            ;we hit a transition
            :transition 
            (let [to (fnext xs)]
              (recur 
               (conj acc [prev to])
               
               to
               (rest xs)))
            (recur (conj acc x)
                   x
                   (rest xs)))))))

(defn clean-transitions [xs]
  (into []
        (r/map (fn [x]
                 (if (vector? x)
                   (let [[l r] x]
                     (if (or (or (nil? l) (nil? r))
                             (or (identical? l :transition)
                                 (identical? r :transition)))
                       (case [l r]
                         [nil :light-sky-blue]           [:green  :light-sky-blue]
                         [nil :orange]         [:green  :orange]
                         [nil :yellow]         [:orange :yellow]
                         [nil :green]          [:light-sky-blue   :green]
                         [nil :transition]     [:light-sky-blue :green]
                         [:light-sky-blue   nil]         [:light-sky-blue   :green]
                         [:orange nil]         [:orange :yellow]
                         [:green  nil]         [:green  :light-sky-blue]
                         [:yellow nil]         [:yellow :green]
                         [:transition nil]     [:light-sky-blue :green]
                         [:transition :light-sky-blue]   [:green  :light-sky-blue]
                         [:transition :orange] [:green  :orange]
                         [:transition :yellow] [:orange :yellow]
                         [:transition :green]  [:light-sky-blue   :green]
                         [:light-sky-blue   :transition] [:light-sky-blue   :green]
                         [:orange :transition] [:orange :yellow]
                         [:green  :transition] [:green  :light-sky-blue]
                         
                         ;(throw (Exception. (str x)))
                         (do ;(println (str "unknown color type:" x))
                             :grey)
                         )
                       (if (= l r) l
                           x)))
                     x))
               xs)))

 
(defn table->colors [rowcols]
  (->> rowcols
       (r/map #(clean-transitions (id-transitions (mapv loc->color %))))
       (into [])))

(defn ->chunk 
  ([color lbl]
     (if (vector? color)
       (->transition (first color) (second color))
       
                                        ;    (s/->rectangle coloro 0 0 +chunk-width+ +chunk-height+)
       (sketch/->labeled-box lbl :black color 0 0 +chunk-width+ +chunk-height+)))
  ([color] (->chunk color "")))

(def colors 
  [:green
   :light-sky-blue
   :orange 
   :yellow
  [:green  :light-sky-blue]
  [:green  :orange]
  [:green  :yellow]
  [:orange :yellow]
  [:light-sky-blue   :green]
  [:light-sky-blue   :green]
  [:light-sky-blue   :green]
  [:orange :yellow]
  [:green  :light-sky-blue]
  [:yellow :green]
  [:light-sky-blue   :green]
  [:green  :light-sky-blue]
  [:green  :orange]
  [:orange :yellow]
  [:light-sky-blue   :green]
  [:light-sky-blue   :green]
  [:light-sky-blue   :yellow]
  [:yellow :light-sky-blue]
  [:orange :yellow]
  [:green  :light-sky-blue]
  [:yellow :orange]])

(def color-set (set colors))
  
(def chunks 
  (zipmap colors 
          (map (comp sketch/outline ->chunk) colors))) 

;; (defn as-chunk 
;;   [x]
;;      (if-let [v (get chunks x)]
;;        v
;;        (do (println (str "Unknown chunk!" x))
;;            (->chunk :red))))

(defn as-chunk 
  [x & [lbl]]
  (if (contains? color-set x)
    (sketch/outline (if lbl (->chunk x lbl) (->chunk x)))
    (do (println (str "Unknown chunk!" x))
        (->chunk :red))))

(def knownlocs #{"Prepare" "Ready" "Ready_Deployable" "TransitionReady_NotDeployable" "DeMobilization" "Mission_Deployable"})

(defn table->labels [rowcols]
  (for [row   rowcols]
    (vec (for [[l r] row]
           (cond (= l r) ""
                 (contains? knownlocs r) ""
                 :else r)))))

;; (defn sketch-history [rowcols]
;;   (let [tbl     (table->colors rowcols)
;;         labels  (table->labels rowcols)
;;         cells   (sketch/stack
;;                         (mapv (fn [xs] (sketch/shelf (mapv as-chunk xs))) tbl))
;;         width   (:width (.shape-bounds cells))]
;;     cells))

(defn sketch-history [rowcols]
  (let [tbl     (table->colors rowcols)
        labels  (table->labels rowcols)
        cells   (sketch/stack
                 (map-indexed (fn [i row]
                                (let [labs (nth labels i)]
                                  (sketch/shelf
                                   (map-indexed (fn [col x]
                                                  (as-chunk x (nth labs col))) row)))) tbl))
        width   (:width (.shape-bounds cells))]
    cells))

(defn strike-through 
  ([shp color]  
     (let [bounds  (spork.graphics2d.canvas/shape-bounds shp)
           x       (:x bounds)
           y       (+ (:y bounds) (/ (:height bounds) 2.0))
           ln      (spork.geometry.shapes/->line color
                                                 x
                                                 y
                                                 (+ x (dec (:width bounds))) y)]
       [shp 
        ln]))
  ([shp] (strike-through shp (apply canv/->color-rgba 
                                                        (spork.graphics2d.canvas/random-color)))))
                                             


