;;Quick set of stuff designed to make aforementioned
;;risk plots and similar response surfaces
(ns proc.risk
  (:require
   [incanter [core :as i]
             [charts :as c]
             [stats :as s]]))

(def rgb-gradient
  [[229 0 18]
   [206 19 16]
   [184 38 14]
   [162 57 12]
   [140 76 10]
   [118 95 9]
   [95 114 7]
   [73 133 5]
   [29 171 1]
   [7 191 0]])

(def rgb-hsv
  [[229 0 17]
   [225 28 0]
   [221 73 0]
   [217 116 0]
   [213 157 0]
   [210 197 0]
   [175 206 0]
   [131 202 0]
   [88 198 0]
   [46 194 0]
   [7 191 0]])

#_(defn sub-sample [n xs]
  (let [k   (count xs)
        bin (quot k n)
        round-up (fn [n]
                   (if (pos? (rem n 1))
                     (inc (long n))
                     (long n)))
        _ (assert (< n k) "sub-sample must be less then origin!")
        step (/ k n)]
    (-> (->> (map (fn [idx]
                    (nth xs (round-up idx)))
                  (range 0 k step))
             (drop 1)
             (butlast)
             (into [(first xs)]))
        (conj (last xs)))))

(def green-amber-red-7
  (assoc (zipmap
          [0
           15 
           20
           35
           30
           35
           40
           45
           50
           55
           60
           100
           ]
          (reverse rgb-hsv))
         :default :red))

;;this is pretty similar to the assessment model in
;;marathon.assessment.  should be included / ported over.

;;cjcsm risk model is going to be...
;;high        [0 {nonsense} 1)
;;significant [1 1.5)
;;moderate    [2 1.5]
;;low         [inf 2)

(defn between? [n l r]
  (and (>= n l) (< n r)))

(def infs
  {:inf          Long/MAX_VALUE
   :inf-negative Long/MIN_VALUE
   :inf+         Long/MIN_VALUE
   :inf-         Long/MIN_VALUE})

(defn as-number [n]
  (if (number? n) n
      (or (get infs n)
          (throw (ex-info "Unknonwn number or constant!" {:n n})))))

;;cjcsm scoring
(def scores
  {:low         0
   :moderate    1
   :significant 2
   :high        3})

(defn invert [m]
  (into {} (for [[k v] m]
    [v k])))

;;These are bascially just projections onto a scale,
;;or other coordinate system.  When thinking in
;;terms of grammar of graphics, that's it.

(def bdr-ranges
  {:high         [0   1]
   :significant  [1   1.5]
   :moderate     [1.5 2]
   :low          [2   :inf]})

(def fill-ranges
  {:high          [0.0   60.0]
   :significant   [60.0  70.0]
   :moderate      [70.0  80.0]
   :low           [80.0  :inf]})
   
;;note: we're not checking to see if 
(defn ->assessor
  ([m f]
   (let [m (into {} (for [[k v] m]
                      [k (mapv as-number v)]))]
     (fn [n]
       (reduce-kv (fn [acc k [l r]]
                    (if (between? n l r)
                      (reduced (f k))
                      acc)) nil m))))
  ([m]
   (let [m (into {} (for [[k v] m]
                      [k (mapv as-number v)]))]
     (with-meta (fn [n]
                  (reduce-kv (fn [acc k [l r]]
                               (if (between? n l r)
                                 (reduced k)
                                 acc)) nil m))
       {:source m}))))

(def bdr  (->assessor bdr-ranges))
(def bdr-range  [0 10])
(def fill-range [0 100 #_101])
(def fill (->assessor fill-ranges))

;;normalized dwell...
(defn arf-risk
  ([b f]
   [(bdr b)
    (fill f)])
  ([bf] (arf-risk (first bf) (second bf))))

;;projects our 2d coordinate onto a 1d ordinal value using
;;a weighted average of the ordinal values + rounding.
(defn normalized-arf-risk [[b f]]
  (let [x (scores b)
        y (scores f)
        ]
    #_(assert (and (number? x) (number? y)) (str [b f x y]))
    (max x y))
  #_(/ (+ x y) 2.0))

;;example
#_(map (comp normalized-arf-risk arf-risk)(for [f (range 0 100 10) b (range 0 9)] [b f]))
#_(3 3 2 2 2 2 2 2 2 3 3 2 2 2 2 2 2 2 3 3 2 2 2 2 2 2 2 3 3 2 2 2 2 2
     2 2 3 3 2 2 2 2 2 2 2 3 3 2 2 2 2 2 2 2 3 2 1 1 1 1 1 1 1 3 2 1 1 1
     1 1 1 1 2 2 1 1 1 1 1 1 1 2 1 0 0 0 0 0 0 0)

;;so we can generate a heatmap for said values

(def ryg4
  (assoc (zipmap
         [0 1 2 3]
         (reverse (map rgb-hsv;[[229 0 17] [217 116 0] [175 206 0] [88 198 0] [7 191 0]]
                        [0 3 5 10])  #_rgb-hsv #_(color/get-palette :red-yellow-green-5)))
         :default :black))

(def color-scale (let [scores (invert scores)]
                   (into {} (for [[w color] ryg4]
                              [w {:text (get scores w "undefined") :color color}]))))

;;have multiple things for datasets...
;;datasets act like layers.
;;datasets have renderers.
;(defn get-datasets [c]
;  (for [i (range (.getDatasetCount c))]
;    (
;  )

;;bypassing incanter's add-lines.
;(defn add-lines [c d]
;  (let [di (.get
;  )

;; (defn discrete-paint-scale [chart colors]
;;   (let [c (count colors)
;;         scl (-> chart .getPlot .getRenderer .getPaintScale)]
;;     (.set
;;   )
;; (in-ns 'proc.risk)

;; (defn add-line-series [chart name xs ys color]
;;   (let [res (c/add-lines chart xs ys)
;;         n   (

(defn line-datasets [p]
  (vec (for [i (range 1 (.getDatasetCount p))] (.getDataset p i))))

(defn has-series? [x]
  (instance? org.jfree.data.xy.XYSeriesCollection x))
;;note: we've probably already written this in prock.stacked or the like.
(defn series-seq [p]
  (->>  (for [i (range (.getDatasetCount p))]
          (let [ds (.getDataset p i)]
            (when (has-series? ds)
              (for [[j xyseries] (map-indexed vector (.getSeries ds))]
                {:dataset i
                 :series j                 
                 :data  xyseries
                 :key (.getKey xyseries)
                 :renderer    (.getRenderer p i)
                 :legend-item (.getLegendItem (.getRenderer p i) 0 j)
                 
                 }))))
        flatten
        (filter identity)))

(defn cobble-legend
  ([chart options]
   (-> (for [{:keys [legend-item key]} (series-seq (.getPlot chart))]
          {:text key
           :shape (.getShape legend-item)
           :color (.getFillPaint legend-item)
           :line-color (.getLinePaint legend-item)
           })
        (c/->discrete-heat-legend options)))
  ([chart] (cobble-legend chart {})))

(defn add-derived-legend
  ([chart options]
   (let [legend (cobble-legend chart options)]
     (.addSubtitle chart legend)
     chart
     ))
  ([chart] (add-derived-legend chart {})))

;;this is a hack to help us get around the clunk in incanter...
(defn append-lines [chart xs ys & options]
  (let [n (-> chart .getPlot .getDatasetCount)
        {:keys [series dataset series-label width dash color points point-size]
         :or {series 0
              dataset n
              series-label (str "series-" n)}} (apply hash-map options)
        _ (println [n series-label])
        chart    (c/add-lines chart xs ys :series-label series-label :points (or points point-size))
        _        (when color
                   (c/set-stroke-color chart color
                                       :series series :dataset dataset ))
        _        (when (or width dash)
                   (c/set-stroke chart
                                 :series series
                                 :dataset dataset
                                 :width (or width 1.0)
                                 :dash (or dash 1.0)))
        _       (when  point-size
                  (c/set-point-size chart point-size
                                 :series  series
                                 :dataset dataset))]
    chart))


(def performance-trends
  [{:x [10 30 55 80]
    :y [8 4 2 0]
    :series-label "Growth (x+1/y+1/z+1), AC1:0"
    :points true
    :width 5.0
    :color :blue
    :point-size 10.0}
   {:x [10 30 55 80]
    :y [0 2 4 8]
    :series-label "Current Structure (x/y/z), AC1:0"
    :points true
    :width 5.0
    :dash 10.0
    :color :black
    :point-size 10.0}
   {:x [10 20 45 80 90 100]
    :y [4  3.5 3 2  1.8 1]
    :series-label "Current Structure (x/y/z), AC1:2"
    :points true
    :width 5.0                         
    :color :dark-blue
    :point-size 10.0}])

(def trend-styles
  {"Growth (x+1/y+1/z+1), AC1:0"  {:series-label "Growth (x+1/y+1/z+1), AC1:0"
                                   :points true
                                   :width 5.0
                                   :color :blue
                                   :point-size 10.0}
   "Current Structure (x/y/z), AC1:0"   {:series-label "Current Structure (x/y/z), AC1:0" 
                                         :points true
                                         :width 5.0
                                         :dash 10.0
                                         :color :black
                                         :point-size 10.0}
   "Current Structure (x/y/z), AC1:2" {:series-label "Current Structure (x/y/z), AC1:2"
                                       :points true
                                       :width 5.0                         
                                       :color :dark-blue
                                       :point-size 10.0}})

(defn add-trend [chart trend]
  (let [{:keys [x  y]} trend]
    (apply append-lines (into [chart x y] (flatten (seq trend))))))

(defn add-performance-trends [chart trends]
  (as-> chart it
      (reduce add-trend it trends)
      (add-derived-legend it{:position :bottom})))

(def surface (atom nil))
(defn simple-response
  [trends & {:keys [min-z max-z colors sample-f title x-label y-label z-label]
      :or {colors color-scale #_ryg4 #_green-amber-red-7
           min-z 0
           max-z 4
           title   "Risk to Mission"
           x-label "% Demand Met"
           y-label "Stress (Bog:Dwell)"
           z-label "Risk Assessment"
           sample-f arf-risk
           }}]
  (let [[xmin xmax] fill-range 
        [ymin ymax] bdr-range
        c (c/heat-map
           (fn [f b]
             (normalized-arf-risk (sample-f b f)))
           xmin xmax
           ymin ymax
           :title   title
           :x-label x-label
           :y-label y-label
           :z-label z-label
           :x-res 100
           :y-res 100
           :include-zero? false
           :min-z min-z
           :max-z max-z
           :colors colors    
           :discrete-legend? true
           :grid-lines? :both
           :position :top
           )
        _ (reset! surface c)]
    (i/view
      (add-performance-trends c trends))))

(defn rotational-discount
  "Computes the resulting supply given
  rotational policy parameters."
  [bog dwell overlap mob]
  (double (/ (- bog overlap mob)
             dwell)))

;;computing trends....
(defn algebraic-supply
  ([supply bog cyclelength overlap mob]
   (let [disc (rotational-discount bog cyclelength overlap mob)
         expected-supply (* disc supply)
         expected-dwell  (- cyclelength bog mob)]
     {:supply supply
      :expected-supply expected-supply
      :dwell expected-dwell}))
  ([supply policy]
   (let [{:keys [bog cyclelength overlap mob]} policy]
     (algebraic-supply supply bog cyclelength overlap mob))))

(defn performance [demand ac-supply  ac-policy rc-supply rc-policy]
  (let [ac (algebraic-supply ac-supply ac-policy)
        rc (algebraic-supply rc-supply rc-policy)
        total-supply ( + (:expected-supply ac) (:expected-supply  rc))
        fill (double (/ total-supply demand))]
    {:ac ac
     :rc rc
     :total-supply total-supply
     :fill fill}))

(def rc10 {:bog 9
           :cyclelength 9
           :overlap 0
           :mob 0})

(def ac10 {:bog 9
           :cyclelength 9
           :overlap 0
           :mob 0})

(def ac12 {:bog 9
           :cyclelength 18
           :overlap 1
           :mob 0})

(def rc-policies
  (take-while (fn [m]
                (>= (/ (:bog m) (:cyclelength m)) 1/8)) 
              (iterate (fn [{:keys [bog cyclelength overlap mob] :as seed}]
                         (assoc seed :cyclelength (+ cyclelength 1)
                                :overlap 1))
                       rc10)))
                
(defn rc-experiment
  "Let's examine various rc access policies, going from a spectrum
   of 1:8 down to 1:0."
  [demand ac-supply ac-policy rc-supply rc-policies]
  (for [rc-policy rc-policies]
    (let [bdr (/ (:bog rc-policy)
                 (:cyclelength rc-policy))
          perf (performance demand ac-supply  ac-policy rc-supply rc-policy)]
      {:bdr bdr
       :fill (:fill perf)})))


(defn experiment->trend [label xs]
  (reduce (fn [acc {:keys [bdr fill]}]
            (-> acc
                (update :x conj (double bdr))
                (update :y conj (double fill))))
          {:series-label label
           :x []
           :y []} xs))


(defn plot-experiment []
  (let [trends (->> (rc-experiment 10 5 ac10 5 rc-policies) (experiment->trend  "Growth (x+1/y+1/z+1), AC1:0"))]
    {:x [10 30 55 80]
     :y [8 4 2 0]
     :series-label "Growth (x+1/y+1/z+1), AC1:0"
     :points true
     :width 5.0
     :color :blue
     :point-size 10.0}
  )
