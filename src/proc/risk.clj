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
  (and (>= n l) (<= n r)))

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

;;These are bascially just projections onto a scale,
;;or other coordinate system.  When thinking in
;;terms of grammar of graphics, that's it.

(def bdr-ranges
  {:high         [0   0.99]
   :significant  [1   1.49]
   :moderate     [1.5 1.99]
   :low          [2   :inf]})

(def fill-ranges
  {:high          [0.0   59.0]
   :significant   [60.0  69.0]
   :moderate      [70.0  79.0]
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
     (fn [n]
       (reduce-kv (fn [acc k [l r]]
                    (if (between? n l r)
                      (reduced k)
                      acc)) nil m)))))

(def bdr  (->assessor bdr-ranges))
(def bdr-range  [0 10])
(def fill-range [0 100])
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
        y (scores f)]
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
          #_(range 7)
         [0
          1
          2
          3
          4
          ]
         (reverse (map rgb-hsv;[[229 0 17] [217 116 0] [175 206 0] [88 198 0] [7 191 0]]
                        [0 0 3 5 10])  #_rgb-hsv #_(color/get-palette :red-yellow-green-5)))
         :default :black))
  

(defn simple-response
  [& {:keys [min-z max-z colors sample-f title x-label y-label z-label]
      :or {colors ryg4 #_green-amber-red-7
           min-z 0
           max-z 4
           title   "Risk to Mission"
           x-label "% Demand Met"
           y-label "Stress (Bog:Dwell)"
           z-label "Risk Assessment"
           sample-f arf-risk
           }}]
  (let [[xmin xmax] fill-range 
        [ymin ymax] bdr-range]
    (i/view
     (c/heat-map (fn [f b]
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
                 :grid-lines? :both
                 ))))

#_(defn held-response [max-flow  client-range jrpc-range]
  (simple-response max-flow client-range jrpc-range
                   :title  "Max Clients Held per JRPC, by (#Clients, #JRPCs)"
                   :z-label "Max Clients Held Per JRPC"
                   :sample-f held-experiment
                   :colors :blues-9
                   :min-z 0
                   :max-z 10000
                   ))
