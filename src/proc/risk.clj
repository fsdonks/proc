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

(defn classify [days]
  (cond (> days 50) 3
        (> days 20) 2
        (> days 10) 1
        :else 0))

(defn arf-risk [bdr fill]
  ;(condp )
 )

(defn simple-response
  []
   & {:keys [min-z max-z colors sample-f title x-label y-label z-label]
      :or {colors green-amber-red-7
           min-z 0
           max-z 3
           title   "Risk to Mission"
           x-label "% Demand Met"
           y-label "Stress (Bog:Dwell)"
           z-label "Risk Assessment"
           sample-f arf-risk
           }}]
  (let [[xmin xmax] client-range
        [ymin ymax] jrpc-range]
    (i/view
     (c/heat-map (fn [x y]
                   (sample-f max-flow y (* x 1000)))
                 xmin xmax
                 ymin ymax
                 :title   title
                 :x-label x-label
                 :y-label y-label
                 :z-label z-label
                 :x-res 500
                 :y-res 200
                 :include-zero? false
                 :min-z min-z
                 :max-z max-z
                 :colors colors 
                 :grid-lines? :both
                 ))))

(defn held-response [max-flow  client-range jrpc-range]
  (simple-response max-flow client-range jrpc-range
                   :title  "Max Clients Held per JRPC, by (#Clients, #JRPCs)"
                   :z-label "Max Clients Held Per JRPC"
                   :sample-f held-experiment
                   :colors :blues-9
                   :min-z 0
                   :max-z 10000
                   ))
