
;;This is a simple simulation to explore 
;;the sensitivity in the parameters of 
;;cycle time deviation
(ns proc.lite
  (:require   [incanter.core :refer :all]
              [incanter.io :refer :all]
              [incanter.charts :refer :all]
              [incanter.stats]
              [spork.cljgui.components.swing :as swing])
  (:import [javax.swing JPanel JSlider JLabel]
           [java.awt  BorderLayout]))
 

(defn deploy? [x y l r ct]
  (if (and (>= ct (- y l))
           (<= ct (+ y r))) 
    1
    0))

(defn dumb-sim [x y l r n] 
  (let [ct (+ x y)
        random-pop! (fn []
                     (take n (repeatedly (fn [] (rand ct)))))
        width       (float (/ ct n))        
        even-pop    (take n (iterate (fn [x] (+ x width)) 0))
        deployable (fn [xs] (reduce (fn [acc c] (+ acc (deploy? x y l r c))) 0 xs))]
    (println [:baseline-deployable  (deployable even-pop)])
    {:baseline (deployable even-pop)
     :sample (fn [] (deployable (random-pop!)))}))
     
(defn simulate [{:keys [sample]} reps] (take reps (repeatedly sample)))


(defn slider-panel
  ([updater-fn slider-values]  (slider-panel updater-fn slider-values nil))
  ([updater-fn slider-values slider-label]
     (let [max-idx (dec (count slider-values))
           label-txt (fn [v] (str (when slider-label (str slider-label " = ")) v))
           label (JLabel. (label-txt (first slider-values)) JLabel/CENTER)
           slider (doto (JSlider. JSlider/HORIZONTAL 0 max-idx 0)
                    (.addChangeListener (proxy [javax.swing.event.ChangeListener] []
                                          (stateChanged [^javax.swing.event.ChangeEvent event]
                                                        (let [source (.getSource event)
                                                              value (nth slider-values (.getValue source))]
                                                          (do
                                                            (.setText label (label-txt value))
                                                            (updater-fn value)))))))
           panel (doto (JPanel. (BorderLayout.))
                   (.add label BorderLayout/NORTH)
                   (.add slider BorderLayout/CENTER))]
       panel)))


;; (defmacro slide-by [atm chrt samplef rng nm]
;;   `(slider-panel #(do (reset! ~atm %)
;;                       (set-data ~chrt  [~'x ~samplef])) 
;;                  ~rng ~nm))

(defmacro slide-by [atm chrt samplef rng nm]
  `(slider #(do (reset! ~atm %)
                (set-data ~chrt  [~'x ~samplef])) 
           ~rng ~nm))
  


(defn interactive-sim  [x y l r n reps]
  (let [x_ (atom x)
        y_ (atom y)
        l_ (atom l)
        r_ (atom r)
        n_ (atom n)               
        reps_ (atom reps)
        resample! (fn [] (simulate (dumb-sim @x_ @y_ @l_ @r_ @n_ ) @reps_))
        chrt   (histogram (resample!)  :density true :nbins 30)]
    (let [x    (slide-by x_ chrt    (resample!) (range 0.1 x 0.1) "X")
          y    (slide-by y_ chrt    (resample!) (range 0.1 y 0.1) "Y")
          l    (slide-by l_ chrt    (resample!) (range 0.1 l 0.1) "L")
          r    (slide-by r_ chrt    (resample!) (range 0.1 r 0.1) "R")
          n    (slide-by n_ chrt    (resample!) (range 0.1 n 0.1) "N")
          reps (slide-by reps_ chrt (resample!) (range 1 reps 10) "REPS")]
      (view chrt))))
      ;; (swing/display (swing/empty-frame)
      ;;                (swing/shelf 
      ;;                  chrt 
      ;;                  (swing/stack x 
      ;;                               y 
      ;;                               l 
      ;;                               r 
      ;;                               n reps))))))
                       
                       
                       
          

          

    
