
(ns proc.satisfaction
  (:require [clojure.pprint :as ppr])
  (:use [incanter.core]
        [incanter.excel]
        [incanter.io]))

;This namespace is a collection of functions used to output demand satisfaciton by period and by sub-periods for each period. i.e
;provide demand satisfaction for PreSurge in 30-day increments (or chunks as I call them here).

(defn ncfa-bad-adj 
  "Adjust the presurge, surge, and postsurge periods for demand shifts based on how we were doing it in Access"
  [[period [low high]] & {:keys [transform]}] ; ["+" 30]
  (let [[op size] transform]
    (if transform
      (case period "Presurge" (case op "+" [period [low high]]
                                       "-" [period [low (- high size)]])
                   "Surge" (case op "+" [period [low (+ high size)]]
                                    "-" [period [(- low size) (- high size)]])
                   "Postsurge" (case op "+" [period [(+ low size) high]]  ;;should high change b/t runs?
                                        "-" [period [(- low size) high]]))
      [period [low high]])))

(defn change-bounds 
  "This sounds dumb to me now though, we should just be pulling the periods from AUDIT_PeriodRecords.
Given a vector of runtimes, use the bad ncfa algorithm to adjust the period timings"
  [op size runts] (map #(ncfa-bad-adj % :transform [op size]) runts))            

;jason was okay with mergelast last for surge and postsurge
(defn get-new-periods 
  "Partitions the period by chunksize from low to high and can merge a smaller last chunk into the next-to-last chunk."
  ;seems like another way to do this is merge the last chunk if it's length is < (* .5 chunksize) [like rounding] or always
  ;leave the remainder and the data can be merged later.
  [low high chunksize & {:keys [mergelast?] :or {mergelast? true}}] 
  (if (> chunksize (- high low))
    [low high]
  (let [lows (range low (inc high) chunksize)
        highs (conj (mapv dec (rest lows)) high)
        newbounds (mapv vector lows highs)
        [start finish] (last newbounds)]
    (if (or (= (- finish start) (dec chunksize)) (not mergelast?))
      newbounds
      (let [head (vec (drop-last 2 newbounds))
            [drop1 drop2] (take-last 2 newbounds)
            newlast [(first drop1) (last drop2)]]
      (conj head newlast))))))

(defn mirror-vals 
  "Wanted to use get-new-periods for the tail-chunks, but had to 'flip' the values first. 
(mirror-vals [1 4 10 23 30] :up? true) returns [30 37 50 56 59]
(mirror-vals [30 37 50 56 59]) returns [1 4 10 23 30]"
  [seq & {:keys [up?]}]  ;assumed sorted from low to high (or low is first and high is last)  
  (let [low (first seq)
        high (last seq)
        delta (- high low)]
    (if up?
      (vec (conj (for [x (reverse (butlast seq))] (+ (- high x) high)) high))
      (conj (vec (reverse (for [x (rest seq)] (- low (- x low))))) low))))

(defn head-chunks 
  "Breaks up the low and high bounds into numchunks number of chunks of chunksize from low to high with any remainer of the
period added on as an extra chunk"
  [low high chunksize numchunks]
  (if (< (inc (- high low)) chunksize)
    (do (ppr/pprint ["Chunksize is bigger than the period bounds"]) [low high])
    (let [hchunks (->> (get-new-periods low high chunksize :mergelast? true)
                    (take numchunks)
                    (vec))
          [nextlast chunkstop] (last hchunks)]
      (if (= chunkstop high)
        (do (when (> numchunks (count hchunks))
              (ppr/pprint ["Couldn't get " numchunks " whole chunks with a chunksize of " chunksize]))
          hchunks)
        (conj hchunks [(+ chunkstop 1) high])))))

(defn tail-chunks 
  "Opposite of head-chunks.  From low to high, partitions the range into numchunks number of vectors.  Each vector is a [lowbound highbound]
pair.  If chunksize and numchunks don't account for the total range of low to high, another vector will be tacked on at the beginning of
the vector wich contains the last bin down to low." 
  [low high chunksize numchunks]
  (let [[newlow newhigh] (mirror-vals [low high] :up? true)]
       (->> (head-chunks newlow newhigh chunksize numchunks)
         (reduce concat)
         (mirror-vals)
         (partition 2)
         (mapv vec))))

(defn split-period
  "Partitions the bounds using splitmap"
  [splitmap name+bounds]
  (let [[period [low high]] name+bounds 
        chunksize (:chunksize (get splitmap period))
        numchunks (:numchunks (get splitmap period))
        tailchunks? (:tailchunks (get splitmap period))
        ]
    (if chunksize
      (if (< (inc (- high low)) chunksize)
        [period [[low high]]]  ;nested bounds to show period fields in later cals       
        [period (if numchunks 
                  (if tailchunks?
                    (tail-chunks low high chunksize numchunks)
                    (head-chunks low high chunksize numchunks))
                  (get-new-periods low high chunksize))])
      name+bounds))) 

(defn split-periods! 
  "Take a vector of period, bounds tuples and partitions all bounds using splitmap"
  [periods splitmap]
  
  (map (partial split-period splitmap) periods))

(defn per-pred
  "Used to filter demandtrends for the period from low to high"
  [low high]
  (if (and (not low) (not high))
    ($fn [t] true)
    (if (not high)
      ($fn [t] (>= t low))
      (if (not low)
        ($fn [t] (<= t high))
        ($fn [t] (and (>= t low) (<= t high)))))))

;Only wanted demandgroup in the surge period for NCFA.
(defn dmd-sat 
  "Takes a dataset and returns the demand satisfaction, #required days, and #deployed days for every SRC."
  [ds [low high] & {:keys [name]}]
  (let [pred (per-pred low high)
        ts (vec (apply sorted-set (map #(:t %) (:rows ds))))
        dt (conj (vec (for [i (range (- (count ts) 1))] (- (nth ts (+ i 1)) (nth ts i)))) 0)
        t2dt (zipmap ts dt)
        rds (->> (assoc ds :rows (map #(assoc % :dt (get t2dt (:t %))) (:rows ds)))
              ($where pred)
              (add-derived-column :Deployed [:dt :Deployed] (fn [dt depd] (* dt depd)))
              (add-derived-column :Required [:dt :TotalRequired] (fn [dt totreqd] (* dt totreqd))))
        bycols (if (= name "Surge")  [:DemandGroup :SRC] [:SRC])]
    (->> (map (fn [col] ($rollup :sum col bycols rds)) [:Required :Deployed])
      (reduce (fn [ds1 ds2] ($join [bycols bycols] ds1 ds2)))
      (add-derived-column :DemandSat [:Required :Deployed] (fn [treq tdep] (if (= treq 0) 0 (float (/ tdep treq))))))))
  
(defn subpsat 
  "For use with dmd-sat! to calculate demand sub-period satisfaction for another sub-period and conj"
  [dtrends name currds bounds]
  (let [[low high] bounds
        newds (dmd-sat dtrends bounds :name name)
        withsubper (add-column :Period (repeat (count (:rows newds)) (keyword (str low "->" high))) newds)]
    (if (empty? currds)
      withsubper
      (conj-rows currds withsubper)))) 
    
(defn dmd-sat!
  "If the bounds are a sequence of periods, we want to calculate demand satisfaction for each period"
  [ds bounds name]
  (if (coll? (first bounds))
    (reduce (partial subpsat ds name) [] bounds)
    (dmd-sat ds bounds)))

(defn get-time-bound 
  "Returns the max or min time in demandtrends"
  [root & {:keys [ds bound] :or {ds (read-dataset (str root "DemandTrends.txt") :header true :delim \tab)}}]
  (apply bound (map #(:t %) (:rows ds))))

(defn dsats 
  "Spits out demand satisfaction stats to root"
  [root & {:keys [splits periods ds] :or {splits {}
                                                     
                                                    ds (read-dataset (str root "DemandTrends.txt") :header true :delim \tab)}}]
  (let [pmap (apply hash-map (vec (reduce concat periods)))
        [prst pren] (get pmap "Presurge")
        [post poen] (get pmap "Postsurge")
        pmap (if prst pmap (assoc pmap "Presurge" [(get-time-bound root :bound min :ds ds) pren]))
        pmap (if poen pmap (assoc pmap "Postsurge" [post (get-time-bound root :bound max :ds ds)]))
        periods (vec (for [x ["Presurge" "Surge" "Postsurge"]] [x (get pmap x)])) 
        _ (ppr/pprint [[:Calculating :and :dumping :demandsat :to] root [:by :periods] periods [:with :subperiods] splits])
        sheets (reduce (fn [curr [name bounds]] (conj curr (str name "DemandSat") (dmd-sat! ds bounds name)))
                       [] (split-periods! periods splits))]                       
    (save-xls sheets (str root "demandsat.xlsx"))))

;probably want to make this a get-periods fn for the Marathon root directory if we use this again.
;notional data
(def runtimes7 [["Presurge" [600 700]] ["Surge" [701 705]] ["Postsurge" [706 nil]]]) ;his post-surge start was a typo?

(def splitmap7 {"Surge" {:chunksize 40 :numchunks 4} 
                ;my interprepation of Jason's new timing.  Surge into 40-day chunks and rest as phase 4
                
                "Presurge" {:chunksize 2000} ;this shows time field in demand sat.  High bound will float with change-bounds
               "Postsurge" {:chunksize 365 :numchunks 2}}) ;defaults to tail-chunks if :numchunks

;or as value (change-bounds "-" 90 runtimes).  Should just pull runtimes from the Marathon root dir though.
(def turn7 {
"V:/Branch - Force Generation Modeling and Analysis/NCFA Support to ASTON/Turn 8 20151019/HLD Excursion/BCA/" runtimes7
"V:/Branch - Force Generation Modeling and Analysis/NCFA Support to ASTON/Turn 8 20151019/HLD Excursion/PB/" runtimes7
"V:/Branch - Force Generation Modeling and Analysis/NCFA Support to ASTON/Turn 8 20151019/Aviation Excursion/" runtimes7})

(defn run-dmd-sat 
   "We probably want to run demand satisfaction for multiple runs"
  [allrunsmap splits] 
  (map (fn [path] (dsats path :splits splits :periods (get allrunsmap path))) (keys allrunsmap)))

; usage:  (run-dmd-sat turn7 runtimes7)
;______________________