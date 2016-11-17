
;This is work done to sample and look at unit lifecycles over time.
(ns proc.lifecycles
  (:require [spork.util.table :as tbl]         
            [clojure.core.reducers :as r]
            [proc.schemas :as schemas]     
            [clojure.string :as str]
            [proc.util :as util])
  (:use [incanter.core]
        [incanter.charts]))

(defn get-policies 
  "returns policy records from AUDIT_Parameters."
  [root]
  (let [recs (into [] (tbl/tabdelimited->records (str root "AUDIT_Parameters.txt") :schema schemas/parameters ))]
    (-> (keep-indexed (fn [idx item] (when (contains? #{6 7 8} idx) item)) recs) ;these are policy lines
      )))

(defn policy-changes-by-unit 
  "Returns a map of UIC to a sequence of policy changes."
  [root & {:keys [include-unit?] :or {include-unit? identity}}]
  (binding [tbl/*split-by* #","]
    (let [recs (->>  (tbl/tabdelimited->records (str root "EventLog.csv") :schema schemas/eventlog
                                                :parsemode :noscience 
                                                :keywordize-fields? false)
                 (r/filter (fn [r] (and (= (r "EventType") "Unit Changed Policy") (include-unit? (r "EntityFrom")))))
                 (r/foldcat))]
    (group-by (fn [r] (r "EntityFrom")) recs))))

(defn get-changes 
  "For one unit, returns x and y coordinates before and after policy changes or lifecycle resets.  xs are records from subcycles.txt, last-day of the simulation
is an integer, and policy-changes are the policy change records for this unit."
  [xs policy-changes last-day]
  (let [new-lifecycles (filter (fn [r] (= (r "Duration") 0) ) (rest xs))
        
        policy-changes (remove (fn [rec] (contains? (set (map (fn [r] (r "t")) new-lifecycles)) ;this could happen I suppose     
                                                    (rec "Time"))) policy-changes)
        events (sort-by (fn [r] (if-let [t (r "t")] t (r "Time"))) 
                        (concat new-lifecycles policy-changes))]             
    (loop [last-cycle-time ((first xs) "Duration")
           last-time ((first xs) "t") 
           xpoints [((first xs) "t")]
           ypoints [((first xs) "Duration")]
           next-recs events]
      (if (empty? next-recs)
        [(conj xpoints last-day) (conj ypoints (+ last-cycle-time (- last-day last-time)))]
        (let [next-rec (first next-recs)
              deltat (if-let [time (next-rec "t")]
                       (- (next-rec "t") last-time)
                       (- (next-rec "Time") last-time))]
          (if (= (next-rec "Duration") 0) ;new lifecycle
              (recur 0
                     (next-rec "t")
                     (conj xpoints (- (next-rec "t") 0.001) (next-rec "t"))
                     (conj ypoints (+ last-cycle-time deltat) 0)
                     (rest next-recs))
              ;otherwise, we're at a policy change
              (let [new-cycle-time (Integer/parseInt (last (str/split (next-rec "Message") #":")))
                    prev-cycle-time (+ last-cycle-time deltat)
                    parsed-prev-cycle-time (-> (filter (fn [s] (.contains s "->" )) (str/split (next-rec "Message") #":" ))
                                             (first)
                                             (str/split  #"->")    
                                             (first)
                                             (Integer/parseInt))
                    _ (when (not= parsed-prev-cycle-time prev-cycle-time)
                        (throw (Exception. (str "There is a problem with cycle time calculation" 
                                                [parsed-prev-cycle-time :not :equal prev-cycle-time]))))]
                                              (recur new-cycle-time
                                                     (next-rec "Time")
                                                     (conj xpoints (- (next-rec "Time") 0.001) (next-rec "Time"))
                                                     (conj ypoints prev-cycle-time new-cycle-time)
                                                     (rest next-recs)
                                                     ))))))))    

;to know where units are in their lifecycles, we just need to know when they start new lifecycles and when they change policies.
;we'll use event-log for policy changes and subcycles.txt for new lifecycles.
(defn graph-sub-cycles 
  "View a graph containing multiple line-plots of unit lifecycles.  Define a filter function via :include-unit? to include only certain units"
  [root & {:keys [include-unit?] :or {include-unit? (fn [uic] true)}}]
  (let [recs (->> (tbl/tabdelimited->records (str root "subcycles.txt") :schema schemas/subcycles 
                                                        :parsemode :noscience 
                                                        :keywordize-fields? false)
                          (r/filter (fn [r] (include-unit? (r "UIC"))))
                          (r/foldcat))
       policy-changes (policy-changes-by-unit root :include-unit? include-unit?)    
       end-day (util/last-day root)
       points  (->> (group-by (fn [r] (r "UIC")) recs)
                 (map (fn [[unit xs]] (concat [unit] (get-changes xs (policy-changes unit) end-day)))))
       [firstunit firstxs firstys] (first points)
       plot (xy-plot firstxs firstys :legend true :series-label firstunit)
       _ (doseq [[unit xs ys] (rest points)]
            (add-lines plot xs ys :series-label unit))]
   (view plot)))
