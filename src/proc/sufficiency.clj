(ns proc.sufficiency
  (:require [proc [core :as core] [util :as util] [stacked :as stacked]]
            [spork.util [table :as tbl]])
  (:use [incanter.core]))

;______________________If we ever want to do a time series, this will help:

(defn load-fills [path] (util/as-dataset path))
;;split up the dataset into two smaller datasets, one where we have missed 
;;demands, and one where we don't.  We'll ignore NotUtilized.  Note the 
;;functions prefixed by $, these are Incanter functions and are meant to 
;;be using when messing with Incanter datasets (which the filldata kind of is).
;;We could use analagous clojure library functions, like group-by and filter, 
;;but for now the post processor just happens to be going the other route.
(defn filled-notfilled [ds] 
  (->> ds
      ($where ($fn [DemandGroup] (not= DemandGroup "NotUtilized")))
      ($group-by   ;using incanter's $group-by function to operate on the dataset
        ;our group-key function depends on the fill-type of the filldata record.
        ($fn [fill-type] (if (= fill-type "Unmet") :missed :filled)))))

;;once we have them split out, we may have two datasets which we can process 
;;in our typical way (building fill data for the fill charts).  This time, 
;;we modify our typical pipeline for processing raw filldata into the aggregated
;;data for the charts, since our aggregation criteria has changed a little.
;;let's assume we have a filldata file at
(def the-path "c:/path/to/some/project/fills/BCT.txt")
;;we'll go ahead and get our datasets pulled in...
(defn datasets [path] (->> path 
                   (load-fills) 
                   (filled-notfilled)))

;;note, that was semantically equivalent to
;;(filled-notfilled (load-fills the-path))

;for pedagogical purposes, we'll get a handle on our data and bind them to some
;top-level vars...
(defn missed-data [ds] (:missed ds))
(defn filled-data [ds] (:filled ds))

;;The task at hand is to, for each SRC DemandGroup Phase Day, 
;;=>determine the number of deployed UICs that are sufficient 
;;=>return a sequence of UICs that are insufficient, and their corresponding 
;;  dwell before deployment.
;;=>return a sequence of UICs that are extended, and their days? of extension 
;;  (TBD)

;;Copied/altered from proc.stacked/sufficiency-cat-fill 
;;We can use our already-existing function to categorize sufficiency
(defn sufficiency-cat-fill 
  [acthresh rcthresh fill-type 
      DwellBeforeDeploy Component DwellYearsBeforeDeploy]
  (cond (= fill-type "Unmet") "Unmet Demand"
        ;(< DwellBeforeDeploy acthresh) "Extended Force"  ;craig: what the hell is the extended force?
    ;extended force is related to BOG and not dwell before deploy, am I wrong?
        :else
        ;craig: looks like we want to use this in days
        (if (>= DwellBeforeDeploy (case Component "AC" acthresh rcthresh))
          "Met Demand" 
          "Dwell: AC<1yr RC<4yr")))

;;used for grouping criteria, returns :ready if the stats indicate the unit had
;;sufficient dwell before deployment according to the threshold, :not-ready 
;;otherwise.
(defn ready? [fill-type {:keys [DwellBeforeDeploy Component 
                                     DwellYearsBeforeDeploy]}]
  (case (sufficiency-cat-fill 365 (* 4 365) fill-type 
           DwellBeforeDeploy Component DwellYearsBeforeDeploy)
       "Met Demand" :ready
       "Extended Force" :extended
       :not-ready))
;;aux function to extract a vector from the keys associated in a 
;;map.
(defn get-key [{:keys [DemandGroup SRC DemandName start]}]
               [DemandGroup SRC DemandName start])
;;note: another way to write get-key is:
;;(def get-key (juxt :DemandGroup :SRC :DemandName :Day))

;if its 1:0, # days extended.  I think this is just, how many units are 
;;deploying at a location that have < threshold of dwell?  Accurate?  or is 
;;it 0 dwell?  "How long" can be computed secondarily.
(defn compute-extension 
  [{:keys [start DwellBeforeDeploy]}]
  ;;hmmmm, to be determined...not certain how we want to compute extended days.
  ;;Do we use the amount of time since the last known non-extended
  ;;deployment?  Probably easiest, presented as a simple reduction.  For now,
  ;;it's just stubbed in though.
  (throw (Exception. (str "Currently not implemented, figure it out Craig :)")))
  ) ;this function is too happy.

;;Given a dataset of filled records, we group the records into populations
;;that share the same SRC DemandName DemandGroup and startday (or t).
;;This lets us operate on the groups separately and then report our two 
;;useful results: the quantity of units that are sufficiently ready, 
;;and the sequences of units that aren't.    We get a sequence of records
;;back in the process, which we may want to spit out to a table (more 
;;on that later).  I tacked on the extended units as well, although we're 
;;not really processing them atm.
(defn compute-results [filled]
  (for [[[DemandGroup SRC DemandName t] xs]  (group-by get-key filled)]
    (let [{:keys [ready extended not-ready]}
                 (group-by (partial ready? "Met Demand") xs)]
      {:t t :DemandGroup DemandGroup :SRC SRC :Phase DemandName 
       :ready     (count ready)
       :not-ready (map  (juxt :name :DwellYearsBeforeDeploy) not-ready)
       :extended  (map compute-extension extended)})))

;;We can get a sequence of rows from the incanter dataset, which is compatible
;;with compute-results:
(def sufficieny-results (compute-results (:rows filled-data)))
;;If we want to spew our results out to a table, we can do so fairly easily..
;;Ugh, this is begging for refactoring, but it's passable.
(defn spew-results [results out-dir]
  (let [not-readies (for [{:keys [not-ready] :as r}  (filter :not-ready results)
                          stats not-ready]
                      (-> r (dissoc :not-ready :extended) (merge stats)))
        extendeds    (for [{:keys [extended] :as r} (filter :extended results)
                           stats extended]
                       (-> r (dissoc :not-ready :extended) (merge stats)))
        ready-counts (map (fn [r] (dissoc r :not-ready)) results)
        spit! (fn [out xs] (->> xs
                             (tbl/records->table)
                             (tbl/table->tabdelimited)
                             (util/spit-table (str out-dir out))))]
    (do (spit! (str out-dir "ready-counts.txt") ready-counts)
        (spit! (str out-dir "notreadies.txt") not-readies)
        (spit! (str out-dir "extended.txt") extendeds))))
 
;;Note, there is an alternate (but very similar) pipeline using incanter's 
;;dataset operations, with an analogous $group-by (and similar operations).  

;;Unfilled demands
;;================

;;Since we have our unfilled demands already broken out, we just want to 
;;collect the missed demands by unit, demand, src, phase.  Easy enough.

;;by type
;;by demandgroup 
;;unfilled
;;days unfilled
;;  missed demand

;;I think we can just scrape this out of the fills, filtering on unmet 
;;demands.  The days unfilled is either the delta of (- end start) for the 
;;proxy unit (the ghost) that's missing the demand, or if they want a daily
;;accounting we can do that too.

;;More to come....