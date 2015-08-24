;;This is a quick example in clojure of how we can convert marathon 
;;location information into generalized location trends, and then 
;;extract location information over time.  Note, we also derive
;;information from marathon demand records, to cross walk the fill 
;;over time.  We may also incorporate deployment information 
;;from marathon to determine dwell before deployment.
;;Ideally.....
;;We have built a database of unit statistics that can 
;;support multiple queries....

;;The queries we want to support are:

;;Location fill over time (including unfilled).
  ;;Supports the generation of trends over time that depict a
  ;;container (location's) fill trend over time.
  ;;Note, we need to annotate the fact that the unit was
  ;;overlapping....
  ;;It may be helpful to annotate the state of the unit at each
  ;;location....
  ;;Particularly if we are broadcasting in clojure data structures...
  ;;Locations would be incredibly helpful if we add the unit's 
  ;;current state, and dwell time....
  ;;That adds to size of the data, however....
  ;;Since unit movement dictates location changes though, it leaves
  ;;the total size still fairly small....much more amenable to
  ;;querying after the fact.
  ;;One important fact is that we have meta-data associated with
  ;;locations.


;;Even better...we can cross-walk a unit's deployment information with 
;;it's location.  We currently know the unit's dwell before deploy 
;;due to location.  We can also filter out followon deployments....

;;If the unit's dwell-before deploy is useful

(ns proc.core
  (:require [spork.util.excel [core :as xl]]
            [spork.util.table :as tbl]
            [spork.util.io    :as io]
            [spork.util.reducers]
            [spork.util.parsing :as parse]
            [spork.cljgui.components.swing :as swing]
            [clojure.core.reducers :as r]
            [proc.util :as util]
            [incanter.core :refer :all]
            [incanter.io :refer :all]
            [incanter.charts :refer :all]
            [incanter.stats]
            [proc.patches :as patches]
            [proc.schemas :as schemas]
            [proc.stacked :as stacked]
            [spork.util.temporal :as temp]
            [spork.sketch :as sketch]
            [iota :as iota])
  (:import [org.jfree.chart ChartPanel]))


(def locs     "C:/Users/thomas.spoon/Documents/MarathonHacks/locations.txt")
(def dtrends  "C:/Users/thomas.spoon/Documents/MarathonHacks/DemandTrends.txt")
(def deps     "C:/Users/thomas.spoon/Documents/MarathonHacks/AUDIT_Deployments.txt")
(def strends  "C:/Users/thomas.spoon/Documents/MarathonHacks/SandTrends.txt")


;;Allows us to define filters to avoid doing work.
(def ^:dynamic *srcs* #{})
;;filters that let us focus on a subset of units instead of processing
;;everything  

;;A quick filter that lets us skip lines in the demand trend.
;;We save a lot of time parsing doing this, particularly if we only
;;care about a specific SRC.
(def ^:dynamic *demand-trend-pre-filter* identity)
;;filters demand trends, defaults to include every demand record.
(def ^:dynamic *demand-trend-filter* identity)

;;filters location records, defaults to include every record.
(def ^:dynamic *location-record-filter* identity)

;;A working set of demand information, maps location->demand-data 
;;if the location exists in the demand-data
(def ^:dynamic *demand-map* nil)
;;demand names have [start...duration] in them, for now. this is a
;;little brittle but it works for us.
(defn get-demanddata! [v]  (get *demand-map* v))



;;A var to hold a set of deployments from the audit trail, 
;;keyed by [deploy-interval name], this lets us 
;;join the information from the deployments onto the sand-trends 
;;metatable, so we can have deployment context.  Alternately, 
;;we can just use the deployment 
(def ^:dynamic *deployment-record-filter* identity)
(def ^:dynamic *deployment-map* nil)

;;lets us map to where the unit's at.
(def ^:dynamic *deployment-name* (juxt :Unit :DeployInterval))

(def depfields (into #{} (map keyword (keys schemas/deprecordschema))))
(defn load-deployments [path]
  (->> (tbl/tabdelimited->table (slurp path) :schema schemas/deprecordschema)
       (tbl/table-records)
       (filter (fn [r] (*deployment-record-filter* r)))
       (reduce (fn [acc r]
                 (assoc acc (*deployment-name* r) r)) {} )))

;;function for determining how to split our trends if we decompose a 
;;sandtrends file.
(def ^:dynamic *split-key* :SRC)
(defn where-key [k f] (fn [r] (f (get r k))))  

(defmacro only-srcs [xs & expr]
  `(let [srcs# (set ~xs)
         filterf# (fn [v#] (contains? srcs# v#))]
     (binding [~'proc.core/*demand-trend-pre-filter*  filterf#
               ~'proc.core/*demand-trend-filter* (where-key :SRC filterf#)
               ~'proc.core/*location-record-filter* (where-key :SRC filterf#)
               ~'proc.core/*deployment-record-filter* (where-key :DemandType filterf#)]
       ~@expr)))

;;this is a hack for now...
(defn tabify [path]
  (let [res (-> (slurp path) 
                (clojure.string/replace   "," (str \tab))
                ;; (clojure.string/replace (str \newline) 
                ;;                         (str \tab \newline)
                ;;                         )
                )]
    (spit path res)))

(defn spit-lines
  "Opposite of slurp.  Opens f with writer, writes content, then
  closes f. Options passed to clojure.java.io/writer.  Assumes content is a sequence of strings to be 
  delimited by newline characters.  Streams over the seq and pushes each line to f."
  {:added "1.2"}
  [f entries & options]
  (with-open [^java.io.Writer w (apply clojure.java.io/writer f options)]
    (doseq [entry entries]
      (util/write! w (str entry \newline)))))

;;first thing to extract is the location information, by unit, by time.

;;One option is to extract the individual location histories of each
;;unit.
;;We know that the entityfrom is actually the unit.
;;so, group-by entity from will split out our location histories over
;;time.

;;convenient processing format, give each record a unique index.
;(def locs (map-indexed (fn [idx r] (assoc r :idx idx)) (tbl/table-records loctable)))

(defn name->unit [^String uname]
  (let [[idx src compo] (clojure.string/split  uname #"_")]
    {:name uname
     :unitid (Long/parseLong idx)
     :SRC  src
     :compo compo}))

(defn rconj [x coll] (conj coll x))

(defn locs->events [xs]
  (let [le (last xs)]
    (->> xs
         (partition 2 1)
         (mapv (fn [[l r]]
              (let [start (:T l)
                    end   (:T r)
                    duration (- end start)]
                (assoc l :start start :duration duration))))
         (rconj (assoc le :start (:T le) :duration 1)))))

(defn unit-locs [recs]
  (->>(group-by :EntityFrom recs)
      (map (fn [[u xs]]
                (let [t-entries (group-by :T (sort-by :idx 
                                                      (map #(select-keys % [:idx :T :EntityTo]) xs)))
                      es (->> (seq     t-entries)
                              (sort-by first)
                              (map     second)
                              (map (fn [es] (if (== (count es) 1)  (first es) ;no
                                        ;single change at that point
                                        ;in time.
                                                (last es))))
                              (locs->events))] 
                  [(name->unit u) es])))))

;;this is a hack...we need a way to be more data driven in the future.
(defn quick-category [loc]
  (case loc 
    ("Ready"  "Ready_Deployable" "Ready_NotDeployable")  "Ready"
    ("Recovery" "TransitionReady_NotDeployable"  "TransitionReady") "TransitionReady"
    "DeMobilization" "Prepare"       
    loc))
    
(defmacro get-else [m k else]
  `(if-let [res# (get ~m ~k)]
     res#
     ~else))

;;produce a seq of {name unitid src compo start duration location}
(defn loc-records [ulocs]  
  (let [inits (atom (transient {}))
        get-init (fn get-init [name start]
                    (if-let [res (get @inits [name start])]
                      res
                      (do (swap! inits assoc! [name start] start)
                          start)))]            
    (mapcat (fn [[{:keys [name unitid SRC compo] :as u}  es]]
              (map (fn [{:keys [start duration EntityTo] :as e}]
                     (let [ddata (get-demanddata! EntityTo)
                           init  (get-init name start)]
                       {:name name :unitid unitid :SRC SRC :compo compo :start start :duration duration 
                        :end (+ init duration) ;new
                        :location EntityTo
                        :quantity 1
                        :operation (get ddata :Operation EntityTo)
                        :category  (get-else ddata :Category (quick-category EntityTo))
                        :fill-type "Filled"})) es))  ulocs)))



;;This ties it all together and lets us build a location table from
;;the records.
(defn location-table [path]
  (->> (tbl/tabdelimited->table (slurp path) :schema schemas/locschema)
       (tbl/table-records)
       (map-indexed (fn [idx r] (assoc r :idx idx)))
       (unit-locs)
       (loc-records)
       (filter *location-record-filter*)
       (tbl/records->table)))

;;We also want to extract some metadata from the demandrecords.
;;They are typically stored in AUDIT_DemandRecords.txt
;;We can augment our demand metadata with this.
;;Demandnames are encoded [priority_vignette_src_[start..]]
(defn demand-key [dname]
  (let [[idx vig src startdur] (clojure.string/split  dname #"_")
        [start _] (clojure.edn/read-string 
                     (clojure.string/replace startdur "..." " "))]
    [vig src start]))

(defn demand-name [d]
  (clojure.string/join "_" [(:Priority d) (:Vignette d) (:SRC d) (str "[" (:StartDay d) "..." 
                                                                       (+ (:StartDay d) (:Duration d)) "]")]))
(defn load-demand-map [path]
  (->> (tbl/tabdelimited->table (slurp path) :schema (util/fit-schema schemas/drecordschema path))
       (tbl/table-records)
       (filter (fn [r] (and (:Enabled r) (*demand-trend-filter* r))))
       (reduce (fn [acc r]
                 (assoc acc (demand-name r) r)) {} )))
  
;;once we have the demandrecords, we'd "like" to slurp in the records
;;that are of interest, to augment our demand meta data.

;;Specifically, we want to append the demandnames to the
;;demandrecords. this is pretty simple...
;;we can slurp the demandrecords into a stream of records, traverse
;;it,  unpack the known demandnames

;;we need to define ranges for how
;;long things were at locations, i.e. we need to insert records.                 
                
;;locations are currently encoded in string form, the location name is
;;assumed to be unique.  So, we can parse the location name to get
;;some information out of the loc.

;;Given a set of demandtrends, we can compute a fill-function.
;;That is, a function that knows the fill/unfill status for every 
;;demand as a function of time.

;;We can do the same thing from a location-table.
;;The big idea is to have these functions built, or derived,
;;and then systematically choose points to sample.

;;So, using vector representations present in the table, we 
;;have discrete functions of time over which nothing changes.
;;So, the most useful datastructure here is to build up an
;;interval tree for each unit in supply (out of the location data), 
;;and an interval tree for each demand in the demand trends. 
;;The interval tree sparsely encodes the temporal data and 
;;allows us to sample discretely at points in time.


;;This allows us to find the sample closest in time to 
;;the desired sample.
(defn previous-sample [ts t] (first  (rsubseq ts <= t)))
(defn previous-entry [m k t]
  (let [es (get m k)]
    (previous-sample es t)))

;;We to sample exactly, else return nothing.
(defn sample  [ts t] (get ts t))

;;the idea is to traverse the demandtrends, and build up
;;a function that can sparsely sample a given trend at a point in
;;time.

;;For each demandname, we want to keep a sorted map of samples 
;;as soon as they show up. 
;;The map is the sample.

(defn sample-trends 
  ([xs trendkey samplekey samplefunc]  
     (persistent!
      (reduce (fn [acc sample]
                (let [trend (trendkey  sample)
                      t     (samplekey sample)]
                  (if-let [samples (get acc trend)]
                    (assoc! acc trend (assoc samples t (samplefunc sample)))
                    (let [samples (sorted-map)]
                      (assoc! acc trend (assoc samples t (samplefunc sample)))))))
              (transient {})
              xs)))
  ([xs trendkey samplekey] (sample-trends xs trendkey samplekey identity)))

(defn zero-trends 
  ([m tzero zerof] 
     (persistent! 
      (reduce-kv (fn [acc trend samples]
                   (assoc! acc trend
                           (assoc samples tzero (zerof (second (first samples))))))
                 (transient {}) 
                 m)))
  ([m] (zero-trends m 0 identity)))

;This one goes by demandname
(defn sample-demand-trends   [xs]     
  ;we want to add zeroed trends, so that we always have a demand.
  (-> (sample-trends xs #(get % :DemandName) #(get % :t))
      (zero-trends 0 (fn [r] (merge r {:t 0 
                                       :TotalRequired 0
                                       :TotalFilled 0
                                       :Overlapping 0 
                                       :Deployed 0
                                       :ACFilled 0 
                                       :RCFilled 0
                                       :NGFilled  0
                                       :GhostFilled 0	
                                       :OtherFilled 0
                                       :Unfilled 0})))))

;;unfilled-trends are stop-stop trends....
;;so, we have a series of demands that constitute concurrent 
;;fill histories, based on demandname.  From there, we create demand
;;profiles for each 
(defn sample-unfilled-trends   [xs]          
  (sample-trends xs #(get % :DemandName) #(get % :t)))

;This one goes by unitname
(defn sample-location-trends [xs] 
  (sample-trends xs #(get % :name) #(get % :start)))

;;a way to build a sampler out of cycles.  We can compute population 
;;BDR over time this way.  Note that it leaves out the last bit of
;;information,  since some units haven't finished their cycles yet.
(defn sample-cycle-trends [xs] 
  (sample-trends xs #(get % :UIC) #(get % :tstart) identity))

(defn dtrend-parser  [fields]
  (let [parsef  (parse/parsing-scheme schemas/dschema :default-parser  identity)
        rec     (fn [xs] (reduce-kv (fn [acc idx v] (assoc acc (nth fields idx) v)) {} xs))]
    (comp rec (parse/vec-parser fields parsef)))) 
  
(defn fill [r] (- (:TotalRequired r)  (:Deployed r)))
(defn unfilled? [r]  (pos? (fill r)))

(defn get-samples [samplers]  
  (for   [s samplers
          [trend ts] s]
    (keys ts)))
   

;;loke some, but returns the nth position of the element found, or nil
;;if no pred yields true.
(defn some-n [pred xs] 
  (let [found (atom nil)]
        (do (reduce (fn [acc x]
                       (if (pred x) 
                         (do (reset! found acc)
                             (reduced acc))
                         (inc acc)))
                     0 xs)
            @found)))
        
;;This probably won't work so hot on the real file....
;;note, we only "really" care about unfilled demandtrends....
;;and when that happens, we want to create n records for each 
;;unfilled (basically ghosts)
(defn get-unfilled-demandtrends [path]
  (let [fields       (util/get-headers path)
        src-field    (some-n #{:SRC} fields) ;this is baked in, may
                                        ;need more generality...
        parse-fields (dtrend-parser fields)
        tlast        (atom 0)
        unfilled     (atom (transient #{}))
        demandmeta   (atom {})]
    (with-open [rdr (clojure.java.io/reader path)]
      [(->> (line-seq rdr)
            (rest)
            (map    tbl/split-by-tab)
            (filter (fn [xs] (*demand-trend-pre-filter* (nth xs src-field))))
            (map     parse-fields)
            (filter  *demand-trend-filter*) ; moved up....
            (map    (fn [r] (if (> (:t r) @tlast) (do (reset! tlast (:t r)) r) r)))
            (map    (fn [r] (if (contains? @demandmeta (:DemandName r)) r
                                (do (swap! demandmeta 
                                           assoc  
                                           (:DemandName r) 
                                           {:DemandGroup (:DemandGroup r)
                                            :Vignette    (:Vignette r)
                                            :Required    (:TotalRequired r)
                                            :Operation   (:Operation r)
                                            :SRC         (:SRC r)}) 
                                    r))))
            (map-indexed (fn [idx r] 
                           (let [fl (fill r)
                                 _  (when (pos?  fl)
                                      (swap! unfilled conj! idx))]
                             (assoc r :Unfilled fl))))
            (tbl/records->table))
       @tlast  @demandmeta (persistent! @unfilled)])))


  
(defn unfilled-demands [dtrends]
  (tbl/select :from dtrends :where (fn [r] (pos? (:Unfilled r))))) 
  

(defn fill-profile [xs] 
  (spork.util.general/clumps (fn [r] (if (pos? (:Unfilled r)) (:Unfilled r) :filled)) xs))

(defn fill-seq [xs]
  (->> xs
       (map (fn [[s recs]]
              (let [ts (:t (first recs))
                    tf (:t (last  recs))]
                (assoc (first recs) :duration (- tf ts) :end tf))))))

;;given a table of demandtrends that contains unfilled records, 
;;we can determine periods of unfilled-ness, assuming we have 
;;the entire history of the deltas in the demandtrends.  We 
;;want derive a table of [demandname start stop qty] 
;;that contains distinct unfilled periods.
(defn unfilled-periods [dtrends] 
  (->> (for [[nm recs] (group-by :DemandName (tbl/table-records dtrends))]                
         (->> recs
              (fill-profile)
              (filter (fn [[s _]] (not (identical? s :filled))))
              (fill-seq)))
       (filter seq)
       (reduce concat)))

(defn fold-count [coll]     (r/fold + (fn ([acc n] (unchecked-inc acc)) ([] 0)) coll))
(defn fold-into-vector [r]  (r/fold (r/monoid into vector) conj r))

;;we want to combine samples from one or more samplers.
;;In this case, we're adding a new set of samples, derived from the
;;demand trends.  We know that we have comparitively sparse samples 
;;in the locations trends.  So, we can build a new sampler according 
;;to the demandtrends samples.

;;Alternately, we can just sample t = 1...n at daily intervals and 
;;pull out the quantities (that sounds palatable, possibly
;;unmanageable though).

;;The downside here is that we end up with fine-grained sampling, 
;;but we may not need that resolution.  We can always alter the 
;;sampling frequency too though....

;;So one idea is to sample from t = 1...some tmax for every member 
;;of the population in the locations.
;;Do the same for the demand trends.

(defn samples-at 
  ([m t intersectf]
     (reduce-kv (fn [acc u samples]
                  (conj acc [u (second (intersectf samples t))]))
                '() 
                m))
  ([m t] (samples-at m t previous-sample)))

;;note: there is another type of sampling, existence sampling.  We
;;only sample items that actually exist, so there is the possibility
;;of returning nil.

(defn supply-samples-at [m t]
  (r/map #(assoc % :start t) (r/map second (samples-at m t))))

;;THis only returns missed demand, we end up filtering out everything
;;else.  We could go a LOT faster if we simply excluded region where
;;there are no misses, although, ostensibly, we're fast enough....still.
(defn expand-unfilled [r t]
  (let [n     (:Unfilled r)
        qty   (if (pos? n) 1 0)
        loc   (:DemandName r)
        ddata (get-demanddata! loc) 
        operation  (get ddata :Operation)
        category   (get ddata :Category)]
    (loop [idx 0 
           acc []]
      (if (== idx n) acc
          (recur (unchecked-inc idx)
                 (conj acc
                       {:location  loc, 
                        :duration   0, 
                        :start t, 
                        :end   t ;new
                        :compo "Ghost", 
                        :SRC        (:SRC r)
                        :unitid     0, 
                        :name (str (:DemandName r) (str "_Unfilled_" (str idx "_" t)))
                        :operation operation
                        :category category                           
                        :quantity qty
                        :fill-type "Unmet"}))))))

(defmacro get-else [m k & e]
  `(if-let [res# (get ~m ~k)]
     res#
     ~@e))
;;missed demands are currently codified as instantaneous, do we want
;;this for missed deployments?
;;A missed deployment should look like a regular deployment sample: 
;;joining a unit-loc record and a deployment record.  In this case, 
;;we're faking the deployment (and creating the unit loc record based
;;off the demand record).
(defn expand-misses [r t idx]
  (let [n     (:Unfilled r)
        qty   (if (pos? n) n 0)
        loc   (:DemandName r)
        ddata (get-demanddata! loc) 
        operation  (get-else ddata :Operation (throw (Exception. (str [:no-ddata loc])))) ;ugh, do we have this information elsewhere?
        category   (get-else ddata :Category)  ;ugh, do we have this elsewhere?
        ]
    {:location  loc, 
     :duration   (:duration r), 
     :start t, 
     :end   (:end r) ;new ;these records are not ephemeral...can we determine
              ;a duration of unfilled?  We can....based on the demandtrends...
     :compo "Ghost", 
     :SRC        (:SRC r)
     :unitid     0, 
     :name (str (:DemandName r) (str "_Unfilled_" (str idx "_" t)))
     :operation operation
     :category category                           
     :quantity n
     :fill-type "Unmet"                           
     }))

;;This is really only returning missed demands, which is perfect.
(defn demand-fill-samples-at [m t]
  (->> (samples-at m t)
       (r/filter identity) ;ensure we have samples...
       (r/map second)
       (r/mapcat #(expand-unfilled % t))))

;;expanding misses is simpler
(defn demand-misses-at [m t]
  (->> (samples-at m t)
       (r/filter (fn [xs] (and (identity xs) (identity (second xs))))) ;ensure we have samples...
       (r/map-indexed 
        (fn [idx r]
          (expand-misses (second r) t idx)))))

(defn sample-sand-trends [samples locsampler demsampler]
  (->> samples 
       (r/mapcat (fn [t]
                   (r/mapcat (fn [f] (f t))
                             [#(supply-samples-at locsampler %)
                              #(demand-fill-samples-at demsampler %)
                              ])))))

(defn missed? [r]  (= (:compo r) "Ghost"))

;;Trying to compress the sampling so we can postproc faster.
;;If we include a deltat in the time, we can always upsample (drop) or 
;;downsample (expand) as needed.  Will not add samples earlier then 
;;the observed min, and later than the observerd max.  To accomplish
;;that, add your own min/max as an entry in the colls.
(defn minimum-samples 
  ([pad colls]
     (let [min (atom (first (first colls)))
           max (atom @min)]
       (->> (r/mapcat identity colls)
            (reduce (fn [acc x]
                      (do (when (< x @min) (reset! min x))
                          (when (> x @max) (reset! max x))
                          (-> acc
                              (conj! x)
                              (conj! (+ x pad))
                              (conj! (- x pad)))))
                    (transient #{}))
            (persistent!)          
            (filter (fn [x]
                      (and (>= x @min)
                           (<= x @max))))
            (sort))))
  ([colls] (minimum-samples 1 colls)))

(defn smooth-samples 
  ([pad colls]
     (let [ts  (minimum-samples pad colls)]
       (minimum-samples pad [ts])))
  ([colls] (smooth-samples 1 colls)))
       
;;#Tom Note# - formalize a sampling API, to include a time-series 
;;with start,  deltat (time since last sample). This will allow us to 
;;more cleanly work with temporal, time-weighted data.
           

;;Takes an unfilled deployment record and turns it into a missed
;;deployment
;;We only need a quantity....no need to break up the records into 
;;individual units (ala sandtrends).
(defn missed-deployment-record 
  [{:keys [location duration start end compo SRC unitid name operation 
           category quantity fill-type]} ]
  (let [ddata (get-demanddata! location)]
  {:category category
   :SRC SRC
   :name name
   :operation operation
   :start start
   :duration duration
   :fill-type fill-type
   :unitid    unitid
   :quantity quantity
   :end end 
   :location location
   :compo compo
   :Unit  name
   :DemandGroup (:DemandGroup ddata)
   :FillType    "Sub"
   :FollowOn    false  
   :Component   "Ghost"
   :DeploymentID -1
   :DwellYearsBeforeDeploy 0
   :DeployDate    0
   :FollowOnCount 0
   :AtomicPolicy nil
   :Category     category
   :DeployInterval start
   :FillPath       "Ghost"
   :Period        nil
   :Demand        location
   :PathLength    1
   :OITitle       (:OITitle ddata)
   :BogBudget     0
   :CycleTime     0 
   :DeploymentCount 1
   :DemandType      SRC
   :FillCount     -1
   :Location      "Available"
   :DwellBeforeDeploy 0
   :Policy        "Ghost"}))

;;Given a demandtrends, extract only the unfilled periods, and for
;;each of those records, convert them to unfilled units, then to
;;deployments.
;;These records should be identical to our deployment records above, 
;;since they are merely a join between a location record and a
;;deployment (in this case, a fake deployment).  We should be able 
;;to simply append these records to the merged deployment records and
;;have the fill sampler like as normal.
(defn derive-missed-deployments [dtrends demand-map] 
  (binding [*demand-map* demand-map]
    (->> (unfilled-periods dtrends) 
         (map-indexed (fn [idx r]
                        (expand-misses r (:t r) idx)))
         (map missed-deployment-record)
         (vec))))


;;Altering this to produce a table of "fills", i.e. location changes
;;that were filling a demand.  Right now, there's too much foreign 
;;context for us to bring in usably, so a better strategy is to 
;;provide a .fills.txt file.  We'll have a .sand.text and a
;;.fills.text  as extensions.  From there, we know we have a
;;like-named file called blah.sand.text and blah.fills.text 
;;We can sample from fills at the same rate we sample from sandtrends.
;;Ideally, we just use the minimum sampling rate and expand from
;;there...much faster, less i/o required too.

;;basically, we're joining based on name and time...
;;if the depmap has the name, and the depmap has the time, we 
;;assoc the record's fields

(defn merge-deployment-data 
  ([depmap loctable]
     (let [
        ;; [units times] (util/distinct-zipped (keys depmap)) 
        ;; deployers (tbl/select  :from loctable  ;only select filling records.
        ;;                        :where (fn [r] (and (units (:name r))
        ;;                                            (times (:start r)))))                           
           deployers (tbl/select  :from loctable  ;only select filling records.
                                  :where (fn [r] (contains? depmap [(:name r)
                                                                    (:start r)])))                           
           excluded-keys #{} ;#{:DemandGroup :Unit}
           new-keys     (vec (filter (complement excluded-keys)
                                     (keys (second (first depmap)))))
           key->idx   (reduce-kv (fn [acc idx k] (assoc acc k idx)) {} new-keys)
           new-fields (atom  (transient (mapv (fn [_] (transient [])) new-keys)))
           empty-rec  (zipmap new-keys (repeat nil))]
       (->>    (tbl/conj-fields 
                (->> (tbl/select :fields [:name :start] :from deployers)
                     (reduce (fn [flds {:keys [name start]}]
                               (let [deprec (get depmap [name start] empty-rec)]
                                 (reduce (fn [acc k] 
                                           (let [idx  (get key->idx k)
                                                 v    (nth @acc idx)]
                                             (do (swap! acc assoc! idx (conj! v (get deprec k)))
                                                 acc)))
                                         flds 
                                         new-keys)))  new-fields)
                     (deref)
                     (persistent!)
                     (mapv persistent!)         
                     (map-indexed (fn [idx xs] [(nth new-keys idx) xs])))
                deployers)
               (tbl/rename-fields {:UnitType :SRC})))))

(defn append-sampling-info [deployment-data]
  (let [n (tbl/record-count deployment-data)]
    (->> deployment-data 
         (tbl/conj-field [:sampled     (vec (take n (repeat false)))])
         (tbl/conj-field [:dwell-plot? (vec (take n (repeat false)))]))))

;;deployment data and missed deployments are identical structurally, 
;;so we can join them in a table.  Optionally, we can do a quick merge 
;;too...

;;we have a couple of options, collect the columns and conj them a row
;;at a time....
(defn concat-records [tab xs]
  (let [fld->idx (into {} (map-indexed (fn [idx f] [f idx]) 
                                       (tbl/table-fields tab)))]
    (->> xs
         (reduce (fn [cols r]
                   (reduce-kv (fn [acc k v]
                                (let [idx (fld->idx k)
                                      c   (nth cols idx k)]
                                  (assoc! acc idx (conj! c v))))
                              cols r))
                 (transient (mapv transient (tbl/table-columns tab))))
         (persistent!)
         (mapv persistent!)
         (tbl/make-table (tbl/table-fields tab)))))

;;sampling misses from a demand-fill-sampler (Which gives us unfilled
;;demands by design) is simple....we join the demand on-demand.
;;Alternatively, where there are missed demands, we can 
;;join the demand records using missed-deployment-record.

;;a fill sampler...
(defn sample-fills [filltable depmap] 
  (sample-location-trends   (if (tbl/tabular? filltable)  
                              (tbl/table-records filltable)
                              filltable)))

;;allows us to sample only when the time intersects....
;;this is hackish.
(defn start-stop-sample [start stop tmap t]
  (->> (samples-at tmap t)
       (r/filter (fn [tr-sample]
                   (when-let [x (second tr-sample)]
                     (and (<= (start x) t) (>= (stop x) t)))))))

(defn fill-samples-at [m t] 
  (r/map (fn [{:keys [DeployInterval FollowOn DemandGroup] :as r}] 
           (let [sampled (not (== DeployInterval t))]
             (assoc r 
               :start t 
               :sampled sampled
               :dwell-plot? (and (not sampled)
                                 (not FollowOn)
                                 (not (= DemandGroup "NotUtilized"))))))
         (r/filter identity
                   (r/map  second (start-stop-sample :start :end m  t)))))

(defn sample-fill-trends [samples fillsampler]
  (->> samples 
       (r/mapcat (fn [t]
                   (fill-samples-at fillsampler t)))))

(defn group-table [keyf tab]
  (for [[g xs] (group-by keyf (tbl/table-records tab))]
    [g (tbl/records->table xs)]))

(def rs (atom nil))

;;Assuming we have demand records, we know which records are unfilled.
(defn dump-fills-with-ghosts
  [rootpath outname splitkey loctable deploymap demandtrends demandmap]
     (let [outpath    (str rootpath outname)
           lazy-headers (atom nil) 
           ms         (util/mstream rootpath outname lazy-headers)
           _          (io/hock outpath "") ;creates folder structure and
                                        ;whatnot
           _          (println [:emitting :fills outpath])    
           _          (println [:preparing :samples])
           _          (println [:computing :missed-deployments])
           missed-map (into {} (group-by splitkey (derive-missed-deployments 
                                                   demandtrends demandmap)))
           ]
       
       (with-open [stream ms]
         (doseq [[k recs]    (group-by splitkey (tbl/table-records loctable))]
           (let [                 _ (do (println k) (reset! rs recs))
                                   
                 filltable   (merge-deployment-data deploymap (tbl/records->table recs))

                 filltable   (if-let [misses (get missed-map k)]
                               (concat-records filltable misses)
                               filltable)
                 filltable   (append-sampling-info filltable) 
                 latest-deployment-sample (reduce max 0 (r/map :end  filltable))
                 fillsampler (sample-fills filltable deploymap)
                 ts          (minimum-samples  (conj (get-samples [fillsampler]) [latest-deployment-sample]))
                 _ (println [:ld latest-deployment-sample
                             :latest (last ts) 
                             :sampled (reduce max (mapcat vec  (get-samples [fillsampler])))])
                 headers     (tbl/table-fields filltable)
                 all-headers (conj headers :deltat)              
                 _           (reset! lazy-headers  (str (clojure.string/join \tab all-headers)))
                 t          (atom (first ts))]          
             (reduce (fn [acc r] 
                       (let [w     (util/get-writer! ms (splitkey r))
                             _ (when (nil? (splitkey r))
                                        ;(throw (Exception. (str r)))
                              
                                 )
                             tnow  (get r :start)
                          delta (if (== tnow @t) acc 
                                    (let [res (- tnow @t)
                                          _   (reset! t tnow)]
                                      res))]
                         (do (doseq [h headers]
                               (util/write! w (str (get r h) \tab)))                          
                             (util/write! w (str delta))
                          (util/new-line! w)
                          delta)))
                     0
                     (sample-fill-trends ts fillsampler)))))))

;;revised version of dump-sandtrends, using multstreams to 
;;create multiple files simultaneously, save processing time.
(defn dump-sandtrends 
  [& {:keys [rootpath locpath dpath outname splitkey samples drecpath deploypath fillpath]
      :or {locpath locs 
           dpath dtrends
           outname "sandtrends.ms.edn"
           splitkey :SRC}}]
  (let [_ (println [:reading :demandrecords drecpath])
        fillpath (or (when fillpath fillpath) (str rootpath "/fills/"))
        dmap (when drecpath (load-demand-map drecpath))
        deploymap (when deploypath (load-deployments deploypath))]
    (binding [*demand-map* dmap]
      (let [_          (println [:reading :locationtable locpath])
            loctable   (location-table locpath) 
            _          (println [:reading :demandtrends dpath])
            res        (get-unfilled-demandtrends dpath) ;appends unfilled field to trends.
            [dtrends tmax demandmeta] res
            _          (println [:sampling :locations])
            locsamples (sample-location-trends (tbl/table-records loctable))            
            _          (when deploymap 
                         (do (println [:dumping :fills])
;                             (dump-fills fillpath "fills.ms.edn" splitkey loctable deploymap))
                             (dump-fills-with-ghosts
                              fillpath "fills.ms.edn" splitkey loctable deploymap dtrends dmap))
                         )
            _           (println [:sampling :demandtrends])
            dsamples    (sample-demand-trends (tbl/table-records dtrends))
            headers     (into (tbl/table-fields loctable) [:DemandGroup :Vignette])
            all-headers (conj headers :deltat)
            get-group   (memoize (fn [^String nm] 
                                  (if-let [res  (get demandmeta nm)]
                                    (get res :DemandGroup)
                                    (first (clojure.string/split nm #"_")))))
            try-get    (fn [r  k] 
                         (case k 
                           :DemandGroup  (get-group (:location r))
                           :Vignette     (get (get demandmeta (:location r)) k nil)
                           (if-let  [res (get r k)] res 
                             (when  (and (not (contains? r k)) 
                                         (not (depfields k)))
                               (throw (Exception. (str "can't find" [k :in r])))))))
            ms      (util/mstream rootpath outname (str (clojure.string/join \tab all-headers)))
            outpath (str rootpath outname)
            _       (io/hock outpath "") ;creates folder structure and whatnot
            _       (println [:emitting :sandtrends outpath])            
            samples (cond   (identical? samples :min) 
                                (minimum-samples (get-samples [locsamples dsamples]))
                            (not (nil? samples))   samples
                            :else (r/range 1 (inc tmax)))
            t       (atom (first samples))]
        (with-open [stream ms]
          (reduce (fn [acc r] 
                    (let [w     (util/get-writer! ms (splitkey r))
                          tnow  (get r :start)
                          delta (if (== tnow @t) acc 
                                    (let [res (- tnow @t)
                                          _   (reset! t tnow)]
                                      res))]
                      (do (doseq [h headers]
                            (util/write! w (str (try-get r h) \tab)))
                          (util/write! w (str delta))
                          (util/new-line! w)
                          delta)))
                  1
                  (sample-sand-trends samples locsamples dsamples)))))))

(defn xs->src [^String ln]  (nth (tbl/split-by-tab ln) 8))

;;this actually finishes, surprisingly, but it's pretty huge.
;;we're not actually looking at any of these guys either in the
;;aggregate.  Dwell stats and misses tell us most of what we need to 
;;know.
(defn sandtrends-from [root & {:keys [splitkey samples] :or {splitkey *split-key*}}]                                                             
  (let [sandroot (str root "/sand/")]
    (dump-sandtrends :locpath (str root "/locations.txt")
                     :dpath   (str root "/DemandTrends.txt")
                     :deploypath (str root "/AUDIT_Deployments.txt")
                     :outpath (str root "/sandtrends.txt")
                     :fillpath (str root "/fills/")
                     :rootpath sandroot
                     :splitkey splitkey 
                     :samples  samples
                     :drecpath (str root "/AUDIT_DemandRecords.txt"))))

(defn fills-from [root & {:keys [splitkey samples] :or {splitkey *split-key*}}]                                                             
  (let [fillroot (str root "/fills/")]
    (dump-fills-with-ghosts
             fillroot "fills.ms.edn" splitkey 
             (do (println [:reading-locations]) 
                 (location-table (str root "/locations.txt")))
             (do (println [:reading-deployments])
                 (load-deployments (str root "/AUDIT_Deployments.txt")))
             (do (println [:reading-demandtrends]) 
                 (first (get-unfilled-demandtrends (str root "/DemandTrends.txt"))))
             (do (println [:reading-demand-map]) 
                 (load-demand-map (str root "/AUDIT_DemandRecords.txt"))))
             ))

;;We've got this big hunk of data to process, and we'd like to split
;;it up into multiple files.  It'd be nice if there was a simple
;;abstraction for this....
;;We can use it to split the demandtrends into multiple folders and 
;;analyze them independently.  This scales better too fyi (less IO
;;bound).

;;We also want to support querying....so, later on we can define
;;abstract queries over the trends and collate the data...

(defn demand-table [dpath]
 (->> (tbl/tabdelimited->table (slurp dpath) 
          :parsemode :noscience :schema {:StartDay :int :Duration :int :Quantity :int :Priority :int})
       (tbl/select :where (fn [r] (= (:Enabled r) "True")) :from)))  

(defn where-src [src tab] (tbl/select :from tab :where (fn [r] (= (:SRC r) src))))
(defn demand-samples [xs] (sample-trends (tbl/table-records xs) :SRC :StartDay :Quantity))



;;not working...
(defn demand-profile [drecs]
  (sample-trends (demand-samples drecs) (fn [[t xs]] (:SRC (first (:actives xs)))) first 
                 (fn [[t xs]] (reduce + (map :Quantity (:actives xs))))))
;testing mstream
(comment        
  (def root "C:/Users/thomas.spoon/Documents/MarathonHacks/"))           
;;given a sandtrends file, we can munge a bunch of different metrics
;;from it.

;;one is the patch-chart.
;;This is just a quarterly alignment of readiness, basically the
;;category in the sandtrends data.

;;We want this for each SRC of interest, for each case.

;;Another is readiness over time, that is c1/c2 readiness over time
;;We want this for each src of interest, for each case.

;;Dwell Before Deploy dotplots are nice as well.
;;SRC/Case 

;;BOG:Dwell / time nice too...
;;Time-series by cycles.
(defn sample-cycles [rootpath] 
  (let [bdkey      (keyword "BDR 1:X")
        cycs       (tbl/rename-fields {bdkey :BDRatio} 
                                      (tbl/tabdelimited->table (slurp (str rootpath "/cycles.txt")) 
                                                               :schema schemas/cycleschema))
        tmax       (:tfinal (tbl/last-record cycs))
        sampler    (sample-cycle-trends (tbl/table-records cycs))
        cycle-samples-at    (fn [m t] (r/map #(assoc % :tstart t) (r/map second (samples-at m t))))
        outpath    (str rootpath "/bdrtime.txt")
        headers    [:tstart :Component :SRC :Period :UIC :BDRatio :Dwell :BOG]]
    (with-open [w (clojure.java.io/writer outpath)]
      (do (util/write! w  (clojure.string/join \tab headers))
          (util/new-line! w)
          (reduce (fn [acc r] 
                    (doseq [h headers]
                      (util/write! w (str (get r h) \tab)))
                    (util/new-line! w))
                  nil
                  (r/mapcat (fn [t] (cycle-samples-at sampler t))  (r/map #(* % 30) (r/range 0 (/ (inc tmax) 30)))))))))

(def interests {:BCT    ["BCT"  ["47112R000"   "77302R500"   "77302R600" "87312R000"]]
                :DIV    ["DIV"  ["87000R000"   "87000R100"]]
                :CAB    ["CAB"  ["01302R200"]] 
                :GSAB   ["GSAB" ["01226R100"]]
                :CSSB   ["CSSB" ["63426R000"]]
                :ATTACK ["ATTACK BN" ["01285R100"]]
                :ENG    ["Engineers" ["05418R000" "05417R000"]]
                :SAPPER ["SAPPER" ["05330R100"]]
                :MP     ["MP" ["19477R000"]]
                :TRUCK  ["TRUCKS" ["55727R100"
                                   "55727R100"
                                   "55727R200"
                                   "55727R300"
                                   "55728R100"
                                   "55728R200"
                                   "55728R300"
                                   "55779R000"]]
                :CA    ["Civil Affairs" ["41750G000" "41750G100"]]
                :All ["All SRCs" ["55727R300" "01226R100" "63426R000" "05418R000" "41750G100" "55728R100" 
                                  "01302R200" "01285R100" "19477R000" "47112R000" "55779R000" "55728R300" 
                                  "55727R100" "87000R000" "55728R200" "05330R100" "05417R000" "77302R500" 
                                  "55727R200" "87000R100" "77302R600" "87312R000" "41750G000"]]})

(def surges {:PreS ["Pre-Surge" ["PreSurge"]]
             :Surge ["Surge" ["Surge1"]]})

(defn interest->srcs [int]  (second (get interests int)))
(defn by-interest [ints]
  (let [pairs (map #(get interests %) ints)
        memb->int    (reduce (fn [acc [k members]]
                               (reduce (fn [inner m]
                                         (assoc inner m k)) acc members))
                             {} pairs)]
    (fn [r] (get memb->int (:SRC r) (:SRC r)))))

(defmacro only-by-interest [ints & expr] 
  `(let [srcs# (set (mapcat (fn [[k# [inter# srcs#]]]  srcs#) (select-keys interests ~ints)))]
    (only-srcs srcs#
               (binding [*split-key* (by-interest ~ints)]
                 ~@expr))))

(defn group-deployments [ds src phase]
  (let [srcs (if (vector? src) 
                           (set (second src))
                           #{src})
        pred (fn [r] (and (srcs (:DemandType r))
                          (zero? (:FollowOnCount r))
                          (not= (:Demand r) "NotUtilized")
                          (if phase
                            (= (:Period r) phase)
                            true)))]
    ($group-by [:Component] ($where pred ds))))
;;Note: we need to standardize the plotting colors for these guys,
;;maybe define a custom theme for the series?
;;plotting
(defn view-deployments [ds src]  
  (let [ds     (util/as-dataset ds)
        title  (if (vector? src) (first src) src)
        groups (group-deployments ds src)
        [series d] (first groups)
        plt (scatter-plot :DeployInterval :DwellYearsBeforeDeploy :series-label (:Component series) :legend true :data d 
                          :title (str title " Dwell Before Deployment")
                          :x-label "Time (days)"
                          :y-label "Dwell (years)")]
    (doseq [[series d] (rest groups)]
      (add-points plt :DeployInterval :DwellYearsBeforeDeploy :series-label (:Component series) :legend true :data d))
    (view plt)))

(defn set-xticks [plot tick] 
  (.setTickUnit (.getDomainAxis (.getPlot plot)) 
                (org.jfree.chart.axis.NumberTickUnit. tick)))

(defn ys [xys] (map second xys))
(defn xs [xys] (map first xys))

(defn all-bounds [chrt]
  (let [plt    (.getXYPlot chrt)
        bound  (.getDatasetCount plt)
        xmin (atom 0)
        xmax (atom 0)]
  (doseq [ds (map #(.getDataset plt %) (range bound))]
    (reduce (fn [_ [nm s]]
              (do (swap! xmin min (.getMinX s))
                  (swap! xmax max (.getMaxX s))))
            nil
             (stacked/series-seq ds)))
  [@xmin @xmax]))

;;If we have an xyplot, it's stored in multiple datasets vs. 
;;a single datatable.
(defn add-trend-lines! 
  ([chrt aggfn]  
     (let [bnds (all-bounds chrt) ;(stacked/get-bounds chrt)
           plt  (.getXYPlot chrt)
           bound    (.getDatasetCount plt)]
       (doseq [ds (map #(.getDataset plt %) (range bound))
               [idx [nm ^XYSeries ser]] (map-indexed vector  (stacked/series-seq ds))]
          (let [xys  (stacked/xycoords ser)
                y    (aggfn xys)
                xmax (reduce max (xs xys))]
            (do (add-lines chrt bnds [y y] :series-label (str nm "Avg"))
                ;not currently working....
                ;(set-stroke chrt :data ds :series (inc idx) :width 4 :dash 10) 
            ))))
     chrt)
  ([chrt] (add-trend-lines! chrt (comp incanter.stats/mean ys))))

(def default-deploy-colors 
  {"AC"    (stacked/faded :blue 50)
   "NG"    (stacked/faded :red 50)
   "RC"    (stacked/faded :green 50)
   "ACAvg" :blue 
   "NGAvg" :red
   "RCAvg" :green
   "USARAvg" :green
   "USAR"  (stacked/faded :green 50)
   "Ghost" (stacked/faded :grey 50)})

(defn deployment-plot [ds src phase & {:keys [colors tickwidth] :or {colors  default-deploy-colors tickwidth 365} :as opts}]
  (let [ds     (util/as-dataset ds)
        title  (if (vector? src) (first src) src)
        groups (group-deployments ds src phase)
        [series d] (first groups)
        plt        (scatter-plot :DeployInterval :DwellYearsBeforeDeploy :series-label (:Component series) :legend true :data d 
                                 :title (str title " " phase " Dwell Before Deployment")
                                 :x-label "Time (days)"
                                 :y-label "Dwell (years)")]
    (doseq [[series d] (rest groups)]
      (add-points plt :DeployInterval :DwellYearsBeforeDeploy :series-label (:Component series) :legend true :data d))
    (set-xticks plt tickwidth)  
    (add-trend-lines! plt)
    (stacked/set-colors  plt colors)
    plt))


(def category-colors 
  {"Mission"              java.awt.Color/blue 
   "Committed"            java.awt.Color/orange
   "Ready"                java.awt.Color/green
   "TransitionReady"     (.brighter java.awt.Color/green)
   "Prepare"              java.awt.Color/yellow})     


(defn hide-labels [plot]
  (.setVisible (.getDomainAxis (.getCategoryPlot plot)) false))
       
;;Patch Charts / Chunk Visualization.
  
;;we want to process a sandtrends file and compute the quarterly
;;metrics from it.
;;Basically, sample the trends where t is some mode of 90. 
(defn sample-every [freq ds]
  (assert (number? freq))
  (->> ds 
       ($where ($fn [start] (zero? (mod start freq))))
       (add-derived-column :Quarter [:start] (fn [t] (quot  t freq)))))

(defn non-ghosts [ds] ($where ($fn [compo] (not= compo "Ghost")) ds))

;;this is more or less a pivot table.
(defn table-by [rowkey colkey valuef recs]
  (let [rows (atom #{})
        cols (atom #{})        
        add-rowcol! (fn [r c]
                      (do (swap! rows conj r)
                          (swap! cols conj c)))
        rowcols   (fn [recs]
                    {:rows @rows :cols @cols
                      :rowcols recs})]
    (->>  (if (dataset? recs) (:rows recs) recs)
          (reduce (fn [rowcols r]
                    (let [row (rowkey r)
                          col (colkey r)
                          v  (valuef r)]
                      (do (add-rowcol! row col)
                          (conj rowcols 
                                {:row row 
                                 :col col 
                                 :value v}))))
                  [])
          (rowcols))))

(defn rowcols->vecs [{:keys [rows cols rowcols]}]
  (for [[row rs] (group-by :row rowcols)]
    [row (persistent!  (reduce (fn [acc r] (conj! acc  (:value r)))
                               (transient   [])
                               (sort-by :col rs)))]))

(defn sand-chunks [ds & {:keys [sortkey] :or {sortkey identity}}]
  (let [row-order (atom [])
        basedata     (->> ds 
                          (sample-every 90) 
                          (non-ghosts)
                          ($where ($fn [Quarter] (<= Quarter 24))))
        unit-map  (reduce (fn [acc r] (assoc acc (:unitid r) r)) {} (:rows basedata))
        sortkey    (fn [uid] (let [info  (get unit-map uid)]
                               [(:compo info) (:SRC info) uid]))                               
        compo-data ($group-by [:compo :src] basedata)]    
    (->> basedata
         (table-by  :unitid  :Quarter (fn [r] [(:category r) (:operation r)]))
         (rowcols->vecs)
         (sort-by (comp sortkey first))
         (reduce (fn [acc [r xs]]                   
                   (do (swap! row-order conj r)
                       (conj acc xs))) [])
         ((fn [r] (with-meta r {:row-order @row-order}))))))

(defn chunk-table [basedata sortkey]
    (let [row-order (atom [])]
      (->> basedata
           (table-by  :unitid  :Quarter (fn [r] [(:category r) (:operation r)]))
           (rowcols->vecs)
           (sort-by (comp sortkey first))
           (reduce (fn [acc [r xs]]                   
                     (do (swap! row-order conj r)
                         (conj acc xs))) [])
           ((fn [r] (with-meta r {:row-order @row-order}))))))

(def ^:dynamic *table-rate* 91)

(defn sand-tables [ds]
    (let [basedata     (->> ds 
                            (sample-every *table-rate*) 
                            (non-ghosts)
                            ($where ($fn [Quarter] (<= Quarter 24))))]
      (for [[k ds] (sort-by (comp vec vals first) ($group-by [:compo :SRC] basedata))]
        (let [unit-map  (reduce (fn [acc r] (assoc acc (:unitid r) r)) {} (:rows ds))
              sortkey   (fn [uid] (let [info  (get unit-map uid)]
                                     [(:compo info) (:SRC info) uid]))
              ctbl      (chunk-table ds sortkey)
              row-order (get (meta ctbl) :row-order)
              names     (map #(:name (get unit-map %))  row-order)]
          [k names ctbl]))))

;; (defn prepend-values [names chunks]
;;   (vec (map-indexed (fn [row cols]
;;                       (into [(nth names row)] cols))
;;                     chunks)))
;; (defn chunk-headers [rowcols]
;;   (let [row (first rowcols)
;;         cols (count r)]
;;     (shelf 
  

(defn render-sand-tables [ds]
  (let [sts (sand-tables (util/as-dataset ds))]    
    (sketch/delineate
     (into 
      [
       ;(chunk-headers (nth 2 (first sts)))
       ]
      (for [[{:keys [SRC compo]}  labels chunks] sts]        
        (let [hist   (patches/sketch-history chunks)
              height (:height (.shape-bounds hist))]
          (sketch/beside (sketch/->labeled-box (str [SRC compo]) :black :light-gray 0 0 114 height)
                         hist)))))))
(comment 
  (def bcttrends "C:/Users/thomas.spoon/Documents/SRMPreGame/runs/revisedfa_surge3/sand/BCT.txt")
  (def bct-ds    (util/as-dataset bcttrends))
)

;;the problem now is this...
;;when we have something, committed, committed, committed, prepare, prepare, 
;;we really should have transition-committed, committed, committed,
;;transition-prepare, prepare, prepare....
              
(defn ^ChartPanel chart! [ ch]  
  (ChartPanel. ch true true true true true))
                  
;;N Charts....
(defn quad-chart [& [l r bl br & rest]]  
  (swing/display (swing/empty-frame)
                        (swing/stack
                         (swing/shelf (chart! l )  (chart! r))
                         (swing/shelf (chart! bl) (chart! br)))))

(defn paired-chart [& [top bottom & rest]]  
  (swing/display (swing/empty-frame)
                        (swing/stack
                           (chart! top) 
                           (chart! bottom))))
;;;;________Craig used:

(defn dwell-over-fill [root src subs phase]
  (let [path (str root "fills/" (first src) ".txt")]
    (if (spork.util.io/fexists? path)
      (paired-chart (deployment-plot  (str root "AUDIT_Deployments.txt") src phase) 
                    (stacked/as-chart (stacked/fill-data (util/as-dataset path) phase subs)
                                      {:title "Fill"
                                       :tickwidth 365}))
      (println [:path path "Does Not Exist!" :ignoring src]))))

(def somephases ["PreSurge" "Surge1" "Surge2" "BetweenSurge" "PostSurge"])
  
(defn do-charts-from 
  ([root] (do-charts-from root [:TRUCK :ENG :CSSB :DIV :GSAB :SAPPER :MP :CAB :ATTACK :BCT :CA]))
  ([root xs & {:keys [subs phases] :or {subs false phases [nil]}}]             
    
    (only-by-interest xs
                      
                      (do (doseq [int xs
                                  phase phases]
                            (dwell-over-fill root (get interests int) subs phase))))))

;;;;_____________


;; (defn set-copyable [frm]
;;   (let [im (.getInputMap (.getContentPane frm))
        
  
(comment 
(def demands (demand-table "C:/Users/thomas.spoon/Documents/SRMPreGame/runs/fa/AUDIT_DemandRecords.txt"))
(def gsab    (where-src  "01205R600" demands))
(def ts      (temp/temporal-profile (tbl/table-records gsab) :start-func :StartDay :duration-func :Duration))
)  
  
;testing 
(comment 

(def case34 "C:/Users/thomas.spoon/Documents/TAA 18-22/derived data/runs/19 Mar/980/Case31_4/")
(only-by-interest [:BCT] 
                  (def locs   (location-table   (str case34 "Locations.txt")))
                  (def deps   (load-deployments (str case34 "AUDIT_Deployments.txt")))
                  (def md     (merge-deployment-data deps locs))                  
                  (def bctds  (first (get-unfilled-demandtrends (str case34 "DemandTrends.txt"))))
                  (def dm     (load-demand-map (str case34 "AUDIT_DemandRecords.txt")))
                  (def missed (derive-missed-deployments bctds dm)))

)
