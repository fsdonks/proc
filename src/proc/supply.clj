;;analyze the supply input before a Marathon run or the location of units at a point in time from Marathon data.
(ns proc.supply
  (:require [proc.stacked :as stacked]
            [spork.util.table :as tbl]
            [proc.schemas :as schemas]
            [proc.util :as util]
            [clojure.pprint :refer [cl-format]]
            [spork.util.general :refer [line-reducer]]
            [spork.util.excel.core :as xl]
            [spork.util.io :as io]
            [clojure.string :as str]
            [clojure.java.io :as java.io])
  (:use [proc.core]))

;;Generic
(defn units-locs-at
  "Given a time, t, and the root directory to the marathon run, this function will
  return the location of all units as records at time t. Can also provide a
  substring of the unit names you want returned as :unitpart."
  [time root & {:keys [unitpart] :or {unitpart ""}}]
  (let [samples (-> (tbl/table-records (tbl/tabdelimited->table (slurp (str root "locations.txt"))))
                  (sample-trends #(get % :EntityFrom) #(get % :T))
                  (samples-at time))]
    (filter (fn [r] (.contains (:EntityFrom r) unitpart)) (map second samples))))

(def supply-order
  [:Type :Enabled :Quantity :SRC :Component :OITitle :Name
   :Behavior :CycleTime :Policy :Tags :Spawntime :Location :Position])

(def key-fields [:Type :SRC :Component :Name :Behavior :CycleTime :Policy
                 :Tags :Spawntime :Location :Position])

(defn merged-quantities
  "Given records of Marathon AUDIT_SupplyRecords and if there are duplicates (same
  compo and SRC) in the records, this is meant to merge the quantities into one
  record and return a new sequence of supply records. The OI title of the record
  will be the last one found"
  [recs]
    (->> (group-by #(map % key-fields) recs)
         (reduce-kv (fn [a k v]
                      (conj a (assoc (last v)
                                     :Quantity (reduce + (map :Quantity v))))) [])))
;;Maybe Unnecessary....Generic?
(defn merge-quants
  "Given the path to Marathon supply records and if there are duplicates (same
   compo and SRC) in supply records, this is meant to merge the quantities into
   one record and write a table to the same directory. The OI title of the record
   will be the last one found"
  [path]
  (let [recs (merged-quantities (tbl/tabdelimited->records (slurp path)
                 :parsemode :noscience))
        out (clojure.string/replace path "AUDIT_SupplyRecords.txt"
                                         "AUDIT_SupplyRecords_merged.txt")]
    (tbl/records->file recs out :field-order supply-order)))


(defn quants-by-compo
  "Given a path to a Marathon audit trail, returns a map where the keys are srcs
  and the values are nested maps where the keys are components and the values
  are quantities. Merges supply records according to merged-quantities."
  [root & {:keys [supply-filter] :or {supply-filter (fn [r] (:Enabled r))}}]
  (let [supprecs (->> (util/supply-records root)
                      (filter supply-filter)
                      (merged-quantities))]
    (reduce (fn [m r] (assoc m (:SRC r) (assoc (m (:SRC r)) (:Component r) (:Quantity r))))
            {} supprecs)))

(defn merge-ints
  "For one interest, combine the quantities into one map keyed by the name in
   interest.  Returns the complete quantity map with the changes made for one
   interest. Removes srcs that are included in the interest."
  [m [name srcs]]
  (let [quant-map (apply merge-with + (map m srcs))
        new-map (reduce #(dissoc %1 %2) m srcs)]
    (assoc new-map name quant-map)))

(defn add-totals [src-compo-map]
  (reduce-kv (fn [m k v] (assoc m k (assoc v "total" (apply + (vals v))))) {} src-compo-map))

(defn print-inventory
  [total ng rc ac] (cl-format nil "~A/~A/~A//~A" ac ng rc total))

(defn return-inv
  "returns [NG RC AC] ints in a vector from an inventory map."
  [inv]
  (let [{:keys [total NG RC AC] :or {total 0 NG 0 RC 0 AC 0}}
        (reduce-kv (fn [m k v] (assoc m (keyword k) v)) {} inv)]
    [total NG RC AC]))
  
(defn print-inv
  "take one inventory and return a string"
  [inv] ;inv is one inventory map
    (apply print-inventory (return-inv inv)))

(defn print-all-invs [invs]
  (reduce-kv (fn [m k v] (assoc m k (print-inv v))) {} invs))

(defn by-compo-supply-map
  "returns a map where the interests and/or SRCs are keywords and the values are
  maps of components and a total to the supply quantities."
  [root & {:keys [interests supply-filter]
           :or {supply-filter :Enabled interests nil}}]
  (let [quants (quants-by-compo root :supply-filter supply-filter)]
    (if interests
      (add-totals (reduce merge-ints quants (vals interests)))
      (add-totals quants))))

(defn by-compo-supply-map-groupf
  "like by-compo-supply-map but groups the srcs according to groupfn"
  [root & {:keys [supply-filter group-fn]
           :or {supply-filter :Enabled group-fn (fn [s] s)}}]
  (let [quants (quants-by-compo root :supply-filter supply-filter)]
    (reduce (fn [acc [group quants]]
              (assoc acc group (apply merge-with + (map second quants)))) {}
            (group-by (fn [[src quantmap]] (group-fn src)) quants))))

;;Generic.
(defn supply-by-compo
  "returns a map where the interests and/or SRCs are keywords and the values are
  the strings computed from print-inventory."
  [root & {:keys [interests supply-filter]
           :or {supply-filter :Enabled interests nil}}]
  (print-all-invs
   (by-compo-supply-map root :interests interests :supply-filter supply-filter)))

;;Generic?
(defn wbpath-from-auditpath
  "Returns the path of the marathon workbook if given the path of the audit trail."
  [auditpath]
  (let [mpath (str/join "/" (butlast (str/split auditpath #"[/\\\\]")))
        run-name (get-run-name auditpath)
        wbpath (str mpath "/" run-name ".xlsx")]
    (if (io/fexists? wbpath)
      wbpath
      (java.io/resource wbpath))))

;;Generic?
(defn get-policies
  "Returns a map of compo to policy name for each component given the path to the audit trail"
  [path]
  (->> (line-reducer (util/resource-check (str path
                                               "AUDIT_Parameters.txt"))
                     :reader-fn clojure.java.io/reader)
       (into [])
       (map tbl/split-by-tab)
       (reduce (fn [acc line-vec] (case (first line-vec)
                                    "DefaultACPolicy" (assoc acc "AC" (second line-vec))
                                    "DefaultRCPolicy" (assoc acc "RC" (second line-vec))
                                    "DefaultNGPolicy" (assoc acc "NG" (second line-vec))
                                    acc)) {})))

(defn parse-template
  "given a template string, categorize the template for compute-discount."
  [template]
  (if (str/includes? template "NearMaxUtilization")
    "NearMaxUtilization"
    (if (= template "MaxUtilization")
      "MaxUtilization"
      (if (= (subs template 0 2) "RC")
        "RC"
        (if (= (subs template 0 2) "AC")
            "AC"
            (throw (Exception. (str "Unknown template " template))))))))

;;Predicate
(defn restricted-static-policy?
  "Throw an exception if the policy violates static analysis assumptions.
  Otherwise, return true. These cases would probably be when static analysis is
  most valid, but a lot of our policies violate these assumptions so I rewrote
  this fn below to be more liberal."
  [bog {:keys [MaxBOG MaxDwell MinDwell StopDeployable StartDeployable PolicyName]
        :as policy}]
  (if (or (not= (- StopDeployable StartDeployable) bog) ;violates static analysis assumption
          (< StartDeployable (- StopDeployable bog)) ;lifecycle shortened when deployed
          (< StopDeployable MaxDwell)
          (not= MaxDwell (+ MinDwell bog)))
    (throw (ex-info (str "The " PolicyName " policy might violate static analysis assumptions.")
                    {:policy policy}))
    true))

;;Predicate
(defn static-policy?
  "In this case, we can assume that a unit always bogs at the end of his lifecycle."
  [bog {:keys [MaxDwell StopDeployable StartDeployable PolicyName] :as policy}]
  ;;make sure they're deployable at the end of the lifecycle
  (or (and (<= (- MaxDwell bog) StopDeployable) (<= StartDeployable (- MaxDwell bog)))
      (throw (ex-info (str "The " PolicyName
                           " policy might violate static analysis assumptions.")
                      {:policy policy}))))

(defn compute-discount
  "given a policy record, compute the static analysis rotational discount."
  [{:keys [MaxBOG MaxDwell MinDwell Overlap Template
           StopDeployable StartDeployable PolicyName] :as policy}]
  (let [;;this might have made more sense sometimes
        staticf (fn [bog] (/ (- bog Overlap) MaxDwell))
        ;;this will be useful for a couple use cases below
        deployablef (fn [bog] (/ (- bog Overlap) (+ StartDeployable bog)))]
    (case (parse-template Template)
      "MaxUtilization" (deployablef MaxBOG)
      ;always RC so far so +95
      "NearMaxUtilization" (deployablef (+ 95 MaxBOG))
      "RC" (case PolicyName
             ;;static analysis is assuming that a unit deploys every lifecycle since
             ;;we remove overlap from bog.  Therefore, if our BOG is > stopdeployable - start
             ;;deployable, we can probably just use
             "TAA21-25_RC_1:2" (deployablef (+ 95 MaxBOG))
      (when (restricted-static-policy? (+ MaxBOG 95) policy)
             (staticf (+ MaxBOG 95))))
      "AC" (case PolicyName
             "ReqAnalysis_MaxUtilization_FullSurge_AC" (deployablef MaxBOG)
      (when (restricted-static-policy? MaxBOG policy)
             (staticf MaxBOG))))))

(defn round-to
  "rounds a number, n to an integer number of (num) decimals"
  [num n]
  (read-string (format (str "%." num "f") (float n))))

;;generic, re-inserted due to poor merge.
(defn vba-round
  "rounds a number, n to 1 decimal place, but if the number is whole,
  the .0 is dropped."
  [n]
  (let [x (str (round-to 1 n))
        [o t] (clojure.string/split x #"\.")]
    (if (= t "0") o x)))

(defn policy-name
  "given a policy record and component, creates a name for the policy in terms of BOG:Dwell"
  [{:keys [MaxBOG MaxDwell MinDwell Overlap Template
           StopDeployable StartDeployable PolicyName] :as policy} component]
  (let [;for mobilization (only seen 95 recently)
        bog (if (and (not= Template "MaxUtilization")
                     (or (= component "RC") (= component "NG"))) (+ MaxBOG 95) MaxBOG)]
      (str (vba-round (/ bog 365)) ":" (vba-round (/ StartDeployable
                                                     365)))))

;;Generic.
(defn policy-info
  "given the path to a Marathon audit trail, return a map of {:composites
  {...} :policies {...}} where the composites map is a map of [CompitePolicyName
  Period] to the policy record and the policies map is a map of PolicyName to
  the policy record."
  [path]
  (let [{composites "CompositePolicyRecords"
         policies   "PolicyRecords"}
        (->> (xl/wb->tables (xl/as-workbook (wbpath-from-auditpath path))
                            :sheetnames ["CompositePolicyRecords" "PolicyRecords"])
             (reduce-kv (fn [acc k v] (assoc acc k
                                        (tbl/table-records (tbl/keywordize-field-names v))))
                        {}))
        policy-map (reduce (fn [acc {:keys [PolicyName] :as r}]
                             (assoc acc PolicyName r)) {} policies)
        composite-map (reduce (fn [acc {:keys [CompositeName Period Policy] :as r}]
                                (assoc acc [CompositeName Period] (policy-map Policy)))
                              {} composites)]
    {:composites composite-map :policies policy-map}))

(defn policy-record
  "given a policy name, period name, and the policies and composites maps from
   policy-info, return the period record for the period.
   An exception is thrown if the policy doesn't exist in either policies or composites."
  [policy period policies composites]
  (if (contains? policies policy)
    (policies policy)
    (if (contains? composites [policy period])
      (composites [policy period])
      (throw (ex-info "does not exist in the PolicyRecords or CompositePolicyRecords"
                      {:policy policy})))))

(defn policies-by-period
  "Given the path to a Marathon audit trail, returns a map of [compo period] to the policy record for
  the AC and RC for each period."
  [path & {:keys [policyinfo periods active-policies]}]
  (let [active-policies (if active-policies active-policies (get-policies path))
        {:keys [composites policies]} (if policyinfo policyinfo
                                          (policy-info path))
        periods (if periods periods (util/period-records path))]
    (into {} (for [[compo policy] active-policies
                   {nm :Name} periods]
               [[compo nm] (policy-record policy nm policies composites)]))))

(defn ra-rc
  "given the result of policies-by-period, compute the policy string in the form of
  RABOG:RAStartDeployable/RCBOG:RCStartDeployable for the period."
  [policies period]
  (str (policy-name (policies ["AC" period]) "AC") "/"
                                            (policy-name (policies ["NG" period]) "NG"))
  )

(defn rc-availability
  "given the result of policies-by-period, compute the policy string
  for % RC available in the form of int%RC for the period."
  [policies period]
  (let [{:keys [MaxBOG MaxDwell MinDwell Overlap Template StopDeployable
           StartDeployable PolicyName] :as policy} (policies ["NG" period])
        ;;for mobilization (only seen 95 recently)
        bog (if (not= Template "MaxUtilization") (+ MaxBOG 95) MaxBOG)]
    (str (round-to 0 (* 100 (/ bog (+ StartDeployable bog)))) "%RC")))
  
(defn policy-string
  "given the path to a marathon audit trail, compute a policy string
  for each period separated by ->.  Define a separate fn called
  period-fn, which takes the result of policies-by-period and the name
  of a period to return a string for each period."
  [path & {:keys [period-fn] :or {period-fn ra-rc}}]
  (let [policies (policies-by-period path)]
    (assert (->> (remove (fn [[[compo period] v]] (= compo "AC")) policies)
                 (group-by (fn [[[compo period] v]] period))
                 (every? (fn [[period xs]]
                           (apply = (map (fn [[[compo period] v]]
                                           (:PolicyName v)) xs)))))
            "Assume that NG and RC follow the same policies.")
                                        ;reduce over period recs,
                                        ;for ac and rc make this joined by ->
    (reduce (fn [acc {:keys [Name]}] (str acc (if (= acc "") "" "->")
  (period-fn policies Name)))
            "" (util/period-records path)))
  )

;;Generic
(defn discounts-by-period
  "Given the path to a Marathon audit trail, compute the rotational discount for
  the AC and RC for each period."
  [path & {:keys [policyinfo periods active-policies]}]
  (into {}
        (map (fn [[[compo nm] policyrec]] [[compo nm]
  (compute-discount policyrec)]) (policies-by-period path :policyinfo
  policyinfo :periods periods :active-policies active-policies))))


;;Generic.
(defn capacities
  "Given the root directory to a Marathon audit trail, returns a map of [int period] to theoretical capacity."
  [path & {:keys [supply-filter] :or {supply-filter (fn [r] (:Enabled r))}}] 
  (let [policyinfo (policy-info path)
        periods (util/period-records path)
        active-policies (get-policies path)
        discounts (discounts-by-period path :policyinfo policyinfo
                                       :periods periods
                                       :active-policies active-policies)
        special-policies (atom #{})
        auto? (fn [policy] (contains? #{"AUTO" ""}  (str/upper-case policy)))
        supprecs (->>
                  (tbl/tabdelimited->table
                   (slurp (util/resource-check
                           (str path "AUDIT_SupplyRecords.txt")))
                   :schema proc.schemas/supply-recs)
                      (tbl/table-records)
                      (filter supply-filter)
                      (map (fn [{:keys [Policy] :as r}]
                             (when (not (auto? Policy)) 
                               (swap! special-policies conj Policy))
                             r))
                                        ;multiple policies for one src will still exist after merging
                      (merged-quantities))
        specials (discounts-by-period path
                                      :policyinfo policyinfo
                                       :periods periods
                                       :active-policies
                                       (map (fn [x] [x x]) @special-policies))]
    (for [{:keys [Policy Quantity Component SRC] :as r} supprecs
          {nm :Name} (util/period-records path)]
      {:src SRC :period nm :capacity (* Quantity (if (auto? Policy)
                                                   (discounts [Component nm])
                                                   (specials [Policy nm])))})))

;;Generic.
(defn capacity-by
  "Given a path to a Marathon audit trail, compute the theoretical capacity by
  period for each group of supply records defined by group-fn."
  [path & {:keys [group-fn supply-filter] :or {group-fn (fn [s] "All") supply-filter :Enabled}}]
  (->> (capacities path :supply-filter supply-filter)
                                        ;(group-by (juxt (fn [{:keys [src]}] (group-fn src)) :period))
       (util/separate-by (fn [{:keys [src]}] (group-fn src)))
       (mapcat (fn [[grp xs]] (for [[period recs] (group-by :period xs)]
                             [[grp period] recs])))
       (map (fn [[[interest period] recs]] [[interest period] (reduce + (map :capacity recs))]))
       (into {})))

;;Generic.
(defn oi-titles
  "given a path to a marathon audit trail, return a map of src to OI title.
   This data is pulled from audit supply records. Uses only enabled supply for now."
  [path]
  (->> (util/supply-records path)
       (reduce (fn [a {:keys [SRC OITitle]}] (assoc a SRC OITitle)) {})))
