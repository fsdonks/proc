;;analyze the supply input before a Marathon run or the location of units at a point in time from Marathon data.
(ns proc.supply
  (:require [proc.stacked :as stacked]
            [spork.util.table :as tbl]
            [proc.schemas :as schemas]
            [proc.util :as util]
            [clojure.pprint :refer [cl-format]]
            [spork.util.general :refer [line-reducer]]
            [spork.util.excel.core :as xl]
            [clojure.string :as str])
  (:use [proc.core]))



(defn units-locs-at 
  "Given a time, t, and the root directory to the marathon run, this function will return the location
of all units as records at time t.  Can also provide a substring of the unit names you want returned as :unitpart."
  [time root & {:keys [unitpart] :or {unitpart ""}}]
  (let [samples (-> (tbl/table-records (tbl/tabdelimited->table (slurp (str root "locations.txt"))))
                  (sample-trends #(get % :EntityFrom) #(get % :T))
                  (samples-at time))]
    (filter (fn [r] (.contains (:EntityFrom r) unitpart)) (map second samples))))

(def supply-order [:Type :Enabled :Quantity :SRC :Component :OITitle :Name :Behavior :CycleTime :Policy :Tags :Spawntime :Location :Position])
(def key-fields [:Type :SRC :Component :Name :Behavior :CycleTime :Policy :Tags :Spawntime :Location :Position] )

(defn merged-quantities
  "Given records of Marathon AUDIT_SupplyRecords and if there are duplicates (same compo and SRC) in the records, this is meant to merge the quantities into one record and return a new sequence of supply records.  The OI title of the record will be the last one found"
  [recs]
    (->> (group-by #(map % key-fields) recs)
         (reduce-kv (fn [a k v] (conj a (assoc (last v) :Quantity (reduce + (map :Quantity v))))) [])))

(defn merge-quants
  "Given the path to Marathon supply records and if there are duplicates (same compo and SRC) in supply records, this is meant to merge the quantities into one record and write a table to the same directory.  The OI title of the record will be the last one found"
  [path]
  (let [recs (merged-quantities (tbl/tabdelimited->records (slurp path) :parsemode :noscience))
        out (clojure.string/replace path "AUDIT_SupplyRecords.txt" "AUDIT_SupplyRecords_merged.txt")]
    (tbl/records->file recs out :field-order supply-order)))

(defn quants-by-compo
  "Given a path to a Marathon audit trail, returns a map where the keys are srcs and the values are nested maps where the keys are components and the values are quantities.  Merges supply records according to merged-quantities."
  [root & {:keys [supply-filter] :or {supply-filter (fn [r] (:Enabled r))}}] 
  (let [supprecs (->> (tbl/tabdelimited->table (slurp (str root "AUDIT_SupplyRecords.txt")) :schema proc.schemas/supply-recs)
                   (tbl/table-records)
                   (filter supply-filter)
                   (merged-quantities))]
    (reduce (fn [map r] (assoc map (:SRC r) (assoc (map (:SRC r)) (:Component r) (:Quantity r)))) {} supprecs)))

(defn merge-ints
  "For one interest, combine the quantities into one map keyed by the name in interest. Returns the complete quantity map
  with the changes made for one interest. Removes srcs that are included in the interest."
  [m [name srcs]]
  (let [quant-map (apply merge-with + (map m srcs))
        new-map (reduce #(dissoc %1 %2) m srcs)]
    (assoc new-map name quant-map)))

(defn add-totals [src-compo-map]
  (reduce-kv (fn [m k v] (assoc m k (assoc v "total" (apply + (vals v))))) {} src-compo-map))

(defn print-inventory
  [total ng rc ac] (cl-format nil "~A/~A/~A//~A" ac ng rc total))

(defn print-inv 
  "take one inventory and return a string"
  [inv] ;inv is one inventory map
  (let [{:keys [total NG RC AC] :or {total 0 NG 0 RC 0 AC 0}} (reduce-kv (fn [m k v] (assoc m (keyword k) v)) {} inv)] 
    (print-inventory total NG RC AC)))

(defn print-all-invs [invs]
  (reduce-kv (fn [m k v] (assoc m k (print-inv v))) {} invs))

(defn by-compo-supply-map
  "returns a map where the interests and/or SRCs are keywords and the values are maps of components
  and a total to the supply quantities."
  [root & {:keys [interests supply-filter] :or {supply-filter (fn [r] (:Enabled r)) interests nil}}]
  (let [quants (quants-by-compo root :supply-filter supply-filter)]
    (if interests 
      (add-totals (reduce merge-ints quants (vals interests)))
      (add-totals quants))))

(defn by-compo-supply-map-groupf
  "like by-compo-supply-map but groups the srcs according to groupfn"
  [root & {:keys [supply-filter group-fn] :or {supply-filter (fn [r] (:Enabled r)) group-fn (fn [s] s)}}]
  (let [quants (quants-by-compo root :supply-filter supply-filter)]
    (reduce (fn [acc [group quants]]
              (assoc acc group (apply merge-with + (map second quants)))) {} 
            (group-by (fn [[src quantmap]] (group-fn src)) quants))))

(defn supply-by-compo 
  "returns a map where the interests and/or SRCs are keywords and the values are the strings computed
 from print-inventory."
  [root & {:keys [interests supply-filter] :or {supply-filter (fn [r] (:Enabled r)) interests nil}}]
  (print-all-invs (by-compo-supply-map root :interests interests :supply-filter supply-filter)))

;(tbl/tabdelimited->table (slurp (str root "AUDIT_PeriodRecords.txt")) :parsemode :noscience 
                                         ;:schema schemas/periodrecs
                        ; )

(defn wbpath-from-auditpath
  "Returns the path of the marathon workbook if given the path of the audit trail."
  [auditpath]
  (let [mpath (str/join "/" (butlast (str/split auditpath #"[/\\\\]")))
        run-name (get-run-name auditpath)]
    (str mpath "/" run-name ".xlsx")))

(defn get-policies
  "Returns a map of compo to policy name for each component given the path to the audit trail"
  [path]
  (->> (line-reducer (str path "AUDIT_Parameters.txt"))
       (into [])
       (map tbl/split-by-tab)
       (reduce (fn [acc line-vec] (case (first line-vec)
                                    "DefaultACPolicy" (assoc acc "AC" (second line-vec))
                                    "DefaultRCPolicy" (assoc acc "RC" (second line-vec))
                                    "DefaultNGPolicy" (assoc acc "NG" (second line-vec))
                                    acc)) {})))

(defn compute-discount
  "given a policy record, compute the static analysis rotational discount."
  [{:keys [MaxBog MaxDwell Overlap]}]
  (/ (- MaxBog Overlap) MaxDwell))

(defn rotational-discounts
  "Given the path to a Marathon audit trail, compute the rotational discount for
  the AC and RC for each period."
  [path]
  (let [{composites "CompositePolicyRecords"
         policies "PolicyRecords"} (->> (xl/wb->tables (xl/as-workbook (wbpath-from-auditpath path))
                                                       :sheetnames ["CompositePolicyRecords" "PolicyRecords"])
                                        (reduce-kv (fn [acc k v] (assoc acc k
                                                                        (tbl/table-records (tbl/keywordize-field-names v))))
                                                   {}))
        periods (util/load-periods path)
        active-policies (get-policies path)
        policy-names (set (vals active-policies))
        policy-map (reduce (fn [acc {:keys [PolicyName] :as r}] (assoc acc PolicyName r)) {} policies)
        composite-map (reduce (fn [acc {:keys [CompositeName Period Policy] :as r}]
                              (if (contains? policy-names CompositeName)
                                (assoc acc [CompositeName Period] (policy-map Policy)) acc)) {} composites)]
    (for [[compo policy] (get-policies path)
          {nm :Name} periods]
      [[compo nm] (if (contains? policy-map policy)
                                    (policy-map policy)
                                    (composite-map [policy nm]))])))

;use rotational-discounts and a compo supply map to return a map of [int period] to theoretical capacities
(defn theoretical-capacities
  [root compo-supply-map]
  (by-compo-supply-map-groupf root :supply-filter (fn [r] (and (:Enabled r)))))

(defn capacity-by
  "Given a path to a Marathon audit trail, compute the theoretical capacity for each group
  of supply records defined by f."
  [f path]
  )
