;;analyze the supply input before a Marathon run or the location of units at a point in time from Marathon data.
(ns proc.supply
  (:require [proc.stacked :as stacked]
            [spork.util.table :as tbl]
            [proc.schemas :as schemas]
            [proc.util :as util]
            [clojure.pprint :refer [cl-format]])
  (:use [proc.core]))



(defn units-locs-at 
  "Given a time, t, and the root directory to the marathon run, this function will return the location
of all units as records at time t.  Can also provide a substring of the unit names you want returned as :unitpart."
  [time root & {:keys [unitpart] :or {unitpart ""}}]
  (let [samples (-> (tbl/table-records (tbl/tabdelimited->table (slurp (str root "locations.txt"))))
                  (sample-trends #(get % :EntityFrom) #(get % :T))
                  (samples-at time))]
    (filter (fn [r] (.contains (:EntityFrom r) unitpart)) (map second samples))))

;what about multiple records for one src and one compo?  this keeps latest.  What does Marathon do?
(defn quants-by-compo [root & {:keys [supply-filter] :or {supply-filter (fn [r] (:Enabled r))}}] 
  (let [supprecs (->> (tbl/tabdelimited->table (slurp (str root "AUDIT_SupplyRecords.txt")) :schema proc.schemas/supply-recs)
                   (tbl/table-records)
                   (filter supply-filter))]
    (reduce (fn [map r] (assoc map (:SRC r) (assoc (map (:SRC r)) (:Component r) (:Quantity r)))) {} supprecs)))


(defn merge-ints
  "For one interest, combine the quantities into one map keyed by the name in interest. Returns the complete quantity map
with the changes made for one interest."
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

(defn supply-by-compo 
  "returns a map where the interests and/or SRCs are keywords and the values are the strings computed
 from print-inventory."
  [root & {:keys [interests supply-filter] :or {supply-filter (fn [r] (:Enabled r)) interests nil}}]
  (let [quants (quants-by-compo root :supply-filter supply-filter)
        invs (if interests 
               (add-totals (reduce merge-ints quants (vals interests)))
               (add-totals quants))]
    (print-all-invs invs)))



(def supply-order [:Type :Enabled :Quantity :SRC :Component :OITitle :Name :Behavior :CycleTime :Policy :Tags :Spawntime :Location :Position])

;This is the laborious method.
(comment
(defn extra-fields [supply-fields]
  (clojure.set/difference
   (set supply-fields)
   (set (map keyword (keys (dissoc schemas/supply-recs "Original?"))))))

(defn merge-quants 
  "If there are duplicate (same compo and SRC) in supply records, this is meant to merge the quantities into one record
   and return a table.  The OI title of the record will be the last one found"
  [path] ;path is expected to be a supply records tab-delim text file
  (let [untable (tbl/tabdelimited->table (slurp path) :parsemode :noscience )
        fields (:fields untable)
        extrafields (conj (extra-fields fields) :OITitle :Enabled)
        extrafields (if (:Spawntime extrafields) (disj extrafields :Spawntime) extrafields)
        reducer (fn [quantmap {:keys [Quantity] :as rec}] (let [k (apply dissoc rec (conj extrafields :Quantity))
                                                                currquant (:Quantity (get quantmap k))] 
                                                                (assoc quantmap k (merge {:Quantity (if currquant (+ Quantity currquant)
                                                                                                 Quantity)}
                                                                                          (reduce #(assoc %1 %2 (get rec %2)) {} extrafields)))))                                                        
        out (clojure.string/replace path "AUDIT_SupplyRecords.txt" "AUDIT_SupplyRecords_merged.txt")
         table (-> (->> (reduce reducer {} (tbl/table-records untable))
          (reduce-kv (fn [acc r p] (conj acc (merge r p))) [])
          (tbl/records->table)
          (tbl/order-fields-by supply-order))
                   )] 
    (tbl/table->file table out :stringify-fields? true
                     )))
)

(def key-fields [:Type :SRC :Component :Name :Behavior :CycleTime :Policy :Tags :Spawntime :Location :Position] )
(defn merge-quants [path]
  (let [recs (tbl/tabdelimited->records (slurp path) :parsemode :noscience )
        out (clojure.string/replace path "AUDIT_SupplyRecords.txt" "AUDIT_SupplyRecords_merged.txt")]
    (-> (->> (group-by #(map % key-fields) recs)
         (reduce-kv (fn [a k v] (conj a (assoc (last v) :Quantity (reduce + (map :Quantity v))))) []))
         (tbl/records->file out :field-order supply-order))))
