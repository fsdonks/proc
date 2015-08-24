(ns proc.ranking
  (:require [proc.sporkpatches]            
            [spork.util.table :as tbl]
            [spork.util.excel.core :as xl]))


;;composite definitions removed for unclass transfer.
(def composites 
  (->>
 
   (tbl/records->table)))

(defn spit-ranks [t path]
  (let [tmap  (into {} 
                    (for [[[compo period] recs] (group-by (comp clojure.edn/read-string :Compo-Period) (tbl/table-records t))]
                      [(str compo (tbl/no-colon period))
                       (->> recs 
                            (sort-by :Compo-Period-rank)
                            (tbl/records->table)
                            (tbl/select-fields (tbl/table-fields t))
                            (tbl/stringify-field-names))]))]
    (xl/tables->xlsx path tmap)))

(def bct-composite [4.77 4.71 2.71 2.74 3.21 4.39 3.95 3.36 2.43 2.87 4.25	4.46	0.74	3.09	3.24])


(defn top-n [n t] (into [] (r/take n (r/map (fn [r] {:SRC (get r "SRC") :Compo (get r "Compo") :Period (get r "Period")}) t))))


(comment 
  (ranks-by {:Total        [(fn [_] :Total)       :Dwell] 
             :Compo-Period [(juxt :Compo :Period) :Dwell]})
  (def colld (collapse mytbl [:UnitofInterest :UnitGrouping :SRC :Title :STR :ACSupply :ARNGSupply :USARSupply :Compo] 
                       [:PreSurge :Surge1 :BetweenSurges :Surge2 :PostSurge] :Period :Dwell))
  (def res (let [get-field (memoize (fn [compo] (keyword (str compo "Supply"))))]
             (ranks-by {:Total        [(fn [_] :Total)       :Dwell] 
                        :Compo-Period [(juxt :Compo :Period) :Dwell]} 
                       (->> (tbl/select  :from colld :where (fn [r] (and (not= (:Dwell r) "nil") 
                                                                         (pos? (get r (get-field (get r :Compo)))))))
                            (tbl/select-fields (tbl/table-fields colld))))))
  (spit-ranks res "C:\\Users\\thomas.spoon\\Documents\\zeto commission\\nlist-results-draft.xlsx")
  (def tabs (xl/xlsx->tables "C:\\Users\\thomas.spoon\\Documents\\zeto commission\\nlist-results-draft.xlsx"))
  (def titles  (reduce (fn [acc r] (assoc acc (get r "SRC") (get r "Title")))  (mapcat tbl/table-records (vals tabs))))
  (def tops (map (comp set (partial top-n 20))   (vals tabs)))
  (def gtops 
    (reduce (fn [acc [p ys]] 
              (if-let [xs (get acc p)] 
                (update-in acc [p] into (map #(dissoc % :Period) ys)) 
                (assoc acc p (set (map #(dissoc % :Period) ys))))) 
            {}  
            (for [ts tops] (let [t (first ts)] [ (:Period t) ts]))))
  (def counts (reduce (fn [stats xs] (reduce (fn [acc r] (if-let [cnt (get acc r)] (assoc acc r (inc cnt)) (assoc acc r 1))) stats xs)) {}  (vals gtops)))
  (tbl/paste-records! (reduce-kv (fn [acc r v] (conj acc (assoc r :periods v :Title (get titles (:SRC r))))) [] counts))
)
