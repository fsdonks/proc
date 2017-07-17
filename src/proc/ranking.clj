(ns proc.ranking
  (:require [proc.sporkpatches]            
            [spork.util.table :as tbl]
            [spork.util.excel.core :as xl]
            [clojure.core.reducers :as r]))

;notional
(def composites 
  (->>
   [{:SRC "BCT-Composite", :Compo "AC", :Period "PreSurge", :Dwell 5}
    {:SRC "BCT-Composite", :Compo "AC", :Period "Surge1", :Dwell 6}
    {:SRC "BCT-Composite", :Compo "AC", :Period "BetweenSurge",  :Dwell 7}
    {:SRC "BCT-Composite", :Compo "AC", :Period "Surge2", :Dwell 8}
    {:SRC "BCT-Composite", :Compo "AC", :Period "PostSurge", :Dwell 9}
    {:SRC "BCT-Composite", :Compo "ARNG", :Period "PreSurge", :Dwell 1}
    {:SRC "BCT-Composite", :Compo "ARNG", :Period "Surge1", :Dwell 2}
    {:SRC "BCT-Composite", :Compo "ARNG", :Period "BetweenSurge", :Dwell 3}
    {:SRC "BCT-Composite", :Compo "ARNG", :Period "Surge2", :Dwell 4}
    {:SRC "BCT-Composite", :Compo "ARNG", :Period "PostSurge", :Dwell 8}
    {:SRC "BCT-Composite", :Compo "USAR", :Period "PreSurge", :Dwell 3.5}
    {:SRC "BCT-Composite", :Compo "USAR", :Period "Surge1", :Dwell 4.2}
    {:SRC "BCT-Composite", :Compo "USAR", :Period "BetweenSurge", :Dwell 8.1}
    {:SRC "BCT-Composite", :Compo "USAR", :Period "Surge2", :Dwell 7.3}
    {:SRC "BCT-Composite", :Compo "USAR", :Period "PostSurge", :Dwell 1.5}]
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

;notional
(def bct-composite [1.01 1.42 3.55 0.61 2.05 1.65 3.12 0.79 4.48 3.56 2.48 4.94 4.42 0.31 1.52])


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
