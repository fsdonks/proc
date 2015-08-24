(ns proc.sporkpatches
  (:require [spork.util.table]
            [spork.util.clipboard]))

(defmacro within-ns [ns & expr]
  `(do (ns ~ns)
       ~@expr
       (ns ~'proc.sporkpatches)))

;;tHIS IS A HACK UNTIL I CAN CHANGE IT BAAAH
;;we're just fixing the vector parsing code, I'll commit this and
;;deploy correctly tomorrow.
(within-ns spork.util.parsing
  (defn vec-parser 
    "Given a set of fields, and a function that maps a field name to 
   a parser::string->'a, returns a function that consumes a sequence
   of strings, and parses fields with the corresponding 
   positional parser.  Alternately, caller may supply a parser as a 
   single argument, to be applied to a vector of strings."
    ([fields field->value]
       (let [xs->values  (vec (map #(partial2 field->value %) fields))
             bound       (count xs->values)
             uptolast    (dec bound)]
         (fn [xs]
           (let [res 
                 (loop [acc []
                        idx 0]
                   (if (= idx uptolast) acc
                       (recur (conj acc ((nth xs->values idx) (nth xs idx)))
                              (inc idx))))]
             (conj res 
                   (cond (== (count xs) bound) 
                         ((nth xs->values uptolast) (nth xs uptolast)) ;;append the last result
                         (== (count xs) uptolast) nil))))))      ;;add an empty value                 
    ([f] 
       (let [parsefunc (lookup-parser f identity)]
         (fn [xs]         
           (reduce (fn [acc x] 
                     (conj acc (parsefunc x)))  []
                     xs))))))

;;patch these in temporarily.
(within-ns 
 spork.util.table
 (defn paste-table! [t]  (spork.util.clipboard/paste! (table->tabdelimited t)))
 (defn add-index [t] (conj-field [:index (take (record-count t) (iterate inc 0))] t))
 (defn no-colon [s]   (if (or (keyword? s)
                              (and (string? s) (= (first s) \:)))
                        (subs (str s) 1)))



 (defn collapse [t root-fields munge-fields key-field val-field]
   (let [root-table (select :fields root-fields   :from t)]
     (->>  (for [munge-field munge-fields]
             (let [munge-col  (select-fields [munge-field] t)
                   munge-name (first (get-fields munge-col))
                   
                   key-col    [key-field (into [] (take (record-count root-table) 
                                                        (repeat munge-name)))]
                   val-col    [val-field  (first (vals (get-field munge-name munge-col)))]]
               (conj-fields [key-col val-col] root-table)))
           (concat-tables)          
           (select-fields  (into root-fields [key-field val-field])))))

 (defn rank-by  
   ([trendf rankf trendfield rankfield t]
      (->> (for [[tr xs] (group-by trendf  (table-records t))] 
             (map-indexed (fn [idx r] (assoc r trendfield tr  rankfield idx)) (sort-by rankf xs)))
           (reduce      concat)
           (records->table)
           (select-fields (into (table-fields t) [trendfield rankfield]))))
   ([trendf rankf t] (rank-by trendf rankf :trend :rank t)))

 (defn ranks-by [names-trends-ranks t]    
   (let [indexed (add-index t)
         new-fields (atom [])
         idx->rankings 
         (doall (for [[name [trendf rankf]] names-trends-ranks]
                  (let [rankfield (keyword (str (no-colon name) "-rank"))
                        _ (swap! new-fields into [name rankfield])]
                    (->> indexed
                         (rank-by trendf rankf name rankfield)
                         (select-fields [:index name rankfield])
                         (reduce (fn [acc r]
                                   (assoc acc (:index r) [(get r name) (get r rankfield)])) {})))))]
     (conj-fields      
      (->> idx->rankings
           (reduce (fn [l r] 
                     (reduce-kv (fn [acc idx xs]
                                  (update-in acc [idx]
                                             into xs))
                                l r)))
           (sort-by first)
           (mapv second)
           (spork.util.vector/transpose)
           (mapv vector @new-fields)
           )
      indexed)))
)
