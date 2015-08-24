;;Just a place to stick crap I'm not ready to junk yet, research stuff.
(ns proc.stash)
;;THese are hopefully more performant implementations....not
;;necessarily though...
(comment 

(defn get-unfilled-demandtrends2 [path]
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
            (map tbl/split-by-tab)
            (filter (fn [xs] (*demand-trend-pre-filter* (nth xs src-field))))
            (map  parse-fields)
            (filter *demand-trend-filter*) ; moved up....
            (map (fn [r] (if (> (:t r) @tlast) (do (reset! tlast (:t r)) r) r)))
            (map (fn [r] (if (contains? @demandmeta (:DemandName r)) r
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
                                  _ (when (pos?  fl)
                                      (swap! unfilled conj! idx))]
                              (assoc r :Unfilled fl))))
             (tbl/records->table))
         @tlast  @demandmeta (persistent! @unfilled)])))

;;still trying to get this to be performant....should be in theory, 
;;but it's spurious.

(defn get-unfilled-demandtrends3 [path]
  (let [fields       (util/get-headers path)
        src-field    (some-n #{:SRC} fields) ;this is baked in, may
                                        ;need more generality...
        parse-fields (dtrend-parser fields)
        tlast        (atom 0)
        unfilled     (atom {})
        demandmeta   (atom {})]
    [(->> (iota/seq path)
;          (r/drop 1)
          (next)
          (r/map tbl/split-by-tab)
          (r/filter (fn [xs] (*demand-trend-pre-filter* (nth xs src-field))))
          (r/map  parse-fields)
          (r/filter *demand-trend-filter*) ; moved up....
          (r/map (fn [r] (if (> (:t r) @tlast) (do (reset! tlast (:t r)) r) r)))
          (r/map (fn [r] (if (contains? @demandmeta (:DemandName r)) r
                             (do (swap! demandmeta 
                                        assoc  
                                        (:DemandName r) 
                                        {:DemandGroup (:DemandGroup r)
                                         :Vignette    (:Vignette r)
                                         :Required    (:TotalRequired r)
                                         :Operation   (:Operation r)
                                         :SRC         (:SRC r)}) 
                              r))))
         (r/map (fn [r]                   
                  (let [fl (fill r)
                        ;; _ (when (pos?  fl)
                        ;;     (swap! unfilled conj idx))
                        ]
                    (assoc r :Unfilled fl))))
         (fold-into-vector)
         ;; (map-indexed (fn [idx r]                   
         ;;                   (let [fl (fill r)
         ;;                         _ (when (pos?  fl)
         ;;                             (swap! unfilled conj idx))]
         ;;                     (assoc r :Unfilled fl))))
         (tbl/records->table)
         )
    @tlast  @demandmeta ;(persistent! @unfilled)
                         @unfilled]))

)
