(ns proc.ccard
  (:require [proc.util :as util]
            [proc.schemas :as schemas]
            [spork.util.table :as tbl]
            [clojure.pprint :refer [cl-format]]
            [clojure.string :as str]
            [clojure.pprint :as ppr]
            [spork.util.io    :as io]
            [proc.constants :as consts]
            [proc.example :as ex]
            [proc.supply :as supply]
            [proc.fillsfiles :as fills])
  (:use [proc.core])
  (:import
    (java.io FileOutputStream FileInputStream)
    (org.apache.poi.xssf.usermodel XSSFWorkbook XSSFCreationHelper)
    (org.apache.poi.ss.usermodel IndexedColors CellStyle)
    (org.apache.poi.hssf.util CellRangeAddress)))

;;IO
;;==

;;aux function that cleans up our headers.  this should be delegated to utils ;;and wrapped, but for now it's exposed.
(defn clean-lines [path]
  (let [headers     (clojure.string/join \tab (util/unkeyed-headers path))]
    (with-open [rdr (clojure.java.io/reader path)]
         (into [headers]
               (rest (line-seq rdr))))))

;;reads our coaches card records (really the filldata with computed fields) ;;from the root path, path.
(defn get-coach-records [path excludes & {:keys [srcs] :or {srcs false}}]
  (with-open [rdr (clojure.java.io/reader (str path "allfills+fields.txt"))]
  (let [;DaysAfterPHII will be empty for NotUtilized records.  will remove these with a filter before parsing this field.
        fillschema (merge schemas/fillrecord {:TrainLevel :text :DaysAfterPHII :text :run :text})
        fillrecs (tbl/lines->records (line-seq rdr)
                                    fillschema
                                    :parsemode :noscience :keywordize-fields? true)]
    (->> (into [] fillrecs)
      (filter (fn [r] (and (not (contains? excludes (clojure.string/upper-case (:DemandGroup r))))
                           (if srcs (contains? (set srcs) (:SRC r)) true))))
      (map (fn [{:keys [DaysAfterPHII] :as r}] (assoc r :DaysAfterPHII (Integer/parseInt DaysAfterPHII))))))))


;;Run Processing
;;===================

;;we can determine, by phase, the fill.
(defn split-misses [recs]
  (let [[fills misses]  
        (reduce (fn [[fills misses] {:keys [fill-type] :as r}]
                  (if (= fill-type "Unmet")
                    [fills (conj misses r)]
                    [(conj fills r) misses]))
                [[] []]
                recs)]
    {:fills  fills
     :misses misses}))

(defn demand-root
  "returns a demand name without the duplicate demand identifier"
  [demand-name]
  (let [splits (str/split demand-name #"_" )]
    (if (= (count splits) 5)
      (str/join "_" (butlast splits))
      demand-name)))

(defn get-trend [fills sampler]
  (let [t (:start (first fills))
        trends (proc.core/samples-at sampler t)
        dname (demand-root (:Demand (first fills)))
        ;trend (some (fn [[k v]] (when (= k dname) v)) trends) this wasn't accounting for duplicate dmd records
        trend (reduce (fn [curr {:keys [Overlapping TotalRequired]}] 
                        (assoc curr :Overlapping (+ (:Overlapping curr) Overlapping)
                                  :TotalRequired (+ (:TotalRequired curr) TotalRequired)))
                      (map second (filter (fn [[k v]] (.contains k dname)) trends)))
        ] 
        ;accounts for duplicate demand records but untested.
        ;using .contains because a duplicate demand name will get a _int concatonated by Marathon
        
    trend))

(defn add-quants [rs]                 
 (reduce + (map :quantity rs)))

;;compute some additional information based off the ;;missed demands and the fills, namely the required and filled stats.
(defn fill-by-time [recs sampler] ;recs are all from one phase
  (->> (group-by :start recs)
       (vals)
       (map split-misses)
       (map
        (fn [{:keys [fills misses] :as r}]
          (if (empty? fills)
              (merge r {:filled 0 :required (add-quants misses) :overlap 0})
           (let [trend (get-trend fills sampler)
                 overlap (:Overlapping trend)
                 actfills (- (count fills) overlap)]
            (merge r
                   {:filled actfills
                    :required (:TotalRequired trend)
                    :overlap overlap})))))))  

;;for a set of fills across a scenario, determine the best 
;;sample of fills (i.e. where filled is maximized).
(defn best-fill 
  "To find the max quantity filled for a phase"
  [xs]
  (reduce (fn [acc {:keys [filled] :as r}] 
            (if (> filled (:filled acc))
              r 
              acc)) 
          xs))      

(defn most-required 
  "To find the max quantity required for a phase"
  [xs]
  (reduce (fn [acc {:keys [required] :as r}] 
            (if (> required (:required acc))
              r 
              acc)) 
          xs))


;;for each unit that filled in the scenario, determine the filldata record that 
;;corresponds to its first arrival in the scenario.  
(defn first-arrival [recs]
  (for [[unit xs] (group-by :Unit recs)] 
    (first (sort-by :start xs))))

;;given a scenario, compute a set of statistics we can use for 
;;building a coach's card.  Specifically, we want to know the max-fill 
;;statistics by phase, the set of units that arrived late, and the set of 
;;units that arrived on-time, but with reduced readiness.  Arrivals and readiness 
;;will be simple sequences of filldata records.  
(defn scenario-stats [scenario sampler recs]
  (let [phase-recs (for [[phase xs] (group-by :operation recs)]
                     (let [fills (fill-by-time xs sampler)] ;fills are {:required :int :filled :int :overlap int :fills :recs
                       ;             :misses :recs} 
                       ;for every :start
                       [phase
                        (best-fill fills) fills (most-required fills)])) ;best-fill is an item from fills
        arrivals (first-arrival (remove (fn [r] (= (:fill-type r) "Unmet")) recs))] ;now we have a bunch of first-arrival fills.
        

    {:max-fills (map (fn [[phase best fills most-required]] [phase best most-required]) phase-recs)
     :late-arrivals (sort-by :DaysAfterPHII (filter (fn [r] (if (number? (:DaysAfterPHII r))
                                                              (and (= (:TrainLevel r) "T1") 
                                                                   (pos? (:DaysAfterPHII r)))
                                                              (throw (Exception. 
                                                                      "We should have :DaysAfterPHII for all demandgroups from demandrecords now")))) arrivals)) 
     :reduced-readiness (sort-by :DaysAfterPHII (filter (fn [r] (if (:TrainLevel r)
                                                                  (not= (:TrainLevel r) "T1")
                                                                  false ;this unit probably deployed first to this demandgroup for another demandtype
                                                                  )) arrivals))
     :scenario scenario}))
    

;;Compute the statistics for each scenario in a given run, labeling the stats with 
;;the run and the demandtype.  This information collectively provides the fundamental 
;;coach's card data for the run.
(defn coach-stats [run recs] ;run is the mpi root dir
  (let [drecs (tbl/table-records (tbl/tabdelimited->table (slurp (str run "DemandTrends.txt"))
                                   :schema schemas/dschema :parsemode :noscience))
        sampler (proc.core/sample-demand-trends drecs)]
   (for [[[scenario unittype demandtype] xs] (group-by (juxt :DemandGroup :SRC :DemandType) recs)] 
     (assoc (scenario-stats scenario sampler xs) :run run :demandtype demandtype :unittype unittype))))


;;Rendering to existing CC Format
;;===============================
;;The coach's card format is not table-friendly, like our current output is.
;;However, we can use the intermediate tables [phase-table, late-table, and reduced-table] 
;;to build the same output that's currently being presented.  Since this is a distillation, 
;;it's preferable to separate the final rendering from the underlying data it's derived from.

;;We'll use some built-in text formatting magic to help us out.
;;let's print out our arrivals here...
;;late-arrivals come in the form of fill-records (they're filtered from the first-arrival, and then ;;the later-arrivals).
;;The format for our arrivals is [q?x"d"+t?]...
(defn print-cell
  ([ready qty t] (cl-format nil "~Ax~A/D+~A" qty ready t))
  ([qty t]       (cl-format nil "~AxD+~A"    qty t)))

;;Since the data is fundamentally the same (they're both derived from sequences of filldata records), 
;;we can group them generically and use the grouping for both operations.  Reduced readiness contains more data 
;;than arrivals, so we'll use it as the default.
(defn group-reduced [recs]
  ;;groups our records...
  (for [[[ready t] xs] (group-by (juxt :TrainLevel :DaysAfterPHII) recs)]
    (let [qty (count xs)]
      [ready qty  t])))

;;just like group-reduced, but drops the readiness 
(defn group-arrivals [recs] (map #(subvec % 1) (group-arrivals recs))) 

;;This isn't the best way to go, but it's fast enough.  It'll detect whether 
;;we have two or three args in the vector and apply the right print path. 
(defn print-cells [xs]
  (let [cells (str (clojure.string/join " " (map (fn [x] (apply print-cell x)) xs)))]
    cells))

(def ^:dynamic ph-orders proc.constants/bundle-1)

(defn get-CA-phases [ph-orders & {:keys [excluded-phases] :or {excluded-phases consts/excluded-phases}}]
  (reduce-kv (fn [m k all-phases] (let [stop-point (get excluded-phases (str/upper-case k))]
                                        (assoc m k (if stop-point
                                                     (for [phase all-phases :while (not= phase stop-point)] phase)
                                                     all-phases))))
                                        {} ph-orders))

(defn sort-maps-by 
  "Sort a sequence of records (maps) by calling key on each record.  Order is determined by
   the order of the values in vals."
  [maps key vals]
  (util/sorted-by-vals [key] vals maps))

;roman numeral sort might be nice for surge 3.
(defn get-phases 
  "Returns a map of {'demandgroup' [phase1 phase2 phase3...]} and hopefully the 
phases are in a logical order"
  [root & {:keys [custom-sort] :or {custom-sort ph-orders}}]
  (->> (spork.util.table/tabdelimited->records 
         (slurp (str root "AUDIT_DemandRecords.txt")) :parsemode :noscience 
         :schema proc.schemas/drecordschema :keywordize-fields? true)
    (into [])
    (filter (fn [r] (:Enabled r)))
    (reduce (fn [acc {:keys [Operation DemandGroup]}] 
              (assoc acc (str/upper-case DemandGroup) 
                     (conj (get acc (str/upper-case DemandGroup) #{}) 
                           (str/upper-case Operation)))) {})
    (reduce-kv 
      (fn [m k v] 
        (let [sorted (get custom-sort (str/upper-case k))
              _ (when (and sorted (not= (set sorted) v)) 
                  (throw (Exception. 
                           (str "Custom-sort doesn't match all phases for demandgroup " (str/upper-case k)))))]
                 (assoc m k (if sorted sorted (sort v))))) {})))
  
;;Building Output Tables
;;======================
(defn expand-phases  [max-fills scenario included-phases]
  (let [exp-phases (for [[phase {:keys [filled] :as best-fill} {:keys [required] :as most-required}] max-fills]
                     {:phase (str/upper-case phase)
                      :required required
                      :filled filled})
        leftovers (map (fn [phase] {:phase (str/upper-case phase)
                                    :required 0
                                    :filled 0}) (apply disj (set included-phases) (map :phase exp-phases)))]
    (sort-maps-by (concat exp-phases leftovers) :phase included-phases)))

(defn check-phase [included-phases r] 
  (contains? (set included-phases) (str/upper-case (:operation r))))

(defn stats->tables [{:keys [run scenario demandtype unittype max-fills late-arrivals reduced-readiness] :as cstats} included-phases]
  (let [root-record  (select-keys cstats [:run :scenario :demandtype :unittype])
        phase-stats  (expand-phases max-fills scenario included-phases)
        arrivals-entry (print-cells (group-reduced (filter (partial check-phase included-phases) late-arrivals))) ;could remove filter for all late arrivals
        reduced-entry  (print-cells (group-reduced (filter (partial check-phase included-phases) reduced-readiness)))
        unit-supply (supply/supply-by-compo run)
        pretty-entries (partition 2
                           (concat [:run run
                                    :demandgroup scenario
                                    :demandtype demandtype
                                    :unittype unittype
                                    :inventory (get unit-supply unittype)]
                                    
                                   
                                (mapcat (fn [{:keys [phase required filled]}]
                                          [;:phase phase  
                                           ;:required required      
                                           ;now these aren't keywords.  Will need to check this if read in a coach card.
                                           ;there really is no required for substitutions
                                           (str "Req " phase) (when (= demandtype unittype) required) 
                                           (str "Fill " phase) filled])
                                           ;:filled filled
                                           
                                        phase-stats)
                                [:reduced-readiness reduced-entry
                                 :late-arrivals arrivals-entry]))]                               
    {"phase-table"   (tbl/records->table (for [r phase-stats] ;concat with scenario, demandtype as prefix
                                          (merge root-record r)))
     "late-table"    (tbl/records->table (for [r late-arrivals]
                                          (merge root-record r)))
     "reduced-table" (tbl/records->table (for [r reduced-readiness]
                                           (merge root-record r)))
     "card"          pretty-entries}))
     
     

(defn spit-cards [card-path cards]
  (let [headers (map first (first cards))]
        
    (->> (cons headers (for [c cards] (map second c)))
      (map (fn [xs] (str/join \tab xs)))
      (str/join \newline)
      (spit card-path))))
 
;;we can compute our "Final" phase-table by condensing the fills arrivals and reduced 
;;down into text blocks, and joining the phase-table with the inventory, as well as the 
;;text from the late-table and reduced-table.

;;we're basically building a set of tables for each coach's card.
;;we can process them further to make the jacked up output that's currently ;;standardized.

;it might be nice to have all srcs in one table for a demandgroup. 
;TODO: looks like we aren't spitting out required 0 when there aren't
;any requirements.  Will work on this
;can probably put more paths as optional for more runs....

(defn sort-cstats [{:keys [unittype demandtype]}]
  (let [combo (str demandtype unittype)]
   (if (= unittype demandtype) 
     demandtype
     combo)))

(defn do-coach-card [path excludes & {:keys [srcs] :or {srcs false}}]
  (let [ordered-phases (get-phases path)
        records (get-coach-records path excludes :srcs srcs)
        cstats  (coach-stats path records)
        included-phases (get-CA-phases (get-phases path :custom-sort ph-orders)) ;map by demandgroup
        ]
    ;now by scenario
    (doseq [[scenario cstats] (group-by :scenario cstats)]
      (let [cards (atom [])
            card-path (str path "coachcards/card_" scenario ".txt")
            _ (io/hock card-path "")]
        ;want to sort so demantypes are in a consistent order
        (doseq [{:keys [scenario demandtype unittype] :as cstat} (sort-by sort-cstats cstats) 
                [table-name table]  (stats->tables cstat (get included-phases (str/upper-case scenario)))]
          (let [_ (when (= table-name "card") (swap! cards conj table))
                target (str path "coachcards/" table-name "_" unittype "_" demandtype "_" scenario ".txt") 
                _ (when (not= table-name "card") (io/hock target ""))] 
            (println ["spitting out " table-name " to " target])
            (if (= table-name "card")
              nil
              (util/spit-table target table))))
            
        (spit-cards card-path @cards)))))
        

(defn coach-cards-from 
  "Prepares input data for and spits coaches' cards. excludes are demandgroups we want to filter out and must be all caps. You can also
modify the order of scenarios in proc.constants and you can set a final phase for a ccard with excluded-phases ih proc.constants"
  [path & {:keys [excludes] :or {excludes #{"NOTUTILIZED" ;"HLD" 
                                            "UNGROUPED"}}}]
  (do (fills/fills+fields path)
    (do-coach-card path excludes)))

;todo: allow the user to specify ph-orders, T level conditions for fills+fields, and cutoff points for first arrivals in get-CA-phases
(defn cards-from-scratch 
  "To make coach's cards for multiple directories given only the Marathon audit trail, 
   try (cards-from-screatch 'filepath1/' 'filepath2/' 'filepath3/')"
  [& paths]
  (doseq [path paths]
    (do (ex/run-sample! path :eachsrc true)
      (coach-cards-from path))))

;;TODO
;;====
;;Join supplyrecords from a run to get the inventory. - Iventories are done.  Just merge them with ccard data via Apache.
;;Can we automatically generate the Excel coaches' cards using Apache?  This would allow easier creation of coaches' cards
;;without copy and paste errors and also allow the user to edit the cards via excel if they would like.
(def headers3 ["Size of Army" "Unit Type" "Inventory\nAC/NG/AR\n//Total"])

;probably want to put the card at a location. Thus rowstart and colstart
;This is getting longish.  I suppose another way would be to copy the named range, but I don't think too much more of the
;formatting remains constant.

(defn ccard-format-at [path & {:keys [rowstart colstart] :or {rowstart 0 colstart 0}}] 
  (let [wb (new XSSFWorkbook)
        fileout (new FileOutputStream 
                     (str path "allccards.xlsx"))
        cardsheet (.createSheet wb "Coaches' Cards") ;if you don't put a sheet in the workbook, you can't open it with Excel
        blackrow (.createRow cardsheet (+ rowstart 4))
        blackstyle (doto (.createCellStyle wb)
                     (.setFillBackgroundColor (.getIndex IndexedColors/BLACK))
                     (.setFillPattern CellStyle/SOLID_FOREGROUND))   ;nothing will show in cell if you don't set the FillPatern
        _ (doseq [c (range 3)]
            (let [cell (.createCell blackrow (+ colstart c))
                  _ (.setCellStyle cell blackstyle)]))
        headrow (.createRow cardsheet (+ rowstart 1))
        headfont (doto (.createFont wb)
                   (.setFontHeightInPoints 9)
                   (.setFontName "Arial")
                   (.setBold true))
        headstyle (doto (.createCellStyle wb)
                    (.setVerticalAlignment CellStyle/VERTICAL_CENTER)
                    (.setAlignment CellStyle/ALIGN_CENTER)
                    (.setFont headfont))
        headcell       (doto (.createCell headrow (+ colstart 0))
                         (.setCellValue (.createRichTextString (.getCreationHelper wb) "Size of Army"))
                        (.setCellStyle headstyle))
        _ (.addMergedRegion cardsheet (new CellRangeAddress (+ rowstart 1) (+ rowstart 3) (+ colstart 0) (+ colstart 0)))
        _ (.autoSizeColumn cardsheet colstart true)]
         
    (do 
      (.write wb fileout) 
      (.close fileout))))

