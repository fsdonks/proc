;;Compatibility layer to extend spork.util.table usage
;;into incanter datasets (as of 1.9.1).
;;Since datasets are now based on an API in clojure.core.matrix,
;;we can extend the API to use our table functions.  This should
;;eliminate the trouble we've have processing incanter
;;datasets effeciently, since we can parse spork tables
;;using schemas.
(ns proc.dataset
  (:require [spork.util [table :as tbl]]
            [clojure.core.matrix.protocols :as mp]
            [clojure.core.matrix.implementations :as imp]
            [clojure.core.matrix.impl.dataset :as dsi]
            [clojure.core.matrix.dataset :as ds])
  (:import [clojure.lang IPersistentVector]))

;;In theory, these get us compatible dataset implementations for spork.util.table...

(defn table-col [t i]
  (if (number? i)
    (nth (tbl/table-columns t) i)
    (tbl/field-vals (tbl/get-field i t))))

(extend-type spork.util.table.column-table
;;"Protocol to support getting slices of an array.  If implemented, must return either a view, a scalar 
;; or an immutable sub-matrix: it must *not* return copied data. i.e. making a full copy must be avoided." 
  mp/PMatrixSlices
;;"Gets a row of a matrix with the given row index."
  (get-row [m i]
    (tbl/nth-row m i))
;;"Gets a column of a matrix with the given row index."
  (get-column [m i]
    (table-col m i))
;;"Gets the major slice of an array with the given index. For a 2D matrix, equivalent to get-row"
  (get-major-slice [m i]
    (mp/get-row m i))
;;"Gets a slice of an array along a specified dimension with the given index."  
  (get-slice [m dimension i]
    (case dimension
      0 (tbl/nth-row m i)
      1 (table-col m i)             
    (throw (Exception. (str [:invalid-slice dimension i])))))
 
  mp/PMatrixRows 
;;"Protocol for accessing rows of a matrix"
;;"Returns the rows of a matrix, as a seqable object"
  (get-rows [m] (tbl/table-rows m))
 
  mp/PMatrixColumns
;;"Protocol for accessing columns of a matrix"
;;"Returns the columns of a matrix, as a seqable object"  
  (get-columns [m] (tbl/table-columns m))

  mp/PDimensionLabels 
  (label [m dim i] 
    (let [dim (long dim)] 
      (cond 
        (== 1 dim) (nth (tbl/table-fields m) i)
        :else nil))) 
  (labels [m dim] 
    (let [dim (long dim)] 
      (cond 
        (== 1 dim) (tbl/table-fields m) 
        :else nil)))
  mp/PColumnNames     
  (column-name [m i]  (nth (tbl/table-fields m) i)) 
  (column-names [m]   (tbl/table-fields m))
  mp/PImplementation
  (implementation-key [m] :table) 
  (meta-info [m] 
    {:doc "spork.util.table implementation for datasets"}) 
  (new-vector [m length] 
    (mp/new-vector [] length)) 
  (new-matrix [m rows columns] 
    (let [col-indexes (range columns)] 
      (dsi/dataset-from-columns
       col-indexes 
       (for [i col-indexes] 
         (mp/new-vector (imp/get-canonical-object) rows))))) 
  (new-matrix-nd [m shape] 
    (mp/new-matrix-nd [] shape)) 
  (construct-matrix [m data] 
    (if (== 2 (long (mp/dimensionality data))) 
      (dsi/dataset-from-array data) 
      nil)) 
  (supports-dimensionality? [m dims] 
    (<= 1 (long dims) 2))
  mp/PConversion
  (convert-to-nested-vectors [ds] 
    (let [cols (mapv mp/convert-to-nested-vectors (tbl/table-columns  ds))] 
      (mp/transpose cols)))
 #_mp/PColumnIndex 
  #_(column-index [ds column-name] 
    (when-let [cnames (mp/column-names ds)] 
      (let [cnames ^IPersistentVector (vec cnames)] 
        (and cnames (first (tbl/find-where #{column-name} cnames))))))
  mp/PColumnSetting 
  (set-column [ds i column] 
    (let [scol   (mp/get-column ds 0) 
          column (mp/broadcast-like scol column)]
      (assoc ds
             :columns
             (assoc (tbl/table-columns  ds) i column))))
  mp/PDimensionInfo 
  (dimensionality [m] 2) 
  (is-vector?     [m] false) 
  (is-scalar? [m] false) 
  (get-shape [m] [(tbl/count-rows m) (count (tbl/table-fields m))])
  (dimension-count [m dim] 
    (.nth ^IPersistentVector (.shape m) (long dim)))
  )
  



(extend-type spork.util.table.column-table  
  ;;Datasets for making tables play nice with incanter...
  mp/PDatasetImplementation
  ;"Returns a persistent vector containing columns in the same order they are placed in the dataset"
  (columns [ds]
    (tbl/table-columns ds))
  ;"Produces a new dataset with the columns in the specified order"
  (select-columns [ds cols]
    (tbl/select-fields cols ds))
  ;"Produces a new dataset with specified rows"
  (select-rows [ds rows]
    (let [rf (set rows)
          empty-cols (mapv (fn [_] []) (:fields ds))]
      (->>   (tbl/table-rows ds)
             (map-indexed (fn [idx x]
                            (when (rf idx) x)))
             (filter identity)
             (tbl/conj-rows  empty-cols)
             (assoc ds :columns))))
  ;"Adds column to the dataset"
  (add-column [ds col-name col]
    (tbl/conj-field [col-name col] ds))
  ;;"Returns a dataset created by combining columns of the given datasets.
  ;;In case of columns with duplicate names, last-one-wins strategy is applied"
  (merge-datasets [ds1 ds2]
    (let [ls     (tbl/table-fields ds1)
          rs     (mp/column-names ds2)
          left?  (set ls)
          right? (set rs)]
      (-> (for [f  (dedupe (concat ls rs))]
            [f  (if (right? f)
                  (mp/get-column ds2 f)
                  (tbl/field-vals (tbl/get-field f ds1)))])
          (vec)
          (tbl/conj-fields tbl/empty-table))))
  ;;"Renames columns based on map of old new column name pairs"
  (rename-columns [ds col-map]
    (tbl/rename-fields col-map ds))
  ;;"Replaces column in a dataset with new values"
  (replace-column [ds col-name vs]
    (tbl/conj-field [col-name vs] ds))
   ;;"Returns a dataset created by combining the rows of the given datasets"
  (join-rows [ds1 ds2]
    (let [ls (tbl/table-fields ds1)
          rs (mp/column-names ds2)
          lset (set ls)
          rset (set rs)]
      (if (= (into #{} ls)
             (into #{} rs))
        (let [cols   (mapv #(mp/get-column ds2 %) rs)
              rcount (count (first cols))]
          (->> (tbl/conj-rows (tbl/table-columns ds1)
                                           (for [i (range rcount)]
                                             (mapv #(nth % i) cols)))
               (assoc ds1 :columns)))
          (throw (Exception. (str "trying to join rows from tables with different column-names: "
                                 [ls rs]))))))
  ;;"Returns a dataset created by combining the columns of the given datasets"
  (join-columns [ds1 ds2]
    (let [ls (tbl/table-fields ds1)
          rs (mp/column-names ds2)]
      (if (empty? (clojure.set/intersection (set ls) (set rs)))
        (mp/merge-datasets ds1 ds2)
        (throw (Exception. (str "cannot merge duplicate fields"))))))

  #_mp/PDatasetMaps
  ;"Returns map of columns with associated list of values"
  (to-map [ds]
    (zipmap (tbl/table-fields ds)
             (tbl/table-columns ds)))
                   
  ;"Returns seq of maps with row values"
  (row-maps [ds] (tbl/table-records ds)))


(comment ;testing dataset api..
  (def res (tbl/make-table   {:name ["tom" "bill" "joe"], :age [1 2 3]}))
  (def other (tbl/make-table {:name ["tom" "bill" "joe"], :age [1 2 3]}))

  (ds/join-columns res other)
  ;#spork.util.table.column-table{:fields [:name :age :year :type], :columns [["tom" "bill" "joe"] [1 2 3] [10 20 30] [:a :b :c]]}
  (ds/add-column res :blah [:A :a :a])
  ;#spork.util.table.column-table{:fields [:name :age :blah], :columns [["tom" "bill" "joe"] [1 2 3] [:A :a :a]]}
  (ds/add-column res :blah [:A :a :a :a])
  ;#spork.util.table.column-table{:fields [:name :age :blah], :columns [["tom" "bill" "joe"] [1 2 3] [:A :a :a]]}
  (ds/column-index res :name)
  ;0
  (ds/column-index res :age)
  ;1
  (ds/except-columns res [:name])
  ;#spork.util.table.column-table{:fields [:age], :columns [[1 2 3]]}
  (ds/replace-column res :age [0 0 0])
  ;#spork.util.table.column-table{:fields [:name :age], :columns [["tom" "bill" "joe"] [0 0 0]]}
  (ds/row-maps (ds/replace-column res :age [0 0 0]))
  ;({:name "tom", :age 0} {:name "bill", :age 0} {:name "joe", :age 0})
  (ds/select-rows res [1])
  ;#spork.util.table.column-table{:fields [:name :age], :columns [["bill"] [2]]}
  (ds/select-rows res [0])
  ;#spork.util.table.column-table{:fields [:name :age], :columns [["tom"] [1]]}
  (ds/select-rows res [0 1])
  ;#spork.util.table.column-table{:fields [:name :age], :columns [["tom" "bill"] [1 2]]}
  )
