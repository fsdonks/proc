(ns proc.tests
  (:require [proc.example :as ex]
            [spork.util.table :as tbl]
            [clojure.test :refer :all]))

;;stas were removed for unclass transfer.

;The following percent stats records have been verified in pivot tables.  I included the day caluclations after
;and just hand-checked a couple.  These were easy to break out from the percentage and should be hard to mess up:
(def somestats)

(let [statsrecs (-> (ex/build-fill-tables ex/behruns :BCT)
                  (ex/fill-table ex/more-stats :name)
                  (tbl/table-records))
      results (into #{} statsrecs)]
  (deftest stats
      (is (=  results somestats))))



