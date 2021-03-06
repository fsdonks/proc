(ns proc.tests
  (:require [proc [example :as ex]
             [stats :as stats]
             [util :as util]
             [core :as core]
             [demandanalysis :as demand]]
            [spork.util [table :as tbl]
             [io :as io]]
            [clojure.test :refer :all]
            [proc.stats :as stats]
            [proc.constants :as consts]
            [proc.fillsfiles :as fills]))

;;the initial path for running tests.
;;This path will have any ephemeral data overwritten
;;every time we run tests.
;;If no test path is bound, we'll try to find the
;;first known test project on the local system,
;;and run tests from there.  This will let us
;;add test projects on different machines without
;;having to worry about locality.
(def ^:dynamic *test-path* nil)

(defn backslashify [^String s]
  (.replace s "/" "\\"))
;;add test paths here...
;;note: paths with spaces in the name don't get parsed nicely...
(def paths
  ["C:/Users/1143108763.C/testdata/"
   "V:/dev/tests/Case 3 - No Phase IV Capacity - After flys/"   
   ])

;;looks for the first known test path, searching
;;paths in order.  If the path doesn't exist it gets
;;skipped.


(defn path! []
  (if *test-path* *test-path*
      (util/path! paths)))

;(run-tests) will run all tests in this namespace from this namespace.  
;You can (do (require ['clojure.test :refer :all]) (run-tests 'proc.tests)) from any other ns as well.

;The following percent stats records have been verified in pivot tables.  I included the day caluclations after
;and just hand-checked a couple.  These were easy to break out from the percentage and should be hard to mess up:
#_(def somestats consts/somestats)


;;Tom: What the hell is this?  Tests fail due to absence of sampledata, can only assume this is
;;Craig Special(tm)

;;tests our stats function
#_(deftest stats
  (is (let [statsrecs (-> (stats/build-fill-tables consts/behruns    :BCT)
                          (stats/fill-table        stats/more-stats :name)

                          (tbl/table-records))
            results (into #{} statsrecs)]
        (=  results somestats))))

(deftest demandmap-test
  (is (let [dpath (str (path!) "AUDIT_DemandRecords.txt")]
        (not (empty? (core/load-demand-map dpath))))
      "Shouldn't have any problems loading the demandmap."))          

(defn try-deployments []
  (let [p (str (path!) "/AUDIT_Deployments.txt")]
    (core/load-deployments p)))
 
;I'll probably bundle the allfillsfast.txt file as a seq of hashes and then package up the marathon run (to include
;allfillfast.txt, barfills, and fills folder (sand will be pending) as a zip for tom.
;Used to verify that barfills.txt was the same before and after the first implementation of flyrecords

;__________________
;;we probably don't want to do these tests all the time, particularly the tests that perform
;;a bunch of io for us.  So, lets' break it up.

;;This lets us get our supposed hashfile that is the frame of
;;reference for comparing fills.


;;once our test is completed, we can verify that the hashes match our known
;;frame of reference.
(deftest hash-test
  (is (do (println [:testing :run-sample])
          (ex/run-sample! (path!) :eachsrc true)
          :done)
      "Should complete with no errors")
  (is  (do (println [:testing :all-fills])
           (fills/all-fills (path!))
           :done)
       "Testing to ensure our fill computations completed.")
  (is (nil? (util/with-rdrs [hashfills (str (path!) "hashes-allfillsfast-case3.txt")
                             runfills  (str (path!)  "allfillsfast.txt")]
              (println [:testing-hashes])
              (first  (util/hash-misses (map util/string->int (line-seq hashfills))
                                        (line-seq runfills)))))       
      "test all fills to make sure they match this somewhat verified case. Modify newrun as necessary"))

(defn incomplete-fills  
  "Do all rows have 40 fields? They should"
  []
  (util/with-rdrs [runfills  (str (path!)  "allfillsfast.txt")]
              (println [:testing :all-fills :for :completeness])
               (doall (->> (map tbl/split-by-tab (line-seq runfills))
                       (map-indexed vector)
                       (filter (fn [[idx x]]
                 (when (not= 40 (count x)) [idx x])))
                       ))))
  
(defn right-hash? 
  "Do I have the right file that was hashed?"
  [filep]
  (nil? (util/with-rdrs [hashfills (str (path!) "hashes-allfillsfast-case3.txt")
                             runfills  filep]
              (println [:testing-hashes])
              (first  (util/hash-misses (map util/string->int (line-seq hashfills))
                                        (line-seq runfills))))))
  
(defn test-lines
  "Given line-seqs for file1 and file2, returns [linenumber false] for every line that doesn't match between the two files."
  [file1 file2]
  (->>  (map (fn [l r] (= l r)) file1 file2)
                (map-indexed vector)
                (filter (fn [[idx x]]
                          (not x)))))

(defn compare-files
  "returns nil if the files are the same. Otherwise, returns [linenumber false] for the first line that is different."
  [filepath1 filepath2]
  (util/with-rdrs [f1 filepath1
                   f2  filepath2]
    (first (test-lines (line-seq f1) (line-seq f2)))))

(defn compare-directories
  "check if all text files in dir1 match the text files under the same name in dir2. Returns a sequence of [filename nil] for files that match, [filename numDifferentLines] for files that don't have the same number of lines, [filename [linenumber false]] for files that don't match, and [filename :notfound] if the file doesn't exist in dir2. Use name-filter to filter the names of the files."
  [dir1 dir2 & {:keys [name-filter] :or {name-filter (fn [n] true)}}]
  (let [filenames (filter name-filter (seq (.list (clojure.java.io/file dir1))))]
    (for [f filenames]
      (if (io/fexists? (str dir2 f))
        (let [linediff (util/with-rdrs [f1 (str dir1 f) f2 (str dir2 f)]
                         (- (count (line-seq f1)) (count (line-seq f2))))]
          (if (= linediff 0)
            [f (compare-files (str dir1 f) (str dir2 f))]
            [f linediff]))
        [f :notfound]))))

  
;added demand-type or unit-type fills views.  fills and sand should remain identical if no subtitutions?
(deftest small-test
  (is (do (println [:testing :fills :byeDemandType?])
          (ex/run-sample! (path!) :eachsrc true :byDemandType? true)
          :done))
  (is  (do (println [:testing :all-fills])
           (fills/all-fills (path!) :outpath "allfillsfastdtype.txt")
           :done))
  (is (nil? (util/with-rdrs [utype-all-fills (str (path!) "allfillsfast.txt")
                             dtype-all-fills  (str (path!) "allfillsfastdtype.txt")]
              (println [:testing-fills-by-demand-type])
              (first (test-lines (line-seq utype-all-fills) (line-seq dtype-all-fills)))))
      "byDemandType? fills should be the same as byDemandType? false fills as long as no substitutions"))



(defn get-line 
  "Returns a string of line number number from a file pointed to by path string."
  [number path]
  (util/with-rdrs [rdr path]
    (-> (line-seq rdr)
      (nth number))))

  
(defn wrong-line 
  "well, if I know I have two lines that don't match, I want to see what they look like"
  []
  (util/with-rdrs [hashfills (str (path!) "hashes-allfillsfast-case3.txt")
                   runfills  (str (path!)  "allfillsfast.txt")]
    (println [:testing-hashes])
    (first  ((fn [hashed xs] (->> (map (fn [l r] (if (= l (hash r)) true [l r])) hashed xs)
                               (map-indexed vector)
                               (filter (fn [[idx x]]
                                         (when (vector? x) idx))))) (map util/string->int (line-seq hashfills))
                                                                (line-seq runfills)))) )

(defn last-demand 
  "Returns a map of demand name to last demandtrend record for that demand name.  Used to verify that proc.core/fill-seq2 is processing
last-day unmet record for a demand name properly."
  [dtrendpath]
  (->> (tbl/tabdelimited->records dtrendpath :schema proc.schemas/dschema :parsemode :noscience)
       (reduce (fn [acc r] (assoc acc (:DemandName r) r)) {} )))

(defn same-delta-ts?
  "One spark chart assumption that we make is that the delta t for all demands on a given day is the same.  Given the path
  to a demandtrends file, this will return true or false for that assumption."
  [])

;;---------------------------------------------
;use sets in case order every changes.  order shouldn't matter.
(deftest peaks-from-test
  (is (= (set (demand/peaks-from "test/resources/peak_times_by_1/" :group-fn (fn [s] s)))
         (set [{:peak 2, :intervals [[721 721]], :group "77202K100", :period "PreSurge"}
               {:peak 4, :intervals [[1636 1636]], :group "77202K100", :period "Surge"}
               {:peak 3, :intervals [[1801 1824]], :group "77202K100", :period "PostSurge"}]))
      ;;My visual check failed at first.  Turns out, proc was right and I wasn't.
      "Verified this visually, so wanted to make a regression test."))

  ;;we want to make sure our tests run in-order.
  ;;specifically, we need the tests to happen in a bundle, like run-sample, then
  ;;all-fills, etc.
  
  ;;possibly rip this out.
  ;;The order of precedence seems to be running correctly already just using
;;run-test.
  (defn test-ns-hook []
    ;lacking NIPR sample data for these first 4 tests.  
    #_(stats)
    ;(demandmap-test)
    ;(hash-test)
    ;(small-test)
    (peaks-from-test)
    )

