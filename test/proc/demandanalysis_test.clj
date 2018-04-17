(ns proc.demandanalysis-test
  (:require [clojure.test :refer :all]
            [proc.demandanalysis :refer :all]
            [spork.util.table :as tbl]
            [proc.supply :as supply]))

(defn add-path [test-name]  (str "test/resources/" test-name))

(def test-paths
  [(add-path "snapchart_v7/")])

(def flatchartdata
{:interest :text
 :OItitle :text
 :demand :text
 :policy :text
 :period :text
 :periodpolicy :text
 :inv-combo :text
 :RAinv :int
 :NGinv :int
 :ARinv :int
 :capacity :int
 :peak :int
 :fill :int
 :nodemand :boolean}
  )

(defn flatdata-records
  "Given the root to a flatchartdata tab delimited text file from the VBA implementatio of TADMUDI, returns the records
  and then only keeps the fields we want to compare with the sparkchart data."
  [root]
  (map (fn [r] (dissoc r :nodemand
                       :inv-combo
                       :periodpolicy
                       :demand
                       :OItitle))
                       (into [] (tbl/tabdelimited->records root :schema flatchartdata))))

  
(defn flatdata-from
  "From a Marathon audit trail, using sparkchart functions, compute a sequence of FlatChartData records like from TADMUDI."
  [root & {:keys [group-fn] :or {group-fn (fn [s] "All")}}]
  (let [capacities (supply/capacity-by root :group-fn group-fn)
        supply (supply/by-compo-supply-map-groupf root :group-fn group-fn)
        policies (supply/policy-string root)
        inscope? (fn [r] (contains? supply (group-fn (:SRC r))))]
    (for [{:keys [group period peak] :as p} (peaks-from root :group-fn group-fn :demand-filter inscope?)
          :let [capacity (supply/round-to 0 (capacities [group period]))]]
      (assoc (select-keys p [:peak :period]) :capacity capacity
             :RAinv (get (supply group) "AC" 0)
             :NGinv (get (supply group) "NG" 0)
             :ARinv (get (supply group) "RC" 0)
             :policy policies
             :fill (if (= peak 0) 0 (supply/round-to 0 (* 100 (/ capacity peak))))
             :interest group))))

(defn run-tests-nout
  "When you don't want to have the expected and actual outputs printed to the REPL, you can use this instead of run-tests"
  []
  (defmethod report :fail [m]
    (with-test-out
      (inc-report-counter :fail)
      (println "\nFAIL in" (testing-vars-str m))
      (when (seq *testing-contexts*) (println (testing-contexts-str)))
      (when-let [message (:message m)] (println message))
      (println "expected: something else")
      (println "  actual: not else")))
  (let [res (run-tests)]
    (defmethod report :fail [m]
      (with-test-out
        (inc-report-counter :fail)
        (println "\nFAIL in" (testing-vars-str m))
        (when (seq *testing-contexts*) (println (testing-contexts-str)))
        (when-let [message (:message m)] (println message))
        (println "expected:" (pr-str (:expected m)))
        (println "  actual:" (pr-str (:actual m)))))
    res))

(deftest compare-tadmudis
  (doseq [t test-paths
          :let [sparks (set (flatdata-from (str t "audit/") :group-fn (fn [s] s)))
                flats (set (flatdata-records (str t "flatchartdata.txt")))]]
    (testing "Test to see if the records of sparkcharts match the records of a flatdata tab-delimited text file. 
Input data should be the same between spark and flatdata."
      (is (= (count sparks) (count flats)))
      (is (= sparks flats))
      )))

;;why not matching?
;;demands and periodrecords are the same
;;peak should be 4 for SRC "05315K000"
;;duplicate demand records.  arrrrgh!
;;taa20-24 vignette consolidated included the same SRC in two records with a quantity
;;of one.  How should we interpret this?
;;now, duplicates are counted in vba tadmudi but not in sparkcharts. Don't take action until
;;next year. when ask FM how duplicates should be interpreted (warn about duplicates in sparks
;;but no need in vba tadmudi since we can check the demand before loaded into tadmudi).
;;looks like we only kept one of the duplicate records for 21-25, so duplicate diff between
;;tadmudi and sparkcharts probably didn't occur.
;;For this test case on NIPR,
;;1) warn about dupes, but run the code. done with duplicate-demands
;;2) make all duplicates non dupes-done by unique priority for each record.
;;what is a demand name?  Priority_vignette_SRC_[startday...endday]
(comment
(def tp (add-path "snapchart_v7/"))
(def sparks (set (flatdata-from (str tp "audit/") :group-fn (fn [s] s))))
(def flats (set (flatdata-records (str tp "flatchartdata.txt"))))
(require '[proc.util :as util])
(first (util/duplicate-demands (util/enabled-demand (str tp "audit/"))))

(count sparks)
885
(count flats)
885
(count (clojure.set/intersection sparks flats)) 
777
(first (clojure.set/difference sparks flats))
{:peak 3, :period "PreSurge", :capacity 2, :RAinv 7, :NGinv 2, :ARinv 0, :policy "1:2/1:5->1:0/1:2->1:1/1:4", :fill 67, :interest "05315K700"}
(filter (fn [r] (= (:interest r) "05315K700")) flats)
({:ARinv 0, :capacity 4, :fill 80, :policy "1:2/1:5->1:0/1:2->1:1/1:4", :interest "05315K700", :peak 5, :period "PostSurge", :NGinv 2, :RAinv 7} {:ARinv 0, :capacity 7, :fill 100, :policy "1:2/1:5->1:0/1:2->1:1/1:4", :interest "05315K700", :peak 7, :period "Surge", :NGinv 2, :RAinv 7} {:ARinv 0, :capacity 2, :fill 50, :policy "1:2/1:5->1:0/1:2->1:1/1:4", :interest "05315K700", :peak 4, :period "PreSurge", :NGinv 2, :RAinv 7})
(peaks-from (str tp "audit/") :group-fn (fn [s] {"05315K700" "05315K700"}))
({:peak 1214, :intervals [[540 547]], :group ["05315K700" "05315K700"], :period "PreSurge"} {:peak 5604, :intervals [[913 921]], :group ["05315K700" "05315K700"], :period "Surge"} {:peak 4462, :intervals [[1801 1824]], :group ["05315K700" "05315K700"], :period "PostSurge"})
(peaks-from (str tp "audit/") :group-fn {"05315K700" "05315K700"})

;;solved
)

