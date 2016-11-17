(ns proc.interests
  (:require [clojure.pprint :as ppr]
            [clojure.core.reducers :as r]))

;For now, it is up to the caller to 
;1) make sure that the srcs in an interest are all unique (use are-unique to help with that)
;2) Make sure that the subset of interests (subints) is in fact a subset and doesn't contains a keyword not found in interests.

;If defining interests here becomes too laborious or difficult for the end user, we could also check the MPI directory
;for a text file containing some interests first.

(defn are-unique? 
  "All SRCs for a map of interests have to be unique.  This checks that condition"
  [interests]
  (apply distinct? (mapcat second (vals interests)))) 
  
(defn ints-in? 
  "When reducer is set to every?, returns true if all subints are found in interests. Returns false otherwise.
When reducer is set to remove, returns a sequence of subints that aren't in interests."
  [interests subints & {:keys [reducer] :or {reducer every?}}]
  (let [ints (set (keys interests))]
    (reducer (fn [int] (contains? ints int)) subints)))

(defn str->seq 
  "Take a string and returns a sequence of number if the string character is a number and string if the string character is not
a number."
  [string]
  (let [split (clojure.string/split string #"")
        removal (if  (= (first split) "") (rest split) split) ; this behaves differently on some machines.
        func (fn [str] (let [rs (when (not= str " ") (read-string str)) ] (if (number? rs) rs str)))]
    (map func removal)))

(defn src9? 
  "Takes a string, src, and checks to see if it conforms to the standards for a 9-digit SRC.  Returns true if it does."
  [src]
  (let [srcseq (str->seq src)]
    (and (= (count srcseq) 9) (every? number? (take 5 srcseq)) (string? (nth srcseq 5 false)))))  


(defn srcs->interests 
  "takes a sequence of srcs as strings and returns an interest for each of these srcs"
  [srcs]  
  (reduce (fn [acc src] (assoc acc (keyword src) [src [src]])) {} srcs))

(def ^:dynamic *include-fn* src9?)

(defn ints-for-srcs 
  "Given a Marathon directory, this function will take look for all fills files with an SRC as the title and return interests
for those SRC fills files.  This enables the caller to call do-charts-from with these interests.  Useful for when run-sample!
was called with :eachsrc true."
  [root & {:keys [srconly?]}]
  (let [path (str root "fills/")
        filenames (vec (.list (clojure.java.io/file path)))
        splits (map (fn [fname] (clojure.string/split fname #"\.")) filenames)
        srcs (map first (filter (fn [split] (and (*include-fn* (first split)) (= "txt" (last split)))) splits))]        
    (if srconly?
      srcs
      (srcs->interests srcs)
      )))

(defn enabled-srcs [root]
  "Get the enabled srcs from the demand records"
  (->> (spork.util.table/tabdelimited->records (slurp (str root "AUDIT_DemandRecords.txt")) :parsemode :noscience 
                                   :schema proc.schemas/drecordschema :keywordize-fields? true)
    (r/filter (fn [r] (:Enabled r)))
    (r/map :SRC)
    (into #{})
  ))
  
(defn ints-from-dmdrecs 
  "Builds interests for each enabled src from demand records"
  [root]
  (->> (enabled-srcs root)
    (srcs->interests)))
    
(defn srcs->interests [srcs]
  (into {} (map (fn [src] [(keyword src) [src [src]]]) srcs)))
         
(defn src-fill-paths [root]
  (map #(str root "fills/" % ".txt") (ints-for-srcs root :srconly? true)))
  
(defn ints-for-branch 
  "Generates subints (a subset of interests) from an ints-for-srcs map given the first two digist of an src as
a string and the root directory for the Marathon run"
  [branchstr root]
  (proc.util/filter-map (fn [[src _]] (= (subs src 0 2) branchstr)) (ints-for-srcs root)))  

(defn strs->ints [strings]
  (apply hash-map
         (reduce concat 
          (for [s strings]
            [(keyword s) [s [s]]]))))

;note that all SRCS listed here must be unique.
(def definterests {:BCT    ["BCT"  ["47112R000"   "77302R500"   "77302R600" "87312R000"]]
                   :DIV    ["DIV"  ["87000R000"   "87000R100"]]
                   :CAB    ["CAB"  ["01302R200"   "01302R100"]] 
                   :GSAB   ["GSAB" ["01226R100"]]
                   :CSSB   ["CSSB" ["63426R000"]]
                   :ATTACK ["ATTACK BN" ["01285R100"]]
                   :ENG    ["Engineers" ["05418R000" "05417R000"]]
                   :SAPPER ["SAPPER" ["05330R100"]]
                   :MP     ["MP" ["19477R000"]]
                   :TRUCK  ["TRUCKS" ["55727R100"
                                      "55727R200"
                                      "55727R300"
                                      "55728R100"
                                      "55728R200"
                                      "55728R300"
                                      "55779R000"]]
                   :CA    ["Civil Affairs" ["41750G000" "41750G100"]]})

;:ABCT :IBCT :SBCT :CABECAB :BCTCAB :TRUCK :ENG :CSSB :DIV :GSAB :SAPPER :MP :CAB :ATTACK :BCT :CA
(def hsubis1922 [:ABCT :IBCT :SBCT :CAB :ECAB :TRUCK :ENG :CSSB :DIV :GSAB :SAPPER :MP :ATTACK :BCT :CA :QM :PATRIOT :AVGBN :CSSB :SUSBDE])
(def hints1922 {;:BCT    ["BCT"  ["47112R000"   "77302R500"   "77302R600" "87312R000"]]
                ;:DIV    ["DIV"  ["87000R000"   "87000R100"]]
                :CAB    ["CAB"  ["01302R100" "01302K000"]] 
                :ECAB   ["ECAB" ["01402K000"]]
                :GSAB   ["GSAB" ["01225K000" "01435K000" "01635K000"]]
                :CSSB   ["CSSB" ["63426R000" "63426K000"]]
                :SUSBDE ["Sus BDE" ["63302R000"]]
                ;:ATTACK ["ATTACK BN" ["01285K000"]]
                ;:ENG    ["Engineers" ["05418R000" "05417R000"]]
                ;:SAPPER ["SAPPER" ["05330R100"]]
                ;:MP     ["MP" ["19477R000"]]
                ;:TRUCK  ["TRUCKS" ["55727R100"
                ;                   "55727R100"
                ;                   "55727R200"
                ;                   "55727R300"
                ;                   "55728R100"
                ;                   "55728R200"
                ;                   "55728R300"
                ;                   "55779R000"]]
                ;:CA     ["Civil Affairs" ["41750G000" "41750G100"]]
                ;:BCTCAB ["BCT and CAB Fills" ["47112R000"   "77302R500"   "77302R600" "87312R000" "01302R100" "01302R200" "01402R000"]]
                :ABCT   ["ABCT" ["87312K000"]]
                :IBCT   ["IBCT" ["77202K000"   "77202K100"]]
                :SBCT   ["SBCT" ["47112K000"]] 
                :QM     ["QM" ["10460R000" "10527RA00"]]
                :PATRIOT ["Patriot" ["44635K000"]]
                :AVGRBN ["Avenger BN" ["44615R600"]]})
                ;:CABECAB["CAB and ECAB" ["01302R100" "01302R200" "01402R000"]]



(def bctscabs {:ABCT  ["ABCT" ["87312R000"]]
               :IBCT  ["IBCT" ["77302R500" "77302R600"]]
               :SBCT  ["SBCT" ["47112R000"]]
               :CAB  ["CAB" ["01302R200" "01302R100"]]
               :ECAB  ["ECAB" ["01402R000"]]})

;compiled by looking at currentish TAA supply. Scope based on not most recent TAA run.
(def cints19-23 {:BCTS ["BCTS"  ["47112K000"   "77202K000"   "77202K100" "87312K000"]] ;all in scope
                 :GSAB ["GSAB" ["01225K000"]] ;in supp not dmd
                 :CAB ["CAB" ["01302K000"]] ;in scope
                 :CSSB ["CSSB" ["63426K000"]] ;in scope
                 :QMSUPP ["QM Supply Co" ["10473R100"]] ;because it's in scope right now. water and qm pol look out of scope
                 :QMWATER ["QM Water Supp Co" ["10460R000"]] ;in supp not dmd
                 :QMPOL    ["QM Pol" ["10527RA00" "10527RC00" "10527RF00" "10527RG00"]] ;in dmd not supply
                 :PATRIOT ["ADA Patriot" ["44635K000"]] ;in scope
                 :AVENGER ["ADA Avenger" ["44615R600"]]}) ;in scope

(def defaults hints1922)
;We could also define more functions to operate on interests such as combining two or more interests into one interest.  
;If we start creating lots of these, it might be less redundant to break up this large interest map into smaller pieces
;and then merge those pieces as we need to.

