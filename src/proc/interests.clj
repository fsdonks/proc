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

(defn srcs-by-branch [root & {:keys [demand] :or {demand true}}]
  (let [filetype (if demand "AUDIT_DemandRecords.txt" "AUDIT_SupplyRecords.txt")
        schema (if demand proc.schemas/drecordschema proc.schemas/supply-recs)]
    (->> (spork.util.table/tabdelimited->records (slurp (str root filetype)) :parsemode :noscience 
                                                 :schema schema :keywordize-fields? true)
      (r/filter (fn [{:keys [Enabled]}] (= (clojure.string/upper-case Enabled) "TRUE")))
      (reduce (fn [acc {:keys [SRC]}]
                (let [branch (subs SRC 0 2)]
                  (if-let [srcs (get acc branch)] (assoc acc branch (conj srcs SRC)) (assoc acc branch #{SRC})))) {}))))

(defn branch-srcs->interests [root & {:keys [demand] :or {demand true}}]
  (->> (srcs-by-branch root :demand demand)
    (reduce-kv (fn [acc k v] (assoc acc (keyword k) [k (vec v)])) {})))
  

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

(def taa2024deny 

  {:14 ["14" ["14537R000" "14423R000" "14527RB00" "14537K000"]],
 :01
 ["01"
  ["01623K000"
   "01825G000"
   "01285K100"
   "01612K100"
   "01402K000"
   "01808G000"
   "01225K000"
   "01707R000"
   "01418K000"
   "01605K100"
   "01285K000"
   "01980G000"
   "01447K000"
   "01302K000"
   "01622K000"
   "01435K000"
   "01205K000"
   "01225K100"
   "01655E000"]],
 :37 ["37" ["37342R200"]],
 :07
 ["07"
  ["07215K000"
   "07815R000"
   "07315K100"
   "07195K000"
   "07315K000"
   "07215K100"]],
 :27
 ["27"
  ["27573RC00"
   "27523RB00"
   "27563RE00"
   "27583RC00"
   "27543RA00"
   "27523RC00"
   "27563RB00"
   "27773K000"
   "27473K000"
   "27583RB00"
   "27523RA00"
   "27563RD00"
   "27563RC00"
   "27583RA00"
   "27573RB00"
   "27563RA00"]],
 :42
 ["42"
  ["42524RB00"
   "42529RE00"
   "42529RA00"
   "42524RD00"
   "42524RC00"
   "42524RA00"]],
 :45 ["45" ["45413L000" "45423L000" "45500GB00" "45607L000"]],
 :03
 ["03"
  ["03579RA00"
   "03420R300"
   "03520R000"
   "03492K000"
   "03323K000"
   "03470R000"
   "03396K000"
   "03310R000"]],
 :77 ["77" ["77202K000" "77202K100"]],
 :31 ["31" ["31815RA00" "31812R100" "31825R100"]],
 :40 ["40" ["40510RC00" "40500RA00"]],
 :52 ["52" ["52410K300"]],
 :05
 ["05"
  ["05473R000"
   "05800R000"
   "05601RH00"
   "05520RD00"
   "05617R000"
   "05437RC00"
   "05510RA00"
   "05438R000"
   "05567RA00"
   "05315K500"
   "05315K800"
   "05402K000"
   "05530RA00"
   "05603KT00"
   "05428R000"
   "05520RE00"
   "05315K600"
   "05520RC00"
   "05520RA00"
   "05520RB00"
   "05437R200"
   "05567RB00"
   "05530RI00"
   "05315K700"
   "05435K000"
   "05640R000"
   "05530RH00"
   "05439R000"
   "05510RB00"
   "05427R000"
   "05543RH10"
   "05429R000"]],
 :12
 ["12"
  ["12413R000"
   "12567RA00"
   "12567RE00"
   "12682R000"
   "12567RB00"
   "12567RL00"
   "12567RK00"]],
 :11
 ["11"
  ["11632R000"
   "11623R000"
   "11975K000"
   "11307R600"
   "11307R500"
   "11693R000"]],
 :10
 ["10"
  ["10560RN00"
   "10527RF00"
   "10567RA00"
   "10414R000"
   "10473R100"
   "10527RG00"
   "10560RM00"
   "10527RA00"
   "10560RA00"
   "10548RB00"
   "10567RC00"
   "10436R000"
   "10417R000"
   "10649R000"
   "10527RC00"
   "10743R000"
   "10538RA00"
   "10538RB00"
   "10538RF00"
   "10567RG00"
   "10548RA00"
   "10560RB00"]],
 :06
 ["06"
  ["06602R000"
   "06385K000"
   "06465K000"
   "06325K000"
   "06604A000"
   "06235K300"
   "06455R000"
   "06425R000"
   "06433K000"
   "06603A000"
   "06475K000"
   "06333K000"
   "06235K100"]],
 :30
 ["30"
  ["30715R200"
   "30815R300"
   "30620R100"
   "30725R100"
   "30702R000"
   "30705R200"
   "30875R100"]],
 :53 ["53" ["53615R300"]],
 :43 ["43" ["43429R000" "43547RA00"]],
 :16 ["16" ["16500FC00" "16500LB00" "16500FD00" "16500LA00"]],
 :44
 ["44" ["44601R600" "44655K000" "44602K000" "44693R000" "44635K000"]],
 :87 ["87" ["87312K000" "87000K100"]],
 :55
 ["55"
  ["55727R300"
   "55522KA00"
   "55740K000"
   "55588RA00"
   "55848K000"
   "55838K000"
   "55789R000"
   "55816K000"
   "55779R000"
   "55819R000"
   "55560RE00"
   "55812K000"
   "55829K000"
   "55728R300"
   "55506RA00"
   "55727R100"
   "55739R000"
   "55728R200"
   "55587RA00"
   "55716R000"
   "55719R000"
   "55522KB00"
   "55560RD00"
   "55603KB00"
   "55530RJ00"
   "55606R000"]],
 :47 ["47" ["47112K000"]],
 :20 ["20" ["20518RC00" "20518RA00" "20518RB00"]],
 :17 ["17" ["17215K100" "17215K000" "17195K000" "17315K000"]],
 :19
 ["19"
  ["19717R000"
   "19882K000"
   "19601RA00"
   "19601R000"
   "19886K000"
   "19646K000"
   "19476K000"
   "19543RE00"
   "19653R000"
   "19539RC00"
   "19402K000"
   "19473K000"
   "19539RA00"
   "19539RB00"
   "19888R000"]],
 :41 ["41" ["41740R100" "41730G000" "41737R100" "41750R100"]],
 :90 ["90" ["90472K000" "90873R000" "90588RA00" "90376K000"]],
 :33
 ["33"
  ["33736G100"
   "33725G200"
   "33737G200"
   "33757G100"
   "33737G100"
   "33712G100"]],
 :51 ["51" ["51100R100" "51632R000" "51659R000"]],
 :63
 ["63"
  ["63426K000"
   "63347R300"
   "63035K000"
   "63347R000"
   "63045K000"
   "63347R200"
   "63475K000"
   "63375K000"
   "63302K000"
   "63702K000"
   "63346R200"
   "63025K000"
   "63702K100"
   "63355R000"
   "63475K100"
   "63055K000"
   "63347R100"]],
 :02
 ["02" ["02523KC00" "02523KB00" "02543KA00" "02553KA00" "02523KD00"]],
 :34 ["34" ["34645R300" "34402K000" "34425K000"]],
 :09
 ["09"
  ["09436K000"
   "09513KB00"
   "09632K000"
   "09513KA00"
   "09537RB00"
   "09537RA00"]],
 :08
 ["08"
  ["08317K000"
   "08979R000"
   "08453R000"
   "08976R000"
   "08988R000"
   "08668R000"
   "08528RA00"
   "08457R000"
   "08429R000"
   "08977R000"
   "08949R000"
   "08670R000"
   "08978R000"
   "08488K000"
   "08473R000"
   "08527AA00"
   "08480K000"
   "08300R000"
   "08460R000"
   "08430R000"
   "08485R000"
   "08528RB00"
   "08567RA00"
   "08420R000"
   "08640R000"]]})

(def taa2024defeat {:14 ["14" ["14537R000" "14423R000" "14527RB00" "14537K000"]],
 :01
 ["01"
  ["01623K000"
   "01825G000"
   "01285K100"
   "01612K100"
   "01402K000"
   "01808G000"
   "01225K000"
   "01707R000"
   "01418K000"
   "01605K100"
   "01285K000"
   "01980G000"
   "01447K000"
   "01302K000"
   "01655E000"
   "01622K000"
   "01435K000"
   "01205K000"
   "01225K100"]],
 :37 ["37" ["37342R200"]],
 :07
 ["07"
  ["07215K000"
   "07815R000"
   "07315K100"
   "07195K000"
   "07315K000"
   "07215K100"]],
 :27
 ["27"
  ["27573RC00"
   "27523RB00"
   "27563RE00"
   "27583RC00"
   "27543RA00"
   "27523RC00"
   "27563RB00"
   "27773K000"
   "27473K000"
   "27583RB00"
   "27523RA00"
   "27563RD00"
   "27563RC00"
   "27583RA00"
   "27573RB00"
   "27563RA00"]],
 :42
 ["42"
  ["42524RB00"
   "42529RE00"
   "42529RA00"
   "42524RD00"
   "42524RC00"
   "42524RA00"]],
 :45 ["45" ["45413L000" "45423L000" "45500GB00" "45607L000"]],
 :03
 ["03"
  ["03579RA00"
   "03420R300"
   "03520R000"
   "03492K000"
   "03323K000"
   "03470R000"
   "03396K000"
   "03310R000"]],
 :77 ["77" ["77202K000" "77202K100"]],
 :31 ["31" ["31815RA00" "31812R100" "31825R100"]],
 :40 ["40" ["40510RC00" "40500RA00"]],
 :52 ["52" ["52410K300"]],
 :05
 ["05"
  ["05473R000"
   "05800R000"
   "05601RH00"
   "05520RD00"
   "05617R000"
   "05437RC00"
   "05510RA00"
   "05438R000"
   "05567RA00"
   "05315K500"
   "05315K800"
   "05402K000"
   "05530RA00"
   "05603KT00"
   "05428R000"
   "05520RE00"
   "05315K600"
   "05520RC00"
   "05520RA00"
   "05520RB00"
   "05437R200"
   "05567RB00"
   "05530RI00"
   "05315K700"
   "05435K000"
   "05640R000"
   "05530RH00"
   "05439R000"
   "05510RB00"
   "05427R000"
   "05543RH10"
   "05429R000"]],
 :12
 ["12"
  ["12413R000"
   "12567RA00"
   "12567RE00"
   "12682R000"
   "12567RB00"
   "12567RL00"
   "12567RK00"]],
 :11
 ["11"
  ["11632R000"
   "11623R000"
   "11975K000"
   "11307R600"
   "11307R500"
   "11693R000"]],
 :10
 ["10"
  ["10560RN00"
   "10527RF00"
   "10567RA00"
   "10414R000"
   "10473R100"
   "10527RG00"
   "10560RM00"
   "10527RA00"
   "10560RA00"
   "10548RB00"
   "10567RC00"
   "10436R000"
   "10417R000"
   "10649R000"
   "10527RC00"
   "10743R000"
   "10538RA00"
   "10538RB00"
   "10538RF00"
   "10567RG00"
   "10548RA00"
   "10560RB00"]],
 :06
 ["06"
  ["06602R000"
   "06385K000"
   "06465K000"
   "06325K000"
   "06604A000"
   "06235K300"
   "06455R000"
   "06425R000"
   "06433K000"
   "06603A000"
   "06475K000"
   "06333K000"
   "06235K100"]],
 :30
 ["30"
  ["30715R200"
   "30815R300"
   "30620R100"
   "30725R100"
   "30702R000"
   "30705R200"
   "30875R100"]],
 :53 ["53" ["53615R300"]],
 :43 ["43" ["43429R000" "43547RA00"]],
 :16 ["16" ["16500FC00" "16500LB00" "16500FD00" "16500LA00"]],
 :44
 ["44" ["44601R600" "44655K000" "44602K000" "44693R000" "44635K000"]],
 :87 ["87" ["87312K000" "87000K100"]],
 :55
 ["55"
  ["55727R300"
   "55522KA00"
   "55740K000"
   "55588RA00"
   "55848K000"
   "55838K000"
   "55789R000"
   "55816K000"
   "55779R000"
   "55819R000"
   "55560RE00"
   "55812K000"
   "55829K000"
   "55728R300"
   "55506RA00"
   "55727R100"
   "55739R000"
   "55728R200"
   "55587RA00"
   "55716R000"
   "55719R000"
   "55522KB00"
   "55560RD00"
   "55603KB00"
   "55530RJ00"
   "55606R000"]],
 :47 ["47" ["47112K000"]],
 :20 ["20" ["20518RC00" "20518RA00" "20518RB00"]],
 :17 ["17" ["17215K100" "17215K000" "17195K000" "17315K000"]],
 :19
 ["19"
  ["19717R000"
   "19882K000"
   "19601RA00"
   "19601R000"
   "19886K000"
   "19646K000"
   "19476K000"
   "19543RE00"
   "19653R000"
   "19539RC00"
   "19402K000"
   "19473K000"
   "19539RA00"
   "19539RB00"
   "19888R000"]],
 :41 ["41" ["41740R100" "41730G000" "41737R100" "41750R100"]],
 :90 ["90" ["90472K000" "90873R000" "90588RA00" "90376K000"]],
 :33
 ["33"
  ["33736G100"
   "33725G200"
   "33737G200"
   "33757G100"
   "33737G100"
   "33712G100"]],
 :51 ["51" ["51100R100" "51632R000" "51659R000"]],
 :63
 ["63"
  ["63426K000"
   "63347R300"
   "63035K000"
   "63347R000"
   "63045K000"
   "63347R200"
   "63475K000"
   "63375K000"
   "63302K000"
   "63702K000"
   "63346R200"
   "63025K000"
   "63702K100"
   "63355R000"
   "63475K100"
   "63055K000"
   "63347R100"]],
 :02
 ["02" ["02523KC00" "02523KB00" "02543KA00" "02553KA00" "02523KD00"]],
 :34 ["34" ["34645R300" "34402K000" "34425K000"]],
 :09
 ["09"
  ["09436K000"
   "09513KB00"
   "09632K000"
   "09513KA00"
   "09537RB00"
   "09537RA00"]],
 :08
 ["08"
  ["08317K000"
   "08979R000"
   "08453R000"
   "08976R000"
   "08988R000"
   "08668R000"
   "08528RA00"
   "08457R000"
   "08429R000"
   "08977R000"
   "08949R000"
   "08670R000"
   "08978R000"
   "08488K000"
   "08473R000"
   "08527AA00"
   "08480K000"
   "08300R000"
   "08460R000"
   "08430R000"
   "08485R000"
   "08528RB00"
   "08567RA00"
   "08420R000"
   "08640R000"]]})

