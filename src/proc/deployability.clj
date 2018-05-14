
;Some functions to prepare an input file for DE QWARRM work.
;The stuff before ;final spits out each units deployable status for every day
;This was unmanageable for Excel and/or Access, so after ;final, we only create a record of a units status
;each time their status changes.  
(ns proc.deployability
  (:require [spork.util.table :as tbl]
            [clojure.core.reducers :as r]
            [clojure.string :as str]
            [proc.util :as util])
    (:use [proc.core]))



(defn deployable-status [{:keys [EventType EntityTo Time]}]
  (let [src (second (str/split EntityTo #"_"))
        newr {:Time Time :Unit EntityTo :SRC src}]
    (case EventType
      "Deploying Unit" (assoc newr :Status (last (str/split EventType #" "))) ;"Deployed"
      "Not Deployable" (assoc newr :Status "NotDeployable")
      "New Deployable Stock" (assoc newr :Status "Deployable"))))
    
    
(defn deployable-events [root] 
  ;(binding [tbl/*split-by* #","]
      (util/with-rdrs [rdr (str root "EventLog.csv")]
                (->> (tbl/lines->records (line-seq rdr) {"Time" :int "EventType" :text "EntityTo" :text};schemas/eventlog 
                                    :keywordize-fields false 
                                    :parsemode :noscience
                                    )
                  (r/filter (fn [{:keys [EventType]}] (contains? #{"Deploying Unit" 
                                                                   "Not Deployable" 
                                                                   "New Deployable Stock"} EventType))) 
                  (into [])
                  (group-by (juxt :EntityTo :Time))
                  (map (fn [[[unit t] recs]] (deployable-status (last recs)))))))
;)

(defn spit-deployable-status [root & {:keys [ubound] :or {ubound (util/last-day root)}}]
  (let [elogrecs (deployable-events root)
        sampler (sample-trends elogrecs #(get % :Unit) #(get % :Time))
        ts (range 1 ubound)]
    (->> (mapcat (fn [t] (->> (samples-at sampler t)
                           (map second)
                           (map (fn [r] (assoc r :Time t))))) ts)
      ((fn [recs] (tbl/records->file recs (str root "deployable_status.txt"))))
  )))

;__________ maybe multiple files for each of the 3 statuses is more manageable?
(defn records->multi-files [recs root split-fn] 
  (let [outname "files.ms.edn"
        headers (keys (first recs))
        ms (util/mstream root outname (str (clojure.string/join \tab headers)))]
    (with-open [stream ms]
      (doseq [r recs]
        (let [w (util/get-writer! ms (split-fn r))]
          (do (doseq [h headers]
                (util/write! w (str (get r h) \tab)))
            (util/new-line! w)))))))
  
(defn spit-deployable-status-3files [root]
  (let [elogrecs (deployable-events root)
        sampler (sample-trends elogrecs #(get % :Unit) #(get % :Time))
        lastday (util/last-day root)
        ts (range 1 lastday)
        deproot (str root "unit_status/")
        split-fn (fn [r key]) ]
    (->> (mapcat (fn [t] (->> (samples-at sampler t)
                           (map second)
                           (map (fn [r] (assoc r :Time t))))) ts)
      ((fn [recs] (records->multi-files recs (str root "unit_status") :Status)))
  )))


;;___________________________________________
;;final
;;todo: Deploying Units demand is vignette and not demandgroup, so
;;S3 is shown instead of S3.1
(defn deploy
  "Assoc the record with a :Status key and vignette value. msg is the event log record msg field."
  [r msg]
  (assoc r :Status (-> (str/split msg #"demand ")
                                             (last)
                                             (str/split #"_")
                                             (second)))) ;"Deployed")

(defn not-deployable [r]
  (assoc r :Status "NotDeployable"))

(defn new-deployable [r]
  (assoc r :Status "Deployable"))

(defn get-status [{:keys [EventType EntityTo Time Message]}]
  (let [[unit src compo] (str/split EntityTo #"_")
        newr {:Start Time :Unit EntityTo :SRC src :compo compo :Identifier unit}]
    (case EventType
      "Deploying Unit" (deploy newr Message)
      ":deploy" (deploy newr Message)
      "Not Deployable" (not-deployable newr)
      ":NotDeployable" (not-deployable newr)
      "New Deployable Stock" (new-deployable newr)
      ":NewDeployable" (new-deployable newr)
      "Supply Update" (not-deployable newr)
      ":supplyUpdate" (not-deployable newr)))) ;unit reset its cycle-time to 0 and is probably not deployable

(defn events-with-deployments [root version] 
  ;;(binding [tbl/*split-by* #","]
  ;;everything is tabbed now, note we can override via :delimiter option in lines->records
  (let [{:keys [deploying notdeployable newdeployable update name]} version
        logname (if (= name "m3") "EventLog.csv" "eventlog.txt")
        schema (if (= name "m3") {"Time" :int "EventType" :text "EntityTo" :text "Message" :text}
                   {"t" :int "type" :text "from" :text "to" :text "msg" :text})]
    (util/with-rdrs [rdr (str root logname)]
      (->> (tbl/lines->records (line-seq rdr) schema ;schemas/eventlog 
                               :keywordize-fields? true
                               :parsemode :noscience
                               :delimiter (if (= name "m3") #"," #"\t")
                               )
           (into [])
           (map (fn [r] (if (= name "m3") r (clojure.set/rename-keys r {:t :Time :type :EventType :to :EntityTo :msg :Message}))))
        (r/filter (fn [{:keys [EventType Message]}] (or (contains? #{deploying
                                                         notdeployable 
                                                         newdeployable} EventType)
                                                        ;When units are resetting, we don't always get a deployable message, so if a unit resets
                                                        ;and we have no other deployable events that day, we'll assume that unit to be
                                                        ;not deployable
                                                        (and (= EventType update) (= (last (str/split Message #" ")) "0")))))
        
        (into [])
        (group-by (juxt :EntityTo :Time))
        (map (fn [[[unit t] recs]] 
               ;if a unit goes through multiple states at time t, consider them deployed if one of the events is "Deploying Unit"
               (let [deployment (first (filter (fn [{:keys [EventType]}] (= EventType deploying )) recs))
                     deployable-update (last (filter (fn [{:keys [EventType]}] (or (= EventType notdeployable )
                                                                                     (= EventType newdeployable))) recs))]
                 (get-status (if (nil? deployment) 
                               (if (nil? deployable-update)
                                 (last recs) ;reset to 0 and not deployable
                                 deployable-update)
                               deployment)))))))))
;)  
                                  
;need to account for no other state but the first one and also
; we are popping chunks when we shouldn't be on this test case
;verify
(defn make-event-blocks [ubound [unit time-map]]
  (let [{:keys [chunks curr]} (reduce (fn [acc [t r]] 
                                  (if (= (:Status r) (:Status (:curr acc)))
                                     acc
                                     (assoc acc :curr r :chunks (conj (:chunks acc) (assoc (:curr acc) :End (- (:Start r) 1))))))                                 
          {:curr (second (first time-map)) :chunks []} (rest time-map))]
    ;end at the end of the simulation
        (conj chunks (assoc curr :End ubound))))

;;use as argument and not as dynamic var.
(def m3 {:deploying "Deploying Unit"
         :notdeployable "Not Deployable"
         :newdeployable "New Deployable Stock"
         :update "Supply Update"
         :name "m3"})

(def m4 {:deploying ":deploy"
         :notdeployable ":NotDeployable"
         :newdeployable ":NewDeployable"
         :update ":supplyUpdate"
         :name "m4"})

;;tom: patched to use the function interface defined in tbl/records->file
(defn deployable-status-chunks  [root & {:keys [ubound version] :or {ubound (util/last-day root) version m4}}]
  (let [elogrecs (events-with-deployments root version)
        sampler  (sample-trends elogrecs #(get % :Unit) #(get % :Start))
        recs     (mapcat (partial make-event-blocks ubound) sampler)]
    (tbl/records->file recs (str root "deployable_status_v6.txt")
         :field-order [:Unit :Identifier :SRC :compo :Status :Start :End])))                    
