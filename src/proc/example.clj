;;This is a sample of stuff we can do using the post processor.
(ns proc.example
  (:require [proc.stacked :as stacked]
            [proc.util :as util]
            [proc.interests :as ints])
  (:use [proc.core]
        [incanter.charts]
        [incanter.core]
        ;[proc.supply]
        )) 

;;This dumps out our fills and sandtrends for the interesting srcs.
(defn run-sample!   "Call with eachsrc true if you want the fills for each SRC individually, or if you want to group the fills by interest,
call with ints set to a symbol from proc.interests.
call with :byDemandType? true if you want fills files by demandtype instead of the default by SRC. NotUtilized and Unmet records 
will always be found in a fills file associated with the respective SRC or interest.  If you don't see a fills file for an SRC, is it
in your interests?"
  [path & {:keys [interests subints eachsrc byDemandType?] :or {interests ints/defaults byDemandType? true}}]
  (let [subs (if subints subints (keys interests))]
    (binding [proc.core/*byDemandType?* byDemandType?
              proc.core/*run-path* path] 
      (if eachsrc
        (binding [*last-split-key* (if byDemandType? :DemandType :SRC)] (sandtrends-from path))
        (only-by-interest subs interests (sandtrends-from path))))))

;Can do trac-like charts by demand group by doing (binding [proc.stacked/*by-demandgroup?* true] 
;                                                        (do-charts-from root :interests blah))

;Do-charts-from creates stacked dwell and fill charts for each interest using AUDIT_deployments.txt and
;the fills files produced from proc.fillsfils/run-sample!.
;You need to call run-sample! in order to make a folder for fills files within the Marathon run folder before calling do-charts-from
(defn do-charts-from ;do we have too many arguments here?
  "Pass in your own interests if you'd like.  See examples of interests in proc.interests
:group-key defaults to :DemandType for dwell-before-deployment plot.  Can set :group-key to :UnitType as well.
Call with :sync false in order to not sync the x and y axis across these charts.  :sync defaults to true.
Call with :phases set to a sequence of phases defined in the run in order to see separate charts for each phase.  Will throw
a null pointer exception if one of your phases doesn't exist in the run.
Call with :fillbnds {:fxlow val0 :fxhigh val1 :fylow val2 :fyhigh val3} and/or 
:dwellbnds {:dxlow val4 :dxhigh val5 :dylow val6 :dyhigh val7} to set the bounds for axes."
  [root & {:keys [subs phases interests subints group-key syncys phase-lines fillbnds dwellbnds] :or {subs false phases [nil] syncys false
                                                                interests ints/defaults group-key :DemandType
                                                                phase-lines true
                                                                fillbnds {:fxlow 0 :fylow 0}
                                                                dwellbnds {:dxlow 0}}}]
  (let [ints (if subints subints (keys interests))
        charts (only-by-interest  ints interests
                       (remove nil? (for [int ints ;remove nil printlns
                                                              phase phases]
                                                          (binding [dep-group-key group-key] 
                                                            (dwell-over-fill root (get interests int) subs phase)))))
        dwells (->> (map (fn [[pane dwell fill]] dwell) charts)
                 (remove (fn [chart] (nil? (.getDataset (.getPlot chart)))))) ;had to remove no deployment data charts
        fills (map (fn [[pane dwell fill]] fill) charts)
        {:keys [fxlow fxhigh fylow fyhigh]} fillbnds
        {:keys [dxlow dxhigh dylow dyhigh]} dwellbnds
        _ (doseq [chart dwells] (do (proc.util/set-bounds chart :x-axis :lower dxlow :upper dxhigh)
                                  (proc.util/set-bounds chart :y-axis :lower dylow :upper dyhigh)))
        _ (doseq [chart fills] (do (proc.util/set-bounds chart :x-axis :lower fxlow :upper fxhigh)
                                  (proc.util/set-bounds chart :y-axis :lower fylow :upper fyhigh)))
        _ (proc.util/sync-scales (concat dwells fills) :axis :x-axis)   ;added for taa. want all charts same x
        _ (when syncys (do  ;it might be useful if our y-axes all match, too
                       (proc.util/sync-scales dwells :axis :y-axis)
                       (proc.util/sync-scales fills :axis :y-axis)))
        _  (doseq [dwell dwells] (add-trend-lines! dwell :bnds (:x (xy-bounds dwell)))) ;need to add trend lines after syncing charts
        phstarts (phase-starts root)
        _ (when phase-lines (doseq [fill fills] (add-phase-lines phstarts 
                                                                 (.getLowerBound (:y-axis (util/state fill))) 
                                                                 (.getUpperBound (:y-axis (util/state fill))) 
                                                               fill)))]
    (doseq [chart charts] (show-stack chart))))
       
;;One function to make the files for the charts and display the charts at the same time
(defn charts-from-unproc-run
  ([path]
    (do (run-sample! path)
        (do-charts-from path))))

(defn view-deployments 
  ([int interests deploymentspath]
    (let [dwell-plot (deployment-plot deploymentspath (get interests int) nil)
          _ (add-trend-lines! dwell-plot :bnds (:x (xy-bounds dwell-plot)))]
      (view dwell-plot)))
  ([deploymentspath] (view-deployments :BCTS ints/cints19-23 deploymentspath)))    

; pipeline copied from sand-chart2
(defn unit-sand-from [path interest] ;oops.  Need to do daily samples here
  (let [ds (util/as-dataset (str path "sand/" interest ".txt"))]
    (-> (->> (-> (util/as-dataset ds)
           (stacked/roll-sand :cat-function stacked/parse-arfor-fill)
           (stacked/expand-samples)) ; this smooths out the picture but what is it doing?
      (stacked/xy-table :start :quantity :group-by :Category :data))
      (stacked/stacked-areaxy-chart2* :legend true :color-by stacked/arforgen-color-2 :order-by stacked/arforgen-order-2)
      (view))))

(defn lines-demo 
  "We can make lines instead of stacked area charts, but need to uncompress x labels, and use fewer groups..."
  [fpath]
  (let [dset (stacked/roll-sand (util/as-dataset fpath) 
                                   :cols [:FillType :DemandType] :cat-function stacked/suff-cat-fill-subs )]
    (view (line-chart :start :quantity :group-by :Category :legend true :data dset))))


;given a parent directory, return us all paths of the run folders.... identify
;a run folder by some marathon file
(defn run-names-from [rootsloc]) 

(comment ;testing
(def tp 
  "C:/Users/tspoon/Documents/srm/tst/notionalv2/maxbase")
  )
  

 






