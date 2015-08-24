(ns proc.schemas)

(def deprecordschema {"ID"	        :int
                      "DeploymentID"	:int
                      "Location"	:text
                      "Demand"	        :text
                      "DwellBeforeDeploy" :int
                      "BogBudget"	:int
                      "CycleTime"	:int
                      "DeployInterval"	:int
                      "DeployDate"	:text
                      "FillType"	:text
                      "FillCount"	:int
                      "UnitType"	:text
                      "DemandType"	:text
                      "DemandGroup"	:text
                      "Unit"	        :text
                      "Policy"	        :text
                      "AtomicPolicy"	:text
                      "Component"	:text
                      "Period"	        :text
                      "FillPath"	:text
                      "PathLength"	:int
                      "FollowOn"	:boolean
                      "FollowOnCount"	:int
                      "DeploymentCount"	:int
                      "Category"	:text
                      "DwellYearsBeforeDeploy"	:text
                      "OITitle"	        :text})

(def locschema {:T :int 
                :EntityFrom :text
                :EntityTo :text
                :EventName :text
                :Msg :text})

(def drecordschema {"Type" :text
                    "Enabled"  :boolean
                    "Priority"	:int
                    "Quantity"	 :int
                    "DemandIndex" :int	
                    "StartDay"	 :int
                    "Duration"	 :int
                    "Overlap"	 :int
                    "SRC"        :text
                    "SourceFirst" :text	
                    "DemandGroup" :text	
                    "Vignette"	 :text
                    "Operation"	 :text
                    "Category"	 :text
                    "Title 10_32" :text	
                    "OITitle" :text})

(def dschema {:t  	        :int
              :Quarter	        :int
              :SRC	        :text
              :TotalRequired	:int
              :TotalFilled	:int
              :Overlapping	:int
              :Deployed	        :int
              :DemandName	:text
              :Vignette	        :text
              :DemandGroup	:text
              :ACFilled	        :int
              :RCFilled	        :int
              :NGFilled	        :int
              :GhostFilled	:int
              :OtherFilled	:int})

(def cycleschema 
  {"tstart"	      :int
   "tfinal"	      :int
   "Deployments"      :int
   "BOG"	      :int
   "MOB"	      :int
   "Dwell"            :int	
   "Duration"         :int 	
   "BDR"	      :float
   "BDR 1:X"	      :float
   "BOGExpected"      :int
   "DwellExpected"    :int 
   "DurationExpected" :int})

;Tom gave craig this schema on 7/8/15:
 (def fillrecord {:Unit      :text
                  :category :text
                  :DemandGroup :text
                  :SRC :text
                  :FillType :text
                  :FollowOn :boolean
                  :name :text
                  :Component :text
                  :operation :text
                  :start :int
                  :DeploymentID :int
                  :duration :int
                  :dwell-plot? :boolean
                  :DwellYearsBeforeDeploy :float
                  :DeployDate :text
                  :FollowOnCount :int
                  :AtomicPolicy :text
                  :Category :text
                  :DeployInterval :int
                  :fill-type :text
                  :FillPath :text
                  :Period :text
                  :unitid :int
                  :deltat :int
                  :Demand :text
                  :PathLength :int
                  :OITitle :text
                  :BogBudget :int
                  :CycleTime :int
                  :DeploymentCount :int
                  :DemandType :text
                  :quantity :int
                  :end :int
                  :FillCount :int
                  :Location :text
                  :location :text
                  :compo :text
                  :DwellBeforeDeploy :int
                  :Policy :text
                  :sampled :boolean
                  })
