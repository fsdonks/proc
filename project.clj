(defproject proc "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [spork "0.2.0.2-SNAPSHOT"]
                 [incanter "1.5.6"]
                 [iota "1.1.2"]]
  :repl-options {:init (do (println "Loading post processor")
                           (require 'proc.example)
                           (println "Switching to post processor namespace")
                           (ns proc.example))
                 :init-ns proc.example}
  ;:jvm-opts ^:replace ["-Xmx1g" "-XX:NewSize=197000"] ;200000 loads and runs run-sample! but 195000 won't load
  :jvm-opts ^:replace ["-Xmx2g"] ;1G did run-sample! okay. 500 gave a heap error.  700 says 
  ;OutOfMemoryError GC overhead limit exceeded  clojure.lang.PersistentHashMap$TransientHashMap.doPersistent (PersistentHashMap.java:301)
  ;900 succeeds with run-sample.  What about 800? 800 runs okay.  750? even 750 works.
 )
