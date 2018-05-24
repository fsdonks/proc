
(defproject proc "0.2.6-SNAPSHOT"
  :description "Embeddable post-processor."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [spork "0.2.0.9-SNAPSHOT"]
                 [joinr/incanter "1.9.3-SNAPSHOT"]
                 ;;Note: the current release of incanter/incanter 1.9.1
                 ;;has a jacked dependency via swingrepl to an ancient clojure.
                 ;;This breaks the build process.  Fortunately, we don't need it.
                 ;;We just specify the individual modules and we're good to go.
                                        ;                 [incanter "1.5.8"]
                 [iota "1.1.2"]
                 ]
  :repl-options {:init (do (println "Loading post processor")
                           (require 'proc.example)
                           (println "Switching to post processor namespace")
                           (ns proc.example))
                 :init-ns proc.example}
  :source-paths ["src" "../spork/src" "../incanter/modules/incanter-charts/src"
                 ]
  :profiles {:order {:aot [proc.example]}}
  ;;jvm-opts ^:replace ["-Xmx1g" "-XX:NewSize=197000"] ;200000 loads and runs run-sample! but 195000 won't load
  ;;jvm-opts ^:replace ["-Xmx2g"] ;1G did run-sample! okay. 500 gave a heap error.  700 says 
  ;;OutOfMemoryError GC overhead limit exceeded  clojure.lang.PersistentHashMap$TransientHashMap.doPersistent (PersistentHashMap.java:301)
  ;;900 succeeds with run-sample.  What about 800? 800 runs okay.  750? even 750 works.
  )
