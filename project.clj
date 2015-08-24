(defproject proc "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [spork "0.1.9.2-SNAPSHOT"]
                 [incanter "1.5.6"]
                 [iota "1.1.2"]]
  :repl-options {:init (do (println "Loading post processor")
                           (require 'proc.example)
                           (println "Switching to post processor namespace")
                           (ns proc.example))
                 :init-ns proc.example})
