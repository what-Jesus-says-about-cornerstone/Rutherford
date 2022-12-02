(defproject program "0.1.0"

  :repositories [["conjars" {:url "https://conjars.org/repo"}]
                 ["clojars" {:url "https://clojars.org/repo"}]
                 ["oracle" {:url "https://download.oracle.com/maven"}]]

  :min-lein-version "2.0.0"

  :plugins [[cider/cider-nrepl "0.21.1"]]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "0.4.490"]
                 [nrepl "0.6.0"]
                 [cider/cider-nrepl "0.21.1"]
                 
                 [cheshire "5.8.1"]
                 [org.clojure/data.csv "0.1.4"]

                 [io.dgraph/dgraph4j "1.7.3"]

                 [io.pedestal/pedestal.service       "0.5.7"]
                 [io.pedestal/pedestal.service-tools "0.5.7"]

                 [io.pedestal/pedestal.jetty         "0.5.7"]
                 [org.clojure/tools.namespace "0.2.11"]

                 [org.clojure/java.jdbc "0.7.9"]
                 [org.postgresql/postgresql "42.2.6"]
                 [clj-time "0.15.0"]
                 [mvxcvi/puget "1.3.4"]
                 [clj-http "3.12.3"]
                 [com.datomic/datomic-free "0.9.5697"]

                 ;
                 ]

  :repl-options {:init-ns          Rutherford.main
                 :main             Rutherford.main
                 :host             "0.0.0.0"
                 :port             4001}
  :profiles {:prod ^:leaky {:main Rutherford.main
                            :aot  [Rutherford.main]}}


  :main ^{:skip-aot false} Rutherford.main
  :jvm-opts ["-Xms768m" "-Xmx2048m" "-Xmx1g"]

  :source-paths ["src"]
  :java-source-paths ["src"]
  
  :auto-clean false)
