(ns Rutherford.rest-db-starcraft
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.repl :refer :all]
            [clojure.data :refer :all]
            [clojure.pprint :as pp]
            [datomic.api :as d]
            [Rutherford.datomic :refer [q-idents q-attrs]]))


(comment

  (def db-uri "datomic:free://datomicfreedb:4334/aligulac")

  (def conn (d/connect db-uri))

  (def db (d/db conn))

  (dir d)

  ;
  )


(comment

  (q-idents db)

  ;
  )