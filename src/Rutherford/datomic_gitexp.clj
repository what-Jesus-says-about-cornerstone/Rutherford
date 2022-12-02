(ns Rutherford.datomic-gitexp
  (:require [datomic.api :as d]
            [clojure.repl :refer :all]
            [clojure.pprint :as pp]))


(comment

  (def db-uri "datomic:free://datomicfreedb:4334/gitexp")

  (d/create-database db-uri)

  (def conn (d/connect db-uri))

  (def db (d/db conn))

  ;
  )
