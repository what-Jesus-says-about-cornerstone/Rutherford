(ns Rutherford.rest-db-datomicui
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.repl :refer :all]
            [clojure.data :refer :all]
            [clojure.pprint :as pp]
            [datomic.api :as d]
            [Rutherford.datomic :refer [q-idents q-attrs]]))

(def base-uri "datomic:free://datomicfreedb:4334/")

(def db-uri (str base-uri "mbrainz"))

(declare conn)
(declare db)



(defn connect-lazy
  "Connect if conn does not exist"
  []
  (defonce conn (d/connect db-uri))
  (defonce db (d/db conn)))


(comment


  (defonce conn (d/connect db-uri))

  (defonce db (d/db conn))

  (dir d)

  ;
  )


(defn q-paginted-entity
  "Returns entities and total count given limit,offset and attribute keword"
  [{:keys [db attribute limit offset]
    :or   {limit  10
           db  Rutherford.rest-db-datomicui/db
           offset 0}}]
  {:entities (->>
              (d/q '{:find  [?e (count ?e)]
                     :in    [$ ?attribute]
                     :where [[?e ?attribute]]}
                   db attribute)
              (drop offset)
              (take limit)
              (map  #(identity (d/pull db '[*] (first %))))
              (into []))
   :count    (d/q '{:find  [(count ?e) .]
                    :in    [$ ?attribute]
                    :where [[?e ?attribute]]}
                  db attribute)})


(defn q-text-search
  [{:keys [db search]
    :or {db Rutherford.rest-db-datomicui/db}}]
  (->
   (d/q '[:find ?entity ?name ?tx ?score
          :in $  ?search
          :where [(fulltext $ :artist/name ?search) [[?entity ?name ?tx ?score]]]]
        db
        search)
   vec))

(defn q-database-names
  []
  (d/get-database-names (str base-uri "*")))

(comment

  (q-attrs db)

  (q-idents db)

  (->
   (q-paginted-entity {; :db        db
                       :attribute :artist/name
                       :limit     2
                       :offset    0})
   pp/pprint)

  (->
   (d/q '[:find ?entity ?name ?tx ?score
          :in $ ?search
          :where [(fulltext $ :artist/name ?search) [[?entity ?name ?tx ?score]]]]
        db
        "Jane")
   vec
   )


  (d/get-database-names "datomic:free://datomicfreedb:4334/*")



  ;
  )