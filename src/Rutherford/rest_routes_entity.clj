(ns Rutherford.rest-routes-entity
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]
            [datomic.api :as d]
            [clojure.edn :as edn]
            [Rutherford.rest-db-mbrainz :as mbrainz]
            [Rutherford.rest-db-starcraft]
            [Rutherford.rest-db-seattle]
            [Rutherford.datomic :refer [q-idents q-attrs]]
            [Rutherford.impl :refer [try-parse-int]]
            [ring.util.response :as ring-resp]
            [cheshire.core :as json]))


(defn hi
  []
  "hi!")

(defn mbrainz-get-attributes
  [request]
  (mbrainz/connect-lazy)
  (ring-resp/response (str (json/generate-string (q-attrs mbrainz/db)))))

(comment
  
  ;
  )

; ; http://localhost:8893/entity-params?limit=1&offset=0&attribute=%22:release/year%22&fmt=str
; (defn gen-resp-entity-params [request]
;   "Calls aq.query and sends edn as repsonse. Parses individual url string params (limit, offset, attribute etc.) "
;   (let [{query-params :query-params} request
;         {x :x
;          limit :limit
;          offset :offset
;          attribute :attribute
;          fmt :fmt
;          :or {attribute ":artist/name"
;               limit 10
;               offset 0
;               fmt "edn"}} query-params]
;     {:status 200
;      :body (let [body {:data (aq.query/get-paginted-entity {:attribute (edn/read-string attribute)
;                                                             :limit (or (try-parse-int limit) 10)
;                                                             :offset (or (try-parse-int offset) 0)})
;                        :query-params query-params
;                        :random (Math/random)
;                        :uuid (d/squuid)
;                        :x x}]
;              (if (= fmt "edn") body (str body)))}))







; (def gen-resp-entity-default-body {:attribute :artist/name
;                                    :limit 10
;                                    :offset 0
;                                    :fmt "edn"
;                                    :x 3})
; ;; http://localhost:8893/entity?data={:attribute
; (defn gen-resp-entity [request]
;   "Calls aq.query and sends edn as repsonse. Uses edn:  parses only one param 
; (edn/read-string (:data query-params)) "
;   (let [{query-params :query-params} request
;         {data-str :data
;          :or {data-str (str {:attribute :artist/name
;                              :limit 10
;                              :offset 0
;                              :fmt "edn"
;                              :x 3})}} query-params
;         data (merge gen-resp-entity-default-body (edn/read-string data-str))]
;     {:status 200
;      :body (let [body {:data (aq.query/get-paginted-entity data)
;                        :request-data data
;                        :random (Math/random)
;                        :uuid (d/squuid)}]
;              (if (= (:fmt data) "edn") body (str body)))}))



; (defn entity-params [request]
;   (gen-resp-entity-params request))


; (defn entity [request]
;   (gen-resp-entity request))