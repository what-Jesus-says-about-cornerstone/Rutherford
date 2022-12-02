(ns Rutherford.rest-routes-datomicui
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]
            [datomic.api :as d]
            [clojure.edn :as edn]
            [Rutherford.rest-db-datomicui :as dtm]
            [Rutherford.rest-db-starcraft]
            [Rutherford.rest-db-seattle]
            [clojure.pprint :as pp]
            [Rutherford.datomic :refer [q-idents q-attrs]]
            [Rutherford.impl :refer [try-parse-int]]
            [ring.util.response :as ring-resp]
            [clj-http.client :as client]
            [slingshot.slingshot :refer [throw+ try+]]
            [cheshire.core :as json]))

(defn get-attributes
  [request]
  (dtm/connect-lazy)
  (ring-resp/response (str (vec (q-attrs dtm/db)))))


(defn get-attributes-route
  [request]
  (get-attributes request))

(comment

  ;
  )


; ; http://localhost:8893/entity-params?limit=1&offset=0&attribute=%22:release/year%22&fmt=str
(defn get-entity-response [request]
  "Queries db and sends edn as repsonse. Parses individual url string params (limit, offset, attribute etc.) "
  (dtm/connect-lazy)
  (let [{query-params :query-params} request
        q-data (-> query-params (get :data) edn/read-string)
        {
         limit :limit
         offset :offset
         attribute :attribute
         fmt :fmt
         :or {
              ; attribute ":artist/name"
              ; limit 10
              ; offset 0
              fmt "edn"
              }} q-data]
    {:status 200
     :body (let [body {
                       :data (dtm/q-paginted-entity {:attribute attribute
                                                            :limit (try-parse-int limit)
                                                            :offset (try-parse-int offset)})
                      ;  :args  {:attribute (edn/read-string attribute)
                      ;                                       :limit (try-parse-int limit)
                      ;                                       :offset (try-parse-int offset)}
                       :query-params (str q-data)
                       :random (Math/random)
                       :uuid (d/squuid)
                      }]
             (if (= fmt "edn") body (str body)))}))



(defn get-entity-route [request]
  (get-entity-response request))

(defn text-search-response
  [request]
  (dtm/connect-lazy)
  (let [{query-params :query-params} request
        q-data (-> query-params (get :data) edn/read-string)
        {input :input} q-data
        ]
    {:status 200
     :body {:data (dtm/q-text-search {:search input} )}}))

(defn text-search-route [request]
  (text-search-response request))


(defn db-names-response
  [request]
  (dtm/connect-lazy)
  (let [{query-params :query-params} request
        q-data (-> query-params (get :data) edn/read-string)
        ]
    {:status 200
     :body {:data (dtm/q-database-names )}}))

(defn db-names-route [request]
  (db-names-response request))



(comment
  
  (try-parse-int "3")
  
  (as-> nil x
    (try+
     (client/get "http://localhost:8080/datomicui/entity"
                 {:query-params {"data" (str {:attribute ":artist/name"
                                              :limit     3
                                              :offset    2
                                              "fmt"       "edn"})}         
                  :headers      {}})
     (catch [:status 500] {:keys [request-time headers body]}
       (pp/pprint ; (json/parse-string body) 
        body))
     )
    (edn/read-string (:body x))
    (pp/pprint x)
    ; (do
      ; (prn (:status x))
      ; (pp/pprint (keys x))
      ; (pp/pprint (:cookies x))
      ; )
    )
  
  (client/get "http://localhost:8080/datomicui/text-search"
              {:query-params {"data" (str {})}
               :headers      {}})
  
  (client/get "http://localhost:8080/datomicui/db-names"
              {:query-params {"data" (str {})}
               :headers      {}})
  
  (as-> nil x
   (client/get "http://localhost:8080/datomicui/entity?data={:limit 10, :offset 20, :attribute \":artist/name\", :fmt \"edn\"}")
   (edn/read-string (:body x))
   (pp/pprint x))
  
  
  
  
  
  (as-> nil x
    (try+
     (client/post ""
                  {:body         (json/generate-string {:login    ""
                                                        :password ""})
                   :content-type :json
                   :accept       :json
                   :headers      {}})
     (catch [:status 500] {:keys [request-time headers body]}
       (pp/pprint ; (json/parse-string body) 
        body)))
    (do
      (prn (:status x))
      (pp/pprint (keys x))
      (pp/pprint (:cookies x))
      (pp/pprint (-> x :body json/parse-string (get "username")))))
  
  ;
  )



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