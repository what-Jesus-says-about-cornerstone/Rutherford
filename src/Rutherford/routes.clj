(ns Rutherford.routes
  (:require [io.pedestal.http :as http]
            [io.pedestal.http.route :as route]
            [io.pedestal.http.body-params :as body-params]
            [ring.util.response :as ring-resp]
            [io.pedestal.http.ring-middlewares :refer [cookies]]
            [cheshire.core :as json]
            [clojure.pprint :as pp]
            [clj-http.client :as client]
            [slingshot.slingshot :refer [throw+ try+]]
            [Rutherford.impl :refer [version]]))

(defn about-page
  [request]
  (ring-resp/response (format "Clojure %s - served from %s"
                              (clojure-version)
                              (route/url-for ::about-page))))

(defn redirect-page
  [request]
  (prn request)
  (-> (ring-resp/redirect  "http://localhost:7080" 301)
      (ring-resp/set-cookie "hello" "world")  
      ; (assoc :session {:name "asd"})
      )
  )



(comment

  (as-> nil x
    (try+
     (client/post ""
                  {:body (json/generate-string {:login    ""
                                                :password ""})
                   :content-type :json
                   :accept :json
                   :headers {}
                   })
     (catch [:status 500] {:keys [request-time headers body]}
       (pp/pprint ; (json/parse-string body) 
                  body
                  )))
    (do 
      (prn (:status x))
      (pp/pprint (keys x))
      (pp/pprint (:cookies x))
      (pp/pprint (-> x :body json/parse-string (get "username") ))
      )
    )

  ;
  )

(defn stew-page
  [req]
  (prn req)
  (let [
        ; {
        ;  login :login
        ;  pssw  :pssw} (:params req)
         pr (try+
             (client/post ""
                          {:body         (json/generate-string {:login    ""
                                                                :password ""})
                           :content-type :json
                           :accept       :json
                           :headers      {}})
             (catch [:status 500] {:keys [request-time headers body status]}
               (pp/pprint ; (json/parse-string body) 
                status)))
        cookies (-> (:cookies pr) json/generate-string (json/parse-string true) )
        ]
  (-> (ring-resp/response  (json/generate-string {:rand (rand-int 5)
                                                  :status (:status pr)
                                                  :cookies ((str cookies))
                                                  :body   (-> pr :body json/parse-string)}))
      ; (ring-resp/set-cookie "hello" "world")
      ; (assoc :cookies cookies)
      (assoc :cookies {"session_id" {:value "session-id-hash"}})
      )))

(def n (atom 0))

(defn home-page
  [request]
  (swap! n inc )
  (ring-resp/response (str (version) "Hello World! #" @n )))

(def common-interceptors [(body-params/body-params) http/html-body cookies])

(def routes #{["/" :get (conj common-interceptors `home-page)]
              ["/about" :get (conj common-interceptors `about-page)]
              ["/r" :get (conj common-interceptors `redirect-page)]
              ["/s" :get (conj common-interceptors `stew-page)]
              ;
              })

;(def routes `{"/" {:interceptors [(body-params/body-params) http/html-body]
;                   :get home-page
;                   "/about" {:get about-page}}})

;(def routes
;  `[[["/" {:get home-page}
;      ^:interceptors [(body-params/body-params) http/html-body]
;      ["/about" {:get about-page}]]]])


(def service {:env :prod
              ;; ::http/interceptors []
              ::http/routes routes

              ;; "http://localhost:8080"
              ;;
              ;;::http/allowed-origins ["scheme://host:port"]
              ::http/allowed-origins ["*"]
              ::http/resource-path "/public"

              ::http/type :jetty
              ;;::http/host "localhost"
              ::http/host "0.0.0.0"
              ::http/port 8080
              ::http/container-options {:h2c? true
                                        :h2? false
                                        ;:keystore "test/hp/keystore.jks"
                                        ;:key-password "password"
                                        ;:ssl-port 8443
                                        :ssl? false}
              ;
              })

