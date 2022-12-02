(ns Rutherford.nba-api
  (:require [clojure.repl :refer :all]
            [clj-http.client :as client]
            [cheshire.core :as json]
            [ala.print :refer [cprn]]))


; https://stats.nba.com/stats/leaguedashplayerptshot/?LeagueID=00&Season=2015-16&Permode=PerGame&SeasonType=Regular%20Season
; https://stats.nba.com/stats/drafthistory/?LeagueID=00
; https://stats.nba.com/stats/scoreboard/?GameDate=02/14/2015&LeagueID=00&DayOffset=0


(def ^:private user-agent-moz "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36") ; noqa: E501

(def ^:private user-agent-lin "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36")

(def ^:private cookie "check=true; AMCVS_248F210755B762187F000101%40AdobeOrg=1; _ga=GA1.2.1179153281.1558022564; ug=5cdd89a30be91a0a3f9cca0014f73bd6; AMCVS_7FF852E2556756057F000101%40AdobeOrg=1; s_vi=[CS]v1|2E6EC4D20503339D-60001198C00015C6[CE]; s_ecid=MCMID%7C91962061547148632861157145715371946879; AMCV_7FF852E2556756057F000101%40AdobeOrg=1687686476%7CMCIDTS%7C18033%7CMCMID%7C91962061547148632861157145715371946879%7CMCAAMLH-1558627365%7C6%7CMCAAMB-1558627365%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1558029764s%7CNONE%7CMCAID%7C2E6EC4D20503339D-60001198C00015C6%7CvVersion%7C3.0.0; __gads=ID=068b286030d6ca48:T=1558022565:S=ALNI_MZSSqXPyPBLru6jAGadTSrt68ot5Q; _fbp=fb.1.1558022565499.1946676558; _gcl_au=1.1.85998875.1558022760; AAMC_nba_0=REGION%7C6; aam_uuid=91922302562689543581153239902168821567; AMCVS_248F210755B762187F000101%2540AdobeOrg%40AdobeOrg=1; AMCV_248F210755B762187F000101%2540AdobeOrg%40AdobeOrg=-1303530583%7CMCAID%7C2E6EC4D20503339D-60001198C00015C6%7CMCIDTS%7C18033%7CMCMID%7C2E6EC4D20503339D-60001198C00015C6%7CMCOPTOUT-1558029967s%7CNONE%7CvVersion%7C3.3.0; s_ppvl=nba%253Ascores%2C100%2C100%2C981%2C1873%2C981%2C1920%2C1080%2C2%2CP; s_tps=6; s_pvs=9; mp_nba_store_mixpanel=%7B%22distinct_id%22%3A%20%2216ac164c2c53d-0bf898a39cfa76-3f770c5a-1fa400-16ac164c2c6623%22%7D; s_sess=%20s_cc%3Dtrue%3B%20tp%3D1952%3B%20s_ppv%3Dnba%25253Ailp%25253Avideo%25253Agames%252C100%252C72%252C1951%3B; AMCV_248F210755B762187F000101%40AdobeOrg=1687686476%7CMCIDTS%7C18033%7CMCMID%7C91958614992296153051156930789105193216%7CMCAAMLH-1558627916%7C6%7CMCAAMB-1558627916%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1558030316s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C3.0.0; s_cc=true; s_fid=3C1AFF8BC9BA613C-14599FE0B944C1C9; s_sq=%5B%5BB%5D%5D; mbox=PC#f350a1606e9340dcad217d0f8a3a785e.22_22#1621270013|session#26b13d42c98b4e428913093f41cbe990#1558065107; ak_bmsc=B516B5863FFE474F2F8B92D5782ADDC102161F3E727A00001D95DF5C64F9403A~pldbr3QFMnvy4bVllqbtFc5oOfmSNc3xiigpbxst4pHBk5hZlT20ZUamVJ5bYQ/eaAUyanRSoKx0jLP6bgRhksNmMk5pyL8Uioz5j7ALqFOaFG0vR2ckjY6ftg4e06FjiqFt2ifvt/GwqRALoHYG6km1Rde4WtTC+YTlcHxut9syyqQOR268CKAPZAE+A09+UFXQc4z4TsmT1mzrSg6a3N9TPJgidZ1om3GoofBmtS7M0=")


(def ^:private headers-nba-stats {"user-agent"      user-agent-moz
                                  "Dnt"             "1"
                                  "Accept-Encoding" "gzip, deflate, sdch"
                                  "Accept-Language" "en"
                                  "origin"          "http://stats.nba.com"})

(def ^:private headers-nba-stats-2 {"User-Agent"                user-agent-lin
                                    "Upgrade-Insecure-Requests" 1
                                    "Host"                      "stats.nba.com"
                                    "Accept"                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3"
                                    "Accept-Encoding"           "gzip, deflate, br"
                                    "Accept-Language"           "en-US,en;q=0.9,ru;q=0.8,de;q=0.7"
                                    "Cache-Control"             "max-age=0"
                                    "Connection"                "keep-alive"
                                    "Origin"                    "http://stats.nba.com"
                                    "Cookie"                    cookie
                                    "referrer"                  "http://stats.nba.com/scores/"
                                    "referer"                   "http://stats.nba.com/scores/"
                                    "origin"                    "http://stats.nba.com"
                             ;
                                    })

(def ^:private response-keys '(:cached
                               :request-time
                               :repeatable?
                               :protocol-version
                               :streaming?
                               :http-client
                               :chunked?
                               :cookies
                               :reason-phrase
                               :headers
                               :orig-content-encoding
                               :status
                               :length
                               :body
                               :trace-redirects))

(def ^:private URIs   (let
                       [hostname "http://stats.nba.com"
                        base-uri (str hostname "/stats")]
                        {:hostname   hostname
                         :base-uri   base-uri
                         :scoreboard (str base-uri "/scoreboard")}))

(defn- trim-resp
  "Returns the resp's body parsed as edn"
  [R & {:keys [parse-body?]
        :or   {parse-body? true}}]
  (merge
   (select-keys R [:trace-redirects :status])
   {:body (if parse-body? (json/parse-string (R :body) true) (R :body))}))

(defn- fetch-nba-stats
  "Makes http req to stats.nba"
  [path {:keys [qp]}]
  (client/get (str (URIs :base-uri)  path)
              {:query-params          (merge {:LeagueID "00"} qp)
               :headers               headers-nba-stats-2
               :async?                false
               :throw-entire-message? true}
                ; (fn [response] (cprn response))
                ; (fn [exception] (cprn (.getMessage exception)))
              ))

(defn- get-nba-stats
  "Makes http req to stats.nba and parses it"
  [path {:keys [qp]
         :as   opts}]
  (try
    (->
     (fetch-nba-stats path opts)
     trim-resp)
    (catch Exception e (trim-resp (ex-data e) :parse-body? false))))



(defn scoreboard
  [& {:keys [qp]}]
  (->
   (get-nba-stats "/scoreboard" {:qp {:GameDate  "02/14/2015"
                                      :DayOffset "0"}})))

(defn shotchartdetail
  [& {:keys [qp]}]
  (->
   (get-nba-stats "/shotchartdetail" {:qp {:Season "2014-15"}})))



(comment

  (client/get "http://stats.nba.com/stats/scoreboard/?GameDate=02/14/2015&LeagueID=00&DayOffset=0"
              {:headers headers-nba-stats})

  (as-> nil R
    (client/get "http://stats.nba.com/stats/scoreboard/?GameDate=02/14/2015&LeagueID=00&DayOffset=0"
                {:headers headers-nba-stats-2
                 :accept  :json})
    (R :body)
    (json/parse-string R true))

  (client/get "http://example.com"
              {:headers {:foo      ["bar" "baz"]
                         :eggplant "quux"}})

  (def res (->
            (get-nba-stats "/scoreboard" {:qp {:GameDate  "02/14/2015"
                                               :DayOffset "0"}})))

  


  ;;;
  )
