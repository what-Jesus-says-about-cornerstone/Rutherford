(ns Rutherford.movies-imdb-etl
  (:require [clojure.repl :refer :all]
            [clojure.pprint :as pp]
            [Rutherford.dgraph :refer [q create-client
                                    drop-all
                                     count-total-nodes
                                     mutate mutate-del set-schema]]
            ; [clojure.data.csv :as csv]
            [Rutherford.movies-csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as cstr]

   ;
            )
  ;
  )

(defn read-column [filename column-index]
  (with-open [reader (io/reader filename)]
    (let [data (csv/read-csv reader)]
      ; (map #(nth % column-index) data) ; lazy
      (mapv #(nth % column-index) data))))

(defn read-csv-file
  [filename]
  (with-open [reader (io/reader filename)]
    (let [data (doall (csv/read-csv reader))]
      data)))

(defn write-file []
  (with-open [w (clojure.java.io/writer  "f:/w.txt" :append true)]
    (.write w (str "hello" "world"))))

(def filename-title-ratings "/opt/.data/imdb/title.ratings.tsv")
(def filename-title-rating-rdf "/opt/.data/imdb.rdf/title.ratings.rdf")


(def filename-names "/opt/.data/imdb/name.basics.tsv")
(def filename-names-rdf "/opt/.data/imdb.rdf/name.basics.rdf")



(def filename-titles "/opt/.data/imdb/title.basics.tsv")
(def filename-titles-rdf "/opt/.data/imdb.rdf/title.basics.rdf")

(def filename-episodes "/opt/.data/imdb/title.episode.tsv")
(def filename-episodes-rdf "/opt/.data/imdb.rdf/title.episode.rdf")

(def filename-akas "/opt/.data/imdb/title.akas.tsv")
(def filename-akas-rdf "/opt/.data/imdb.rdf/title.akas.rdf")

(def filename-crew "/opt/.data/imdb/title.crew.tsv")
(def filename-crew-rdf "/opt/.data/imdb.rdf/title.crew.rdf")

(def filename-principals "/opt/.data/imdb/title.principals.tsv")
(def filename-principals-rdf "/opt/.data/imdb.rdf/title.principals.rdf")


(def filename-all-rdf "/opt/.data/imdb.rdf/all.rdf")
(def filename-sample-rdf "/opt/.data/imdb.rdf/sample.rdf")



(defn nl
  "append newline char to str"
  [s]
  (str s "\n"))

(defn write-lines
  "Write lines vector to file"
  [filename lines-vec]
  (with-open [w (clojure.java.io/writer filename :append true)]
    (doseq [line lines-vec]
      (.write w (nl line)))))

(defn wrap-brackets
  "Wrap string in brackets"
  [s]
  (str "<" s ">"))

(defn wrap-quotes
  "Wrap string in quotes"
  [s]
  (str "\"" s "\""))

(defn split-tab
  "Splits the string by tab char"
  [s]
  (cstr/split s #"\t"))

(defn replace-double-quotes
  [s & {:keys [ch] :or {ch "'" }}]
  (clojure.string/replace s #"\"" ch)
  )

(defn create-ctx
  [data specs]
  (atom {:specs specs
         :genres {}
         :genres-count 1000000
         :genres-prefix-id "gg"
         }))

(defn merge-ctx
  [ctx part]
  (swap! ctx merge @ctx part))

(defn inc-genres-cnt!
  [ctx]
  (as-> nil x
    (inc (:genres-count @ctx))
    (merge-ctx ctx {:genres-count x})))

(defn genre->id!
  [ ctx genre]
  (let [prefix   (:genres-prefix-id @ctx)
        cnt      (:genres-count @ctx)
        genres   (:genres @ctx)
        genre-id (get genres genre)
        id       (str prefix cnt)]

    (cond
      genre-id genre-id
      :else (do
              (merge-ctx ctx {:genres (merge genres {genre id})})
              (inc-genres-cnt! ctx)
              id)
      )))



(comment
  
  (def context (create-ctx {} {}))
  
  (inc-genres-cnt! context)
  
  (genre->id "asd" context)
  
  (get (:genres @context) "3" )
  
  (genre->id! context "Anime" )
  
  ;
  )

(defn apply-imdb-specs-val
  "Returns string. Apply specs to val"
  [attr val specs]
  (let [xform (get-in specs [:val attr] ) ]
    (as-> val e
      
      (if xform (xform e attr specs) e )
      (str  e (get-in specs [:suffix attr])))
    )
  )

(defn apply-imdb-specs-attr
  "Returns string. Apply specs to attr"
  [attr  specs]
  (str (:domain specs) (get-in specs [:subdomains attr]) attr))

(defn tsv-vec->rdf-line
  [id val attr specs]
  (as-> val e
    ; (replace-double-quotes e)
    (cstr/escape e {\\ "\\"})
    (cstr/escape e {\" "\""})
    (vector (wrap-brackets id)
            (wrap-brackets (apply-imdb-specs-attr attr specs))
            (apply-imdb-specs-val attr
                                  (if (get-in specs [:xid attr]) (wrap-brackets e) (wrap-quotes e))
                                  specs)
            ".")
    (cstr/join " " e))
  
  )


(defn tsv-line->rdf-line
  [line attrs specs ctx]
  (let [elems   (split-tab line)
        id   (first elems)
        vals (rest elems)]
    (->>
     (map (fn [val attr]
          ; (prn (if (get-in specs [:xid attr]) (wrap-brackets val) "-"))
            (let [val->arr (get-in specs [:array attr])]
              (cond
                (= val "\\N") nil ;(do (prn val) nil)
                val->arr (map #(tsv-vec->rdf-line id % attr specs) (val->arr val attr specs ctx))
                :else (tsv-vec->rdf-line id val attr specs)))) vals attrs)
     (keep #(if (nil? %) nil %))
    ;  (prn)
     flatten
    ;  (map #(cstr/join " " %))
                  ;
     )))

(defn tsv-strings->rdf-strings
  "Converts tsv strings to rdf strings"
  [tsv-strings specs]
  (let [header (split-tab (ffirst tsv-strings))
        attrs  (rest header)]
    (map (fn [s]
           (tsv-line->rdf-line (first s) attrs specs)
           ;
           ) (rest tsv-strings))
    ;
    ))

(defn read-nth-line
  "Read line-number from the given text file. The first line has the number 1."
  [file line-number]
  (with-open [rdr (clojure.java.io/reader file)]
    (nth (line-seq rdr) (dec line-number))))

(defn count-lines
  [file]
  (with-open [rdr (clojure.java.io/reader file)]
    (count (line-seq rdr) )))

(comment
  
  (read-nth-line filename-names 1)
  
  
  (read-nth-line filename-names-rdf 500000)
  
  (read-nth-line filename-names-rdf 4)
  
  
  (count-lines filename-names)
  (* 9459601 5)
  (* 9459601 4)
  
  (count-lines filename-names-rdf)
  
  (read-nth-line filename-names-rdf 5000000)
  
  
  ;
  )






(def imdb-specs
  {:domain     "imdb."
   :suffix     {"averageRating"  "^^<xs:float>"
                "numVotes"       "^^<xs:int>"
                "birthYear"      "^^<xs:int>"
                "deathYear"      "^^<xs:int>"
                "name"           "@en"
                "isAdult"        "^^<xs:boolean>"
                "startYear"      "^^<xs:int>"
                "endYear"        "^^<xs:int>"
                "runtimeMinutes" "^^<xs:int>"}
   :subdomains {"averageRating"     "title."
                "numVotes"          "title."
                "primaryName"       "name."
                "birthYear"         "name."
                "deathYear"         "name."
                "primaryProfession" "name."
                "knownForTitles"    "name."

                "titleType"         "title."
                "primaryTitle"      "title."
                "originalTitle"     "title."
                "isAdult"           "title."
                "startYear"         "title."
                "endYear"           "title."
                "runtimeMinutes"    "title."
                "genres"            "title."
                "directors"         "title."
                "writers"           "title."}
   :xid        {"knownForTitles" true
                "directors"      true
                "writers"        true
                "genres"         true}
   :val        {"isAdult" (fn [val attr specs]
                    ;  (prn val)
                            (if (= "\"0\"" val) "\"false\"" "\"true\""))}
   :array      {"genres"            (fn [val attr specs ctx]
                      ; (prn val)
                                      (->>
                                       (cstr/split  val #"\,")
                                       (map #(genre->id! ctx %))))
                "knownForTitles"    (fn [val attr specs ctx]
                      ; (prn val)
                                      (cstr/split  val #"\,"))
                "directors"         (fn [val attr specs ctx]
                      ; (prn val)
                                      (cstr/split  val #"\,"))
                "writers"           (fn [val attr specs ctx]
                      ; (prn val)
                                      (cstr/split  val #"\,"))
                "primaryProfession" (fn [val attr specs ctx]
                      ; (prn val)
                                      (cstr/split  val #"\,"))}}
  )

(defn in-steps
  "Process lazy seq in steps"
  [fnc & {:keys [data step total]
          :or   {step 100000}}]
  (let [total*  (or total (count data))
        ran    (range 0 total* step)
        points (concat ran [total*])]
    (prn points)
    (doseq [p points]
      (fnc p ))))

(defn names->rdf
  [filename-in filename-out]
  (with-open [reader (io/reader filename-in)]
    (let [data (csv/read-csv reader)
          step 100000]
      (in-steps
       (fn [p]
         (as-> nil e
           (take step (drop p data))
           (tsv-strings->rdf-strings e imdb-specs)
           (flatten e)
           (write-lines filename-out e)
                    ;
           ))
       :step step
       :data data
      ;  :total 250000
       ;
       ))))

(defn titles->rdf
  [filename-in filename-out]
  (with-open [reader (io/reader filename-in)]
    (let [data (csv/read-csv reader :separator \tab :quote \space )
          step 100000]
      (in-steps
       (fn [p]
         (as-> nil e
           (take step (drop p data))
           (tsv-strings->rdf-strings e imdb-specs)
           (flatten e)
           (write-lines filename-out e)
                    ;
           ))
       :step step
       :data data
       :total 250000
       ;
       ))))


(defn names->rdf-2
  [filename-in filename-out]
  (with-open [reader (io/reader filename-in)
              writer (clojure.java.io/writer filename-out :append true)]
    (let [data        (csv/read-csv reader :separator \tab :qoute \])
          ; header-line (read-nth-line filename-in 1)
          header-line (ffirst data)
          header      (split-tab header-line)
          attrs       (rest header)]
      ; (prn (count data))
      (doseq [
              line (rest data)
              ; line (take 50 (rest data))
              ]
        ; (prn (tsv-line->rdf-line (first line) attrs imdb-specs))
        (as-> nil e
          (tsv-line->rdf-line (first line) attrs imdb-specs)
          (cstr/join \newline e)
          ; (str e "\n")
          (str e \newline)
          ; (do (prn e) e)s
          (.write writer e)
          ;
          )))))


(defn genres->rdf
  [filename-out  specs ctx]
  (with-open [writer (clojure.java.io/writer filename-out :append true)]
    (doseq [[genre id] (:genres @ctx)]
      (as-> nil e
        (tsv-vec->rdf-line id genre "genre.name" specs)
      ; (do (prn e) e)
      ; (cstr/join \newline e)
        (str e \newline)
        (.write writer e)
          ;
        ))))

(defn names->rdf-3
  [filename-in filename-out ctx specs & {:keys [limit ]  }]
  (with-open [reader (io/reader filename-in)
              writer (clojure.java.io/writer filename-out :append true)]
    (let [data        (line-seq reader)
          ; header-line (read-nth-line filename-in 1)
          header-line (first data)
          header      (split-tab header-line)
          attrs       (rest header)
          lines (if limit (take limit (rest data)) (rest data) )
          
          ]
      ; (prn header-line)
      ; (prn header)
      ; (prn attrs)
      (doseq [
              ; line (rest data)
              line lines
              ]
        ; (prn (tsv-line->rdf-line (first line) attrs imdb-specs))
        (as-> nil e
          (tsv-line->rdf-line line attrs specs ctx)
          (cstr/join \newline e)
          ; (str e "\n")
          (str e \newline)
          ; (do (prn e) e)s
          (.write writer e)
          ;
          ))
      
      )))

(defn files->rdfs
  [filenames filename-out specs & {:keys [limits limit] }]
  (let [ctx (create-ctx nil specs)]
    (time
     (do
       (doseq [src filenames]
         (names->rdf-3  src filename-out ctx specs :limit (or (get limits src) limit)))
       (genres->rdf  filename-out  specs ctx)))))


(comment

  (def orig-files [filename-names
                   filename-titles
                   filename-title-ratings])

  (def rdf-files [filename-title-rating-rdf
                  filename-names-rdf
                  filename-titles-rdf
                  filename-episodes-rdf
                  filename-crew-rdf
                  ; filename-all-rdf
                  filename-sample-rdf
                  ])

  (doseq [filename rdf-files]
    (.delete (java.io.File. filename)))

  (count-lines filename-names)  ;  9459601
  (count-lines filename-titles)  ;  6018812
  (count-lines filename-title-ratings)  ;954350
  (count-lines filename-crew)  ;  6018812
  (count-lines filename-akas)  ;  3808942
  (count-lines filename-episodes)  ;  4190729
  (count-lines filename-principals)  ;  34717826



  (names->rdf-3  filename-names filename-names-rdf :limit 1000)
  (names->rdf-3  filename-titles filename-titles-rdf :limit 100)
  (names->rdf-3  filename-title-ratings filename-title-rating-rdf :limit 1000)
  (names->rdf-3  filename-crew filename-crew-rdf :limit 1000)

  (count-lines filename-all-rdf)  ; 94024551
  (-> 94024551 (/ 20000) (/ 60) float) ; 78min
  (count-lines filename-sample-rdf)  ; 17291723  
  (.delete (java.io.File. filename-all-rdf))

  ; (time
  ;  (doseq [src [filename-names filename-titles filename-title-ratings filename-crew]]
  ;    (names->rdf-3  src filename-all-rdf :limit 100000)))


  (files->rdfs [filename-names filename-titles filename-title-ratings filename-crew]
               filename-sample-rdf
               imdb-specs
              ;  :limit 1000000
               )
  
  ; (cstr/escape "Fred /3 Astaire" {\3 "\\" })
  
  ; (cstr/replace "Fred /\ Astaire" #"A" "3"  )
  
  ; (cstr/escape "a b \" " {\" "\""})
  

  (def c (create-client {:with-auth-header? false
                         :hostname          "server"
                         :port              9080}))


  (count-total-nodes c)

  (drop-all c)

  ;
  )


(comment

  (split-tab (read-nth-line filename-titles 1))

  (read-nth-line filename-titles 10000)

  (read-nth-line filename-titles 525045)
  
  (read-nth-line filename-titles 525044)
  
  (read-nth-line filename-names 100)
  

;tt0544863	tvEpisode	"Consuela" (Or 'The New Mrs Saunders')	"Consuela" (Or 'The New Mrs Saunders')	0	1986	\N	36	Comedy

  (count-lines filename-titles)

  (count-lines filename-titles-rdf)
  
  (count-lines filename-names-rdf)
  
  (count-lines filename-names)
  
  (read-nth-line filename-names 1)
  
  (split-tab (read-nth-line filename-titles 679321))
  
  (split-tab (read-nth-line filename-titles 14))
  
  
  
  (count-lines filename-all-rdf)
  
  

  ;
  )


(comment

  (def title-ratings (read-csv-file  filename-title-ratings))

  (names->rdf  filename-names filename-names-rdf)
  
  (names->rdf-2  filename-names filename-names-rdf)
  
  (names->rdf-2  filename-titles filename-titles-rdf)
  
  (names->rdf-3  filename-names filename-all-rdf)
  
  (names->rdf-3  filename-titles filename-all-rdf)
  
  
  (titles->rdf filename-titles filename-titles-rdf)

  (.mkdirs (java.io.File. "/opt/.data/imdb.rdf"))

  (.delete (java.io.File. filename-title-rating-rdf))

  (.delete (java.io.File. filename-names-rdf))

  (.delete (java.io.File. filename-titles-rdf))
  

  (.createNewFile (java.io.File. filename-title-rating-rdf))

  (spit filename-title-rating-rdf (nl "<123> <name> \"asd\" .") :append true)

  (count title-ratings)

  (type title-ratings)

  (take 10 title-ratings)



  (cstr/split "tt0000002\t6.3\t185" #"\t")

  (cstr/join  "3" ["asd" "d"])


  (->
   (tsv-strings->rdf-strings  (take 10 title-ratings) imdb-specs)
   flatten
   pp/pprint)

  (def title-ratings-rdfs (tsv-strings->rdf-strings title-ratings imdb-specs))

  ; (count title-ratings-rdfs)


  (->>
   (drop 500000 title-ratings-rdfs)
   (take 10)
   pp/pprint)

  (->>
   (drop 500000 title-ratings-rdfs)
   (take 100000)
   flatten
   (write-lines filename-title-rating-rdf))

  (let [total  (count title-ratings)
        step   100000
        ran    (range 0 total step)
        points (concat ran [total])]
    (prn points)
    (doseq [p points]
      (->>
       (take step (drop p title-ratings-rdfs))
       flatten
       (write-lines filename-title-rating-rdf))))




  ;
  )

(defn find-uids
  [{:keys [query client vars]}]
  (as-> nil e
   (q {:qstring "{
    all(func: has(imdb.name.primaryName)) {
    uid
    }
  }"
       :client  client
       :vars    (or vars {})})
   (get e "all")
   (map #(get % "uid") e)
   ))

(defn delete-uids
  [{:keys [query client vars] :as opts}]
  (as-> nil e
    (find-uids opts )
    (map #(str (wrap-brackets %) " *" " *" " .") e)
    (cstr/join \newline e)
    ; (str "{ delete
    ;      { 
    ;      " e "
    ;      }
    ;      }")
    ; (spit "/opt/.data/imdb/delete1" e)
    (mutate-del {:s e
                 :client client
                 })
    )
  )

(comment

  (def c (create-client {:with-auth-header? false
                         :hostname          "server"
                         :port              9080}))

  (mutate {:data   {"name" "John"}
           :client c})

  (->
   (q {:qstring "{
    all(func: has(imdb.name.primaryName)) {
    uid
    }
  }"
       :client  c
       :vars    {}})

   (pp/pprint))

  (->>
   (find-uids {:client c
               :query "{
    all(func: has(imdb.name.primaryName)) {
    uid
    }
  }"
               })
   (map #(wrap-brackets %))
   )
  
  (delete-uids {:client c
                :query  "{
    all(func: has(imdb.name.primaryName)) {
    uid
    }
  }"})


  (mutate-str {:s      "
               {
                 delete {
                   * <imdb.name.primaryName> * .
                 }
               }
               "
               :client c})


  (def mother-of-all-files
    (with-open [rdr (clojure.io/reader "/home/user/.../big_file.txt")]
      (into []
            (comp (partition-by #(= % "")) ;; splits on empty lines (double \n)
                  (remove #(= % "")) ;; remove empty lines
                  (map #(clojure.string/join "\n" %)) ;; group lines together
                  (map clojure.string/trim))
            (line-seq rdr))))

  ;
  )