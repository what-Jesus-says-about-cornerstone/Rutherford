(ns Rutherford.movies-imdb-psql
  (:require [clojure.repl :refer :all]
            [clojure.pprint :as pp]
            [Rutherford.movies-csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as cstr]
            [clojure.java.jdbc :as jdbc]
            [Rutherford.impl :refer [prn-members nth-seq split-tab drop-nth]]
            [Rutherford.io :refer [delete-files create-file
                                  read-nth-line count-lines mk-dirs]]
            [clj-time.core :as ctime]
            [clj-time.format :as ctimef]
            [clj-time.jdbc]
   ;
            )
  ;
  )


(def db
  {:dbtype   "postgresql"
   :dbname   "postgresdb"
   :user     "postgres"
   :host     "postgres-imdb"
   :port     5432
   :password "postgres"})

(comment

  (jdbc/execute! db ["
                         
    CREATE TABLE account(
                         user_id serial PRIMARY KEY,
                              username VARCHAR (50) UNIQUE NOT NULL,
                              password VARCHAR (50) NOT NULL,
                              email VARCHAR (355) UNIQUE NOT NULL,
                              created_on TIMESTAMP NOT NULL,
                              last_login TIMESTAMP
                         );
                         "])

  (jdbc/query db ["select * from account"])

  
  (jdbc/execute! db ["DROP TABLE account"])
  
  
  (.getTime (java.util.Date.))
  (prn-members (java.util.Date.))
  (str (java.util.Date.))
  (.toGMTString (java.util.Date.))

  (java.sql.Timestamp/valueOf "2004-10-19 10:23:54")
  (type (java.sql.Timestamp/valueOf "2004-10-19 10:23:54"))
  (java.sql.Timestamp/valueOf (.toString (java.util.Date.)))

  (ctime/now)

  (jdbc/insert! db "account" {"username"   "leo"
                                     "password"   "root"
                                     "email"      "vinci@gmail.com"
                                     "created_on" (ctime/now)
                                     "last_login" (ctime/now)})

  (.toGMTString (java.util.Date.))
  (.toString (java.util.Date.))
  (.toLocaleString (java.util.Date.))
  (.ttb (java.util.Date.))


  (ctimef/show-formatters)

  (ctime/default-time-zone)

  (def formatter-mysql (f/formatters :mysql))
  (ctimef/parse formatter-mysql (.toString (java.util.Date.)))
  (def multi-parser (f/formatter (t/default-time-zone)  :mysql "YYYY-MM-dd"))

  (f/unparse multi-parser (f/parse multi-parser "2012-02-01"))

  ;
  )

(comment 
  (jdbc/execute! db ["DROP TABLE titles"])
  (jdbc/execute! db ["DROP TABLE names"])
  (jdbc/execute! db ["DROP TABLE crew"])
  (jdbc/execute! db ["DROP TABLE ratings"])
  
  (jdbc/execute! db ["
                     DROP SEQUENCE table_id_seq
                     "])
  
  (jdbc/query db ["select * from titles"])
  
  (->>
   (jdbc/query db ["select * from titles offset 5 limit 5"])
   pp/pprint
   )
  
  
  
  ;
  )

(def filedir "/opt/.data/imdb/")

(def filenames {:titles  "title.basics.tsv"
                 :names   "name.basics.tsv"
                 :crew    "title.crew.tsv"
                 :ratings "title.ratings.tsv"
                 :akas "title.akas.tsv"
                 :principals "title.principals.tsv"
                 :episode "title.episode.tsv"
                })

(def filedir-out "/opt/.data/imdb.out/")
(def filenames-out {:director-credits "director_credits.tsv"
                    :writer-credits   "writer_credits.tsv"
                    :titles           "titles.tsv"
                    :names            "names.tsv"
                    :akas             "akas.tsv"
                    :episodes         "episodes.tsv"
                    :known-for-titles "known_for_titles.tsv"
                    :akas-types "akas_types.tsv"
                    :title-genres "title_genres.tsv"
                    })

(def files (reduce-kv (fn [acc k v]
                        (assoc acc k (str filedir v))) {} filenames))
(def files-out (reduce-kv (fn [acc k v]
                        (assoc acc k (str filedir-out v))) {} filenames-out))

(defn process-file!
  "Processes an in-file line by line in a lazy manner
   and writes to out-file"
  [filename-in
   filename-out
   ctx
   line->lines
   & {:keys [limit offset with-header]
      :or   {offset 0 with-header true }}]
  (->
   (with-open [rdr (io/reader filename-in)
               wtr (clojure.java.io/writer filename-out :append true)]
     (let [data        (line-seq rdr)
           header-line (first data)
           header      (split-tab header-line)
          ; attrs       (rest header)
           lines       (if limit (take limit (drop offset (rest data))) (rest data))]
       (if with-header (do (.write wtr (str header-line \newline))))
       (doseq [line lines]
         (as-> line e
          ; (do (prn e) e)
           (line->lines e header ctx)
           (cstr/join \newline e)
           (str e \newline)
           (.write wtr e)
          ;
           ))))
   (time)
   ))




(comment
  
  (get-file-spec spec :names-out)
  (source cstr/join)
  
  ;
  )


; (defn files->rdfs
;   [filenames filename-out specs & {:keys [limits limit]}]
;   (let [ctx (create-ctx nil specs)]
;     (time
;      (do
;        (doseq [src filenames]
;          (names->rdf-3  src filename-out ctx specs :limit (or (get limits src) limit)))
;        (genres->rdf  filename-out  specs ctx)))))

(comment
  (mk-dirs filedir-out)

  (process-file! (:crew files) (:director-credits files-out)  {}
                 (fn [line header ctx]
                   (let [vals      (split-tab line)
                         title     (nth-seq vals 0)
                         directors (cstr/split (nth-seq vals 1) #",")]
                     (map (fn [x]
                            (cstr/join  \tab [title x])) directors)))
                ;  :offset 2000  :limit 10
                 )

  (split-tab (read-nth-line (:director-credits files-out) 2))

  (process-file! (:crew files) (:writer-credits files-out)  {}
                 (fn [line header ctx]
                   (let [vals    (split-tab line)
                         title   (nth-seq vals 0)
                         writers (cstr/split (nth-seq vals 2) #",")]
                     (map (fn [x]
                            (cstr/join  \tab [title x])) writers)))
                ;  :offset 2000  :limit 10
                 )

  (process-file! (:names files) (:known_for_titles files-out)  {}
                 (fn [line header ctx]
                   (let [vals   (split-tab line)
                         nconst (first vals)
                         titles (cstr/split (last vals) #",")]
                     (map (fn [x]
                            (cstr/join  \tab [nconst x])) titles)))
                ;  :offset 0  :limit 10
                 )

  (process-file! (:names files) (:names files-out)  {}
                 (fn [line header ctx]
                   (let [vals   (split-tab line)]
                     (list (cstr/join \tab (drop-last vals)))))
                ;  :offset 0  :limit 10
                 )


  (process-file! (:titles files) (:titles files-out)  {}
                 (fn [line header ctx]
                   (let [vals   (split-tab line)]
                     (list (cstr/join  \tab (drop-last vals)))))
                ;  :offset 0  :limit 10
                 )

  (process-file! (:akas files) (:akas-types files-out)  {}
                 (fn [line header ctx]
                   (let [vals     (split-tab line)
                         tconst   (first vals)
                         ordering (second vals)
                         types    (cstr/split (nth-seq vals 5) #",")]
                     (map (fn [x]
                            (cstr/join  \tab [tconst ordering x])) types)))
                ;  :offset 0  :limit 10
                 )

  (process-file! (:akas files) (:akas-types files-out)  {}
                 (fn [line header ctx]
                   (let [vals     (split-tab line)
                         tconst   (first vals)
                         ordering (second vals)
                         types    (cstr/split (nth-seq vals 5) #",")]
                     (map (fn [x]
                            (cstr/join  \tab [tconst ordering x])) types)))
                ;  :offset 0  :limit 10
                 )
  
  (process-file! (:akas files) (:akas files-out)  {}
                 (fn [line header ctx]
                   (let [vals     (split-tab line)]
                     (list (cstr/join  \tab (drop-nth 5 vals)))))
                ;  :offset 0  :limit 10
                 )
  
  (process-file! (:titles files) (:title-genres files-out)  {}
                 (fn [line header ctx]
                   (let [vals     (split-tab line)
                         tconst   (first vals)
                         genres    (cstr/split (last vals) #",")]
                     (map (fn [x]
                            (cstr/join  \tab [tconst x])) genres)))
                ;  :offset 0  :limit 10
                 )

  (->
   (source drop-last)
   time)

  (drop-last [1 2 3])

  (seq 1)

  (count-lines (:akas files))
  (split-tab (read-nth-line (:akas files) 2500000))

  (split-tab (read-nth-line (:known_for_titles files-out) 2))

  (split-tab (read-nth-line (:episode files) 10000))
  
  (split-tab (read-nth-line (:writer-credits files-out) 2))
  
  (split-tab (read-nth-line (:crew files) 3))
  
  (split-tab (read-nth-line (:title-genres files-out) 868))
  

  ;
  )

(defn drop-tables
  [db tables]
  (doseq [name tables]
    (jdbc/execute! db [(str "DROP TABLE IF EXISTS " name)])))

(comment
  
  (delete-files
  ;  (:names files-out)
              ;  (:known_for_titles files-out)
   (:writer-credits files-out)
   (:director-credits files-out))
  
  (delete-files (:title-genres files-out) )
  (delete-files "/opt/.data/imdb.out/title.crew.out.tsv" )
  
  (drop-tables db ["titles" "names" "ratings"
                   "akas" "episodes" "director_credits"
                   "writer_credits" "known_for_titles"
                   "title_genres" "akas_types"])

  (drop-tables db ["akas_types" ])

  ;
  )

(comment

  (jdbc/execute! db ["                     
                     CREATE SEQUENCE table_id_seq
                     "])

  (jdbc/execute! db ["
    CREATE TABLE titles(
                              tconst VARCHAR(50) PRIMARY KEY NOT NULL, 
                              titleType VARCHAR(50),
                              primaryTitle  VARCHAR (512),
                              originalTitle VARCHAR (512),
                              isAdult INT,
                              startYear INT,
                              endYear INT,
                              runtimeMinutes INT
                              -- genres [string]
                         );
                         "])
  (jdbc/execute! db ["
    CREATE TABLE names(
                              nconst VARCHAR(50) PRIMARY KEY NOT NULL, 
                              primaryName  VARCHAR (512),
                              birthYear INT,
                              deathYear INT,
                              primaryProfession VARCHAR (512)
                              -- knownForTitles VARCHAR (256)
                         );
                         "])

  (jdbc/execute! db ["
    CREATE TABLE ratings(
                              id    SERIAL PRIMARY KEY,
                              tconst VARCHAR (50) NOT NULL,
                              averageRating FLOAT8,
                              numVotes  INT
                         );
                         "])

  (jdbc/execute! db ["
    CREATE TABLE akas(
                              tconst  VARCHAR (50),
                              ordering  INT,
                              title TEXT ,
                              region VARCHAR (50),
                              language VARCHAR (50),
                              -- types 
                              attributes TEXT,
                              isOriginalTitle INT,
                              PRIMARY KEY (tconst, ordering)
                         );
                         "])

  (jdbc/execute! db ["
    CREATE TABLE episodes(
                              tconst  VARCHAR (50) PRIMARY KEY NOT NULL,
                              parentTconst VARCHAR (50),
                              seasonNumber INT,
                              episodeNumber INT
                         );
                         "])


  (jdbc/execute! db ["
    CREATE TABLE director_credits(
                              id    SERIAL PRIMARY KEY,
                              tconst VARCHAR (50) NOT NULL,
                              nconst VARCHAR (50)
                         );
                         "])

  (jdbc/execute! db ["
    CREATE TABLE writer_credits(
                              id    SERIAL PRIMARY KEY,
                             tconst VARCHAR (50) NOT NULL,
                              nconst VARCHAR (50)
                         );
                         "])

  (jdbc/execute! db ["
    CREATE TABLE known_for_titles(
                              id    SERIAL PRIMARY KEY,
                              nconst VARCHAR(50) NOT NULL, 
                              tconst VARCHAR (50 )
                         );
                         "])

  (jdbc/execute! db ["
    CREATE TABLE title_genres(
                              id    SERIAL PRIMARY KEY,
                              tconst VARCHAR (50),
                              name TEXT
                         );
                         "])

  (jdbc/execute! db ["
    CREATE TABLE akas_types(
                              id    SERIAL PRIMARY KEY,
                              tconst VARCHAR (50),
                              ordering INT,
                              name TEXT
                         );
                         "])
  
  (jdbc/execute! db ["
    CREATE TABLE principals(
                              tconst VARCHAR (50)  NOT NULL,
                              ordering INT,
                              nconst VARCHAR (50) NOT NULL,
                              category TEXT,
                              job TEXT,
                              characters TEXT,
                              PRIMARY KEY (tconst, ordering)
                         );
                         "])


  ;
  )
  
(defn import-tsv
  [db filename table & {:keys [cols] }]
  (->
   (jdbc/execute! db [(str "
                     COPY " table (if cols (str "(" (cstr/join \, cols) ")") "") " FROM "
                           "'" filename "'"
                           " DELIMITER E'\t' 
          NULL '\\N'  QUOTE E'\b' ESCAPE E'\b' CSV HEADER 
                     ")])
   (time)))

(comment

  (import-tsv db (:ratings files) "ratings" :cols ["tconst" "averageRating" "numVotes"])
  ; 954349

  (import-tsv db (:principals files) "principals")
  ; 34717825

  (import-tsv db (:episode files) "episodes")
  ; 4190728

  (import-tsv db (:titles files-out) "titles")
  ; 6018810
  (jdbc/query db ["select count(*) from titles"])

  (import-tsv db (:names files-out) "names")
  ; 9459599

  (import-tsv db (:writer-credits files-out) "writer_credits" :cols ["tconst" "nconst"])
  ; 10068413

  (import-tsv db (:director-credits files-out) "director_credits" :cols ["tconst" "nconst"])
  ; 7106571

  (import-tsv db (:known-for-titles files-out) "known_for_titles" :cols ["nconst" "tconst"])
  ; 16604682


  (import-tsv db (:title-genres files-out) "title_genres" :cols ["tconst" "name"])
  ; 9612240


  (import-tsv db (:akas files-out) "akas")
  ; 3808940


  (import-tsv db (:akas-types files-out) "akas_types" :cols ["tconst" "ordering" "name"])
  ; 3808940



  ;
  )


(comment

  (jdbc/execute! db [(str "
                     COPY titles FROM "
                          "'" (:titles files) "'"
                          " DELIMITER E'\t' 
          NULL '\\N'  QUOTE E'\b' ESCAPE E'\b' CSV HEADER 
                     ")])
  ; 6018811

  (jdbc/execute! db [(str "
                     COPY names FROM "
                          "'" (:names files) "'"
                          " DELIMITER E'\t' 
          NULL '\\N'  QUOTE E'\b' ESCAPE E'\b' CSV HEADER 
                     ")])
  ; 9459600

  (jdbc/execute! db [(str "
                     COPY crew(tconst,directors,writers) FROM "
                          "'" (:crew files) "'"
                          " DELIMITER E'\t' 
          NULL '\\N'  QUOTE E'\b' ESCAPE E'\b' CSV HEADER 
                     ")])
  ; 6018811

  (jdbc/execute! db [(str "
                     COPY ratings(tconst,averageRating,numVotes) FROM "
                          "'" (:ratings files) "'"
                          " DELIMITER E'\t' 
          NULL '\\N'  QUOTE E'\b' ESCAPE E'\b' CSV HEADER 
                     ")])
  ; 954349
  

  ;
  )


(defn pqry
  [db query-vec]
  (time (->
         (jdbc/query db query-vec)
         (pp/pprint))))

(comment
  
  (->
   (jdbc/query db ["
               SELECT 
               t.primaryTitle,
               r.averageRating
               FROM titles t
               INNER JOIN ratings r on t.tconst = r.tconst  
               LIMIT 10
               
               "])
   (pp/pprint)
   )
  
  (->
   (jdbc/query db ["
               SELECT 
               t.primaryTitle,
               r.averageRating
               FROM titles t
               LEFT JOIN ratings r on t.tconst = r.tconst  
               LIMIT 10
               
               "])
   (pp/pprint))
  
  (->
   (jdbc/query db ["
               SELECT 
               t.primaryTitle,
               r.averageRating
               FROM titles t
               RIGHT JOIN ratings r on t.tconst = r.tconst  
               LIMIT 10
               
               "])
   (pp/pprint))
  
  (->
   (jdbc/query db ["
               SELECT 
               t.primaryTitle,
               r.averageRating
               FROM titles t
               FULL JOIN ratings r on t.tconst = r.tconst  
               LIMIT 10
               
               "])
   (pp/pprint))
  
  (pqry db ["
               SELECT 
               t.primaryTitle,
               r.averageRating,
               r.numVotes
               FROM titles t
               INNER JOIN ratings r on t.tconst = r.tconst 
               WHERE r.averageRating > 8 
               ORDER BY r.averageRating DESC
               LIMIT 10
               
               "])
  
  (pqry db ["
               SELECT 
               t.primaryTitle,
               r.averageRating,
               r.numVotes
               FROM titles t
               INNER JOIN ratings r on t.tconst = r.tconst 
               WHERE r.averageRating > 8 
                     AND r.numVotes > 10000
               ORDER BY r.averageRating DESC
               LIMIT 10
               
               "])
  
  
  (pqry db ["
              SELECT 
            a.primaryTitle,
            a.averageRating
           -- a.numVotes
           --  a.tconst,
           -- g.name
            FROM (
               SELECT 
               t.primaryTitle,
               r.averageRating,
               r.numVotes,
               t.tconst
               FROM titles t
               INNER JOIN ratings r on t.tconst = r.tconst 
               WHERE r.averageRating > 7
                     AND t.startYear > 2010
                     AND r.numVotes > 5000
            ) as a
            INNER JOIN title_genres  g on a.tconst = g.tconst
             WHERE g.name ILIKE 'documentary'
               ORDER BY a.averageRating DESC
               LIMIT 50
               
               "])
  
  (pqry db ["
              SELECT count(distinct tconst) from titles;
               
               "])
  ; 954349
  
  (pqry db ["
              SELECT count(distinct tconst) from ratings 
            WHERE averageRating IS NOT NULL;
               
               "])
  ;954349
  
  (pqry db ["
              SELECT count(distinct tconst) from titles
           WHERE tconst in (SELECT distinct tconst FROM ratings) 
               
               "])
  ; 954348
  
  

  
  
  ;
  )
  
