(ns Rutherford.movies-imdb-schema
  (:require [clojure.repl :refer :all]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [Rutherford.dgraph :refer [q create-client 
                                     set-schema 
                                     drop-all 
                                     count-total-nodes]])
  ;
  )


(comment

  (def c (create-client {:with-auth-header? false
                         :hostname          "server"
                         :port              9080}))


  (drop-all c)

  (count-total-nodes c)


; schema
  (->
   (q {:qstring "schema(pred: [imdb.genre.name, imdb.title.genres imdb.name.primaryName]) {
  type
  index
}"
       :client  c
       :vars    {}})

   (pp/pprint))

  (def schema-string "
              <imdb.title.averageRating>: float @index(float) .
              <imdb.title.numVotes>: int @index(int) .
              
              <imdb.akas.title>: uid .
              <imdb.akas.ordering>: int .
              <imdb.akas.titleString>: string @index(exact,fulltext) @count .
              <imdb.akas.region>: string .
              <imdb.akas.language>: string .
              <imdb.akas.attributes>: [string] .
              <imdb.akas.isOriginalTitle>: bool .
               
              <imdb.title.titleType>: string .
              <imdb.title.primaryTitle>: string @index(fulltext) @count .
              <imdb.title.originalTitle>: string .
              <imdb.title.isAdult>: bool .
              <imdb.title.startYear>: int .
              <imdb.title.endYear>: int .
              <imdb.title.runtimeMinutes>: int .
              <imdb.title.genres>: uid @reverse .
              
              <imdb.title.directors>: uid @reverse .
              <imdb.title.writers>: uid @reverse .
               
              <imdb.episode.parentTconst>: uid .
              <imdb.episode.seasonNumber>: int .
              <imdb.episode.episodeNumber>: int .
               
              <imdb.principals.title>: uid . 
              <imdb.principals.name>: uid .
              <imdb.principals.ordering>: int .
              <imdb.principals.category>: string .
              <imdb.principals.job>: string .
              <imdb.principals.characters>: string .
               
              <imdb.name.primaryName>: string @index(fulltext) @count .
              <imdb.name.birthYear>: int .
              <imdb.name.deathYear>: int .
              <imdb.name.primaryProfession>: [string] @index(term) .
              <imdb.name.knownForTitles>: uid @reverse .
               
              <imdb.genre.name>: string @index(fulltext,term) @count .
               
              ")

  (spit "/opt/.data/imdb.rdf/imdb.schema" schema-string)

  (set-schema {:schema-string schema-string
               :client        c})

  (def schema-string2 "
              <imdb.title.averageRating>: float .
              <imdb.title.numVotes>: int  .
              
              <imdb.akas.title>: uid .
              <imdb.akas.ordering>: int .
              <imdb.akas.titleString>: string  .
              <imdb.akas.region>: string .
              <imdb.akas.language>: string .
              <imdb.akas.attributes>: [string] .
              <imdb.akas.isOriginalTitle>: bool .
               
              <imdb.title.titleType>: string .
              <imdb.title.primaryTitle>: string .
              <imdb.title.originalTitle>: string .
              <imdb.title.isAdult>: bool .
              <imdb.title.startYear>: int .
              <imdb.title.endYear>: int .
              <imdb.title.runtimeMinutes>: int .
              <imdb.title.genres>: uid  .
              
              <imdb.title.directors>: uid  .
              <imdb.title.writers>: uid  .
               
              <imdb.episode.parentTconst>: uid .
              <imdb.episode.seasonNumber>: int .
              <imdb.episode.episodeNumber>: int .
               
              <imdb.principals.title>: uid . 
              <imdb.principals.name>: uid .
              <imdb.principals.ordering>: int .
              <imdb.principals.category>: string .
              <imdb.principals.job>: string .
              <imdb.principals.characters>: string .
               
              <imdb.name.primaryName>: string .
              <imdb.name.birthYear>: int .
              <imdb.name.deathYear>: int .
              <imdb.name.primaryProfession>: [string] .
              <imdb.name.knownForTitles>: uid .
               
              <imdb.genre.name>: string .
               
              ")

  (set-schema {:schema-string schema-string2
               :client        c})


  ;
  )