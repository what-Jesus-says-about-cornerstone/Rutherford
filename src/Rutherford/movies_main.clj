(ns Rutherford.movies-main
  (:require [Rutherford.nrepl]
            [Rutherford.impl]
            [Rutherford.io]
            [Rutherford.dgraph]
            [Rutherford.server]
            [movies.imdb.schema]
            [Rutherford.movies-imdb-etl]
            [Rutherford.movies-imdb-query]
            [Rutherford.movies-imdb-psql]
            [movies.stack.psql]
   ;
            )
  ;
  )


(defn -dev  [& args]
  (Rutherford.nrepl/-main)
  (Rutherford.server/run-dev)
  )

(defn -main  [& args]
  (Rutherford.nrepl/-main)
  )

(comment
  (Rutherford.impl/try-parse-int "3")
  
  (Rutherford.impl/version)
  
  
  ;
  )