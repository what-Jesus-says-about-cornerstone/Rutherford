(ns Rutherford.main
  (:require [Rutherford.nrepl]
            [Rutherford.impl]
            [Rutherford.io]
            [Rutherford.dgraph]
            [Rutherford.server]
            [Rutherford.datomic-mbrainz]
            [Rutherford.datomic-starcraft-etl]
            [Rutherford.datomic-seattle]
            [Rutherford.datomic-gitexp]
            [Rutherford.datomic-schema]
   
   ;
            )
  ;
  )


(defn -dev  [& args]
  (Rutherford.nrepl/-main)
  ; (Rutherford.server/run-dev)
  )

(defn -main  [& args]
  (Rutherford.nrepl/-main)
  )

(comment
  
  (Rutherford.impl/try-parse-int "3")
  
  (Rutherford.impl/version)
  
  
  ;
  )