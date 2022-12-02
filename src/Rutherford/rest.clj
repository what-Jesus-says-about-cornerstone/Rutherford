(ns Rutherford.rest-main
  (:require [Rutherford.nrepl]
            [Rutherford.impl]
            [Rutherford.io]
            [Rutherford.dgraph]
            [Rutherford.rest-server]
   ;
            )
  ;
  )


(defn -dev  [& args]
  (Rutherford.nrepl/-main)
  (Rutherford.rest-server/run-dev)
  )

(defn -main  [& args]
  (Rutherford.nrepl/-main)
  )

(comment
  
  (Rutherford.impl/try-parse-int "3")
  
  (Rutherford.impl/version)
  
  
  ;
  )