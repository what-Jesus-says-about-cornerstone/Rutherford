(ns Rutherford.repl
  (:require [Rutherford.main]))

(defn java-version
  []
  (System/getProperty "java.vm.version"))

(comment
  

  (System/getProperty "java.vm.version")
  (System/getProperty "java.version")
  (System/getProperty "java.specification.version")
  (clojure-version)
  
  ;
  )