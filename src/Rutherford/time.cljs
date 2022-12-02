(ns Rutherford.time
  (:require [cljs-time.core :as t]))


(defn hi
  []
  "hi!!")

(defn ts
  []
  (t/epoch))

(comment

  (ts)
  
  (.log js/console (ts))

  ;
  )