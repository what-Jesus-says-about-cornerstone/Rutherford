(ns Rutherford.impl
  (:require [clojure.repl :refer :all]
            [clojure.reflect :refer :all]
            [clojure.pprint :as pp]
            [clojure.java.javadoc :refer [javadoc]]
            [puget.printer :as pug]
            [clojure.string :as cstr]
            [clojure.xml :as xml]
            [clojure.zip :as zip]
            ;
            ))

(comment

  ; (Examples/hello)
  {:hello "world"}

  (System/getProperty "java.vm.version")
  (System/getProperty "java.version")
  (System/getProperty "java.specification.version")
  (clojure-version)


  ;
  )

(defn version 
  []
  (clojure-version)
  )


(defn try-parse-int 
  "returns number or nil"
  [number-string]
  (cond 
    (int? number-string) number-string
    :else (try (Integer/parseInt number-string)
               (catch Exception e nil))
  ))

(defn try-parse-float
  "returns number or nil"
  [number-string]
  (cond
    (float? number-string) number-string
    :else (try (Float/parseFloat number-string)
               (catch Exception e number-string))))

(defn replace-double-quotes
  [s & {:keys [ch]
        :or   {ch "'"}}]
  (clojure.string/replace s #"\"" ch))

(defn split-tab
  "Splits the string by tab char"
  [s]
  (cstr/split s #"\t"))

(defn filter-by-key
  [coll key]
  (filter #(-> % :key #{key}) coll))

(defn drop-nth
  "Remove nth element from coll"
  [n coll]
  (keep-indexed #(if (not= %1 n) %2) coll))

(comment


  (try-parse-int "3")

  ;
  )

(defn prn-members
  "Prints unique members of an instance using clojure.reflect"
  [inst]
  (->>
   (reflect inst)
   (:members)
   (sort-by :name)
   (map #(:name %))
   (set)
   (into [])
   (sort)
   pp/pprint
  ;  (pp/print-table )
  ;  (pp/print-table [:name :flags :parameter-types])
   ))


(defn javadoc-print-url
  "Opens a browser window displaying the javadoc for the argument.
  Tries *local-javadocs* first, then *remote-javadocs*."
  {:added "1.2"}
  [class-or-object]
  (let [^Class c (if (instance? Class class-or-object)
                   class-or-object
                   (class class-or-object))]
    (if-let [url (#'clojure.java.javadoc/javadoc-url (.getName c))]
    ;   (browse-url url)
      url
      (println "Could not find Javadoc for" c))))

(comment

  (source javadoc)
  (source clojure.java.javadoc/javadoc-url)


  (apropos "javadoc-url")

  (javadoc-print-url Runtime)
  (javadoc-print-url String)

;;;
  )


(defn partition-into-vecs
  "Returns vec of vecs "
  [part-size v]
  (->>
   (partition part-size v)
   (mapv vec)))


(defn cprn
  "color-prints a value "
  ([x]
   (pug/cprint x nil))
  ([x opts]
   (pug/cprint x opts)))

(defn sytem-prn
  "print using System.out.println"
  [msg]
  (.println (System/out) msg))

(comment

  (cprn [1 2 3 4 5])

  (sytem-prn "3")
  ;;;
  )

(defn rand-int-in-range
  "returns random int in range a b"
  [a b]
  (int (- b (* (rand) (- b a)))))

(defn nth-seq
  "Returns the nth element in a seq"
  [coll index]
  (first (drop index coll)))

;;convenience function, first seen at nakkaya.com later in clj.zip src
(defn zip-str [s]
  (zip/xml-zip
   (xml/parse (java.io.ByteArrayInputStream. (.getBytes s)))))