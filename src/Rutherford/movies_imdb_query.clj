(ns Rutherford.movies-imdb-query
  (:require [clojure.repl :refer :all]
            [clojure.pprint :as pp]
            [Rutherford.dgraph :refer [q qry create-client set-schema]])
  ;
  )




(comment

  (def c (create-client {:with-auth-header? false
                         :hostname          "server"
                         :port              9080}))


  (->
   (q {:qstring "{
  caro(func: allofterms(name@en, \"Marc Caro\")) {
    name@en
    director.film {
      name@en
    }
  }
  jeunet(func: allofterms(name@en, \"Jean-Pierre Jeunet\")) {
    name@en
    director.film {
      name@en
    }
  }
}"
       :client  c
       :vars    {}})
   (pp/pprint))

  (->
   (q {:qstring "{
  all(func: has(imdb.title.numVotes)) {
    count(uid)
        }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))


  (as-> nil X
    (identity "{
  all(func: has(imdb.title.primaryTitle)) {
    count(uid)
        }
  }")
    (q {:qstring X
        :client  c
        :vars    {}})
    (pp/pprint X))

  (as-> nil X
    (identity "
            query  q($primaryTitle: string) {
             title(func: anyoftext(imdb.title.primaryTitle,$primaryTitle)) {
                 imdb.title.primaryTitle
               }
             }
             ")
    (q {:qstring X
        :client  c
        :vars    {"primaryTitle" "The Matrix"}})
    (pp/pprint X))


  (->
   (q {:qstring "{
    all(func: has(imdb.name.primaryName)) {
    count(uid)
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))

  (->
   (q {:qstring "{
    all(func: has(imdb.title.primaryTitle)) {
    count(uid)
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))

  (->
   (q {:qstring "{
    all(func: has(imdb.name.primaryName), first: 5)  {
      imdb.name.primaryName
      imdb.name.birthYear
      imdb.name.deathYear
      imdb.name.knownForTitles {
       imdb.title.primaryTitle
       imdb.title.genres {
        imdb.genre.name
      }	
       }
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))

  (->
   (q {:qstring "{
    all(func: anyoftext(imdb.genre.name, \"sci-fi\")) {
    imdb.genre.name
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))

; genres list
  (->
   (q {:qstring "{
    all(func: has(imdb.genre.name)) {
    imdb.genre.name
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))

  ; high rating documentaries
  (->
   (q {:qstring "{
    
       
       ID as var(func: has(imdb.title.primaryTitle)) {
       imdb.title.primaryTitle
       rating as imdb.title.averageRating
       imdb.title.genres {
          genre as imdb.genre.name
       } 
      }
                 
    all(func: uid (ID), first: 10) @filter(gt(val(rating), 7) AND eq(val(genre), \"Documentary\" ) ) {
     imdb.title.primaryTitle
     imdb.title.averageRating
       imdb.title.genres {
       imdb.genre.name
       } 
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))

  (->
   (q {:qstring "{
    
    all(func: has(imdb.title.primaryTitle), first: 10) @cascade    {
     imdb.title.primaryTitle
     imdb.title.averageRating
     imdb.title.genres @filter(anyofterms(imdb.genre.name,\"Drama\")) {
        genre: imdb.genre.name
       } 
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))


  (->
   (q {:qstring "{
    
    all(func: has(imdb.title.primaryTitle)
     ,offset: 10
     ,first: 10
     #,orderasc: imdb.title.averageRating
     ,orderdesc: imdb.title.averageRating
     ) 
     @cascade
       @filter(gt(imdb.title.averageRating, 7))
     {
     imdb.title.primaryTitle
     imdb.title.averageRating
     imdb.title.genres {
        genre: imdb.genre.name
       } 
    }
  }"
       :client  c
       :vars    {}})
   (pp/pprint))


  (qry "{
    
    all(func: has(imdb.title.writers), first: 10) @cascade    {
     imdb.title.primaryTitle
     imdb.title.averageRating
     
    }
  }" c)
  
  (qry "{
    
    all(func: has(imdb.name.primaryName), first: 10) @cascade {
     imdb.name.primaryName
     ~imdb.title.writers (first: 2) {
       imdb.title.primaryTitle
       }
    }
  }" c)
  
  (qry "{

      all(func: has(imdb.title.primaryTitle)) @filter(gt(imdb.title.averageRating,7)) {
       count(uid)
      } 
   
  }" c)
  
  (qry "{

      documentaries as var(func: has(imdb.title.primaryTitle)) @filter(ge(imdb.title.averageRating,8.3)) @cascade {
        name: imdb.title.primaryTitle
        genres: imdb.title.genres @filter(allofterms(imdb.genre.name,\"Documentary\")) {
          genre: imdb.genre.name
        }
      }
      all(func: uid(documentaries)) {
       count(uid)
       } 
  }" c)
  
   (qry "{

      documentaries as var(func: has(imdb.title.primaryTitle)) 
        @filter(gt(imdb.title.averageRating,8.3) AND anyoftext(imdb.title.primaryTitle, \"cow\"))
        @cascade {
        name: imdb.title.primaryTitle
        genres: imdb.title.genres @filter(allofterms(imdb.genre.name,\"Documentary\")) {
          genre: imdb.genre.name
        }
      }
      all(func: uid(documentaries), offset: 5, first: 10) {
       title: imdb.title.primaryTitle
       genres: imdb.title.genres {
        genre: imdb.genre.name
        }
       } 
  }" c)
   

;
  )