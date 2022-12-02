#!/bin/bash

fetch_movies(){
    wget "https://github.com/dgraph-io/tutorial/blob/master/resources/1million.rdf.gz?raw=true" -O 1million.rdf.gz -q 
    # -q quiet, no output
}

load_movies(){
    dgraph live -r 1million.rdf.gz --zero zero:5080 -c 1
}

alter_schema(){
   curl localhost:8080/alter -XPOST -d '
        director.film: uid @reverse .
        genre: uid @reverse .
        initial_release_date: dateTime @index(year) .
        name: string @index(term) @lang .
    '  | python -m json.tool | less
}


# PGPASSWORD=postgres

import_sql(){
    gunzip -c ./.data/aligulac.sql.gz | \
    PGPASSWORD=postgres psql -h postgres -p 5432 -d postgres -U postgres 
    #  psql -h postgresdb -p 5432 -d aligulac -c  "CREATE ROLE aligulac"
}

qry(){
     PGPASSWORD=postgres psql -h postgres -p 5432 -d postgresdb -U postgres \
     -c 'SELECT * FROM titles'
}

import(){
     PGPASSWORD=postgres psql -h postgres -p 5432 -d postgresdb -U postgres \
     -c   "COPY titles FROM  
          '/opt/app/.data/imdb/title.basics.tsv'
           DELIMITER E'\t' 
          NULL '\N'  QUOTE E'\b' ESCAPE E'\b' CSV HEADER "

     #\b	U+0008	backspace
}

load_seattle(){
  1
  # uri = "datomic:mem://seattle";
  # Peer.createDatabase(uri);
  # conn = Peer.connect(uri);

  # schema_file = "samples/seattle/seattle-schema.edn";
  # schema_rdr = new FileReader(schema_file);
  # schema_tx = Util.readAll(schema_rdr).get(0);
  # conn.transact(schema_tx).get();
  # schema_rdr.close();

  # data_file = "samples/seattle/seattle-data0.edn";
  # data_rdr = new FileReader(data_file);
  # data_tx = Util.readAll(data_rdr).get(0);
  # conn.transact(data_tx).get();
  # data_rdr.close();
}

load_mbrainz(){
  PREFIX=./.probedata/datomic/
  mkdir -p $PREFIX
  wget -P $PREFIX -O $PREFIX/mbrainz.tar https://s3.amazonaws.com/mbrainz/datomic-mbrainz-1968-1973-backup-2017-07-20.tar 
  tar -xvf $PREFIX/mbrainz.tar -C $PREFIX
}


import_mbrainz(){
    # bin/datomic restore-db file:///opt/datomic-pro/mbrainz-1968-1973 datomic:dev://datomicdb:4334/mbrainz-1968-1973
    bash ./c dc exec datomicfreedb bash -c " \
    bin/datomic restore-db \
    file:///opt/probe/.probedata/datomic/mbrainz-1968-1973 \
    datomic:free://datomicfreedb:4334/mbrainz "
    # clojure -m initdev
}


# https://stackoverflow.com/questions/3430810/multiple-simultaneous-downloads-using-wget
# https://datasets.imdbws.com/

load_imdb(){
  PREFIX=./.probedata/movies/imdb.gz
  HOST=https://datasets.imdbws.com

  wget -P $PREFIX $HOST/name.basics.tsv.gz &
  wget -P $PREFIX $HOST/title.akas.tsv.gz &
  wget -P $PREFIX $HOST/title.basics.tsv.gz &
  wget -P $PREFIX $HOST/title.crew.tsv.gz &
  wget -P $PREFIX $HOST/title.episode.tsv.gz &
  wget -P $PREFIX $HOST/title.principals.tsv.gz &
  wget -P $PREFIX $HOST/title.ratings.tsv.gz &
  # wget -r -np -N -P ./.probedata/movies $HOST
}

ungz_imdb(){
  PREFIX=./.probedata/movies/imdb.gz
  TARGET=./.probedata/movies/imdb
  # find $PREFIX -type f -name "*.gz" -print0 | xargs -0 -I{} tar xf {} -C $PREFIX
  # find $PREFIX -type f -name "*.gz" -exec tar xf {} -C $PREFIX/a \;
  gunzip -r -k $PREFIX
  mkdir -p $TARGET
  mv $PREFIX/*.tsv $TARGET
}

gz_imdb(){
  PREFIX=./.probedata/movies/imdb.rdf
  TARGET=./.probedata/movies/imdb.rdf.gz
  gzip -k -r  $PREFIX
  mkdir -p $TARGET
  mv $PREFIX/*.gz $TARGET
}

zip_all_imdb(){
  DIR=$(pwd)
  PREFIX=./.probedata/movies/imdb.rdf
  TARGET=./.probedata/movies/imdb.rdf.gz
  OUT=all.tar.gz
  mkdir -p $TARGET
  # gzip -k $PREFIX/*.rdf 
  cd $PREFIX && \
      tar -czvf $OUT *.rdf
  cd $DIR
  ls -a
  mv $PREFIX/$OUT $TARGET
}


import_imdb_dgraph(){
    # dc exec server bash -c "cd /opt/app; dgraph live -r .probedata/movies/imdb.rdf.gz/title.ratings.rdf.gz --zero zero:5080 -c 1"
    # dc exec server bash -c "cd /opt/app; dgraph live -r .probedata/movies/imdb.rdf.gz/title.ratings.rdf.gz \
    # -r .probedata/movies/imdb.rdf.gz/name.basics.rdf.gz \
    #  --zero zero:5080 -c 1"

    # dc exec server bash -c "cd /opt/app; dgraph live -r .probedata/movies/imdb.rdf/name.basics.rdf --zero zero:5080 -c 1"
    
    bash bin/dgraph dc exec server bash -c "cd /opt/app; dgraph live -b 2000 -c 20 \
     -r .probedata/movies/imdb.rdf/all.rdf \
     --zero zero:5080 -c 1"

    # dc exec server bash -c "cd /opt/app; dgraph live -b 1000 -c 10 \
    #  -r .probedata/movies/imdb.rdf.gz/all.tar.gz --zero zero:5080 -c 1"

}

import_imdb_test_dgraph(){
      bash bin/dgraph dc exec server bash -c "cd /opt/app; dgraph live -b 2000 -c 20 \
     -r ./bin/test.rdf \
     --zero zero:5080 -c 1"
}

import_imdb_dgraph_bulk(){
  RDFS=".probedata/movies/imdb.rdf/sample.rdf"
  SCHEMA=".probedata/movies/imdb.rdf/imdb.schema"
   bash bin/dgraph dc exec server bash -c "cd /opt/app; \
      dgraph bulk -s $SCHEMA \
     -r $RDFS \
     --map_shards=4 --reduce_shards=2 \
     --num_go_routines=6 \
     --http 0.0.0.0:8000 \
     --zero zero:5080"
}

load_ml(){
  PREFIX=./.probedata/movies/lens
  wget -P $PREFIX http://files.grouplens.org/datasets/movielens/ml-latest.zip
  unzip  $PREFIX/ml-latest.zip -d $PREFIX
}

install7zip(){
  sudo apt-get install -y p7zip-full
}

load_stack(){
  PREFIX=./.data/stack
  # BASE=https://archive.org/download/stackexchange
  # wget -P $PREFIX  $BASE/softwareengineering.stackexchange.com.7z
  7z x -o$PREFIX $PREFIX/softwareengineering.stackexchange.com.7z
}

load_aligulac(){
  PREFIX=./.probedata/starcraft/
  mkdir -p $PREFIX
  wget -P $PREFIX http://static.aligulac.com/aligulac.sql.gz 
}

import_aligulac(){
  #  gunzip -c /opt/probe/.probedata/starcraft/aligulac.sql.gz | \
  #   PGPASSWORD=postgres psql -h postgresdb -p 5432 -d aligulac -U aligulac 
   bash ./c dc exec postgres-aligulac bash -c " \
     gunzip -c /opt/probe/.probedata/starcraft/aligulac.sql.gz | \
    PGPASSWORD=postgres psql -d postgresdb -U aligulac 
   " 
}


"$@"
