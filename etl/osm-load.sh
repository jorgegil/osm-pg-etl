#!/bin/bash

# just got recommended to change privileges on the pgpass file
chmod 0600 .pgpass

# create a database
psql postgres -U postgres

postgres=> CREATE DATABASE osm_testing;
postgres=> \q

# prepare the postgis extensions
psql osm_testing -U postgres

osm_testing=> CREATE EXTENSION postgis;
osm_testing=> CREATE EXTENSION hstore;
osm_testing=> CREATE EXTENSION pgrouting;
osm_testing=> \q

# prepare the OSM schemas
psql -U postgres -d osm_testing -f pgsnapshot_schema_0.6.sql
psql -U postgres -d osm_testing -f pgsnapshot_schema_0.6_linestring.sql
pgsnapshot_schema_0.6_action.sql
pgsnapshot_schema_0.6_bbox.sql
psql osm_testing -U postgres
osm_testing=> CREATE INDEX idx_nodes_tags ON nodes USING GIN(tags);
osm_testing=> CREATE INDEX idx_ways_tags ON ways USING GIN(tags);
osm_testing=> CREATE INDEX idx_relations_tags ON relations USING GIN(tags);

# load the data
# this is the classic approach works only for small files and uses a tmp folder that cna easily get full, and the operation stops
bin/osmosis --read-pbf file="montenegro-latest.osm.pbf" --write-pgsql host="localhost" database="osm_testing" user="postgres" password='postgres'

# https://help.openstreetmap.org/questions/14085/osmosis-planetosm-to-postgresql-disk-space-caching
export JAVACMD_OPTIONS="-Xmx5G -Djava.io.tmpdir=/database/files/tmp/"
JAVACMD_OPTIONS="-Djava.io.tmpdir=/home/database/files/tmp" bin/osmosis --read-pbf file="../files/osm/sweden-latest.osm.pbf" --write-pgsql  database="se_osm" user="urbaninfo" password='Urb4n!nf0'

# this worked but has more steps and can give erors
# https://www.paulnorman.ca/blog/2011/11/loading-a-pgsnapshot-schema-with-a-planet-take-2/
bin/osmosis --read-pbf-fast file="montenegro-latest.osm.pbf" workers=5 --buffer --write-pgsql-dump directory="../files/tmp/" nodeLocationStoreType=InMemory
psql -d osm_testing -U postgres -f pgsnapshot_load_0.6.sql

###
May 08, 2020 12:29:59 AM org.openstreetmap.osmosis.core.Osmosis run
INFO: Osmosis Version 0.45
May 08, 2020 12:30:00 AM org.openstreetmap.osmosis.core.Osmosis run
INFO: Preparing pipeline.
May 08, 2020 12:30:00 AM org.openstreetmap.osmosis.core.Osmosis run
INFO: Launching pipeline execution.
May 08, 2020 12:30:00 AM org.openstreetmap.osmosis.core.Osmosis run
INFO: Pipeline executing, waiting for completion.
May 08, 2020 12:32:39 AM org.openstreetmap.osmosis.core.Osmosis run
INFO: Pipeline complete.
May 08, 2020 12:32:39 AM org.openstreetmap.osmosis.core.Osmosis run
INFO: Total execution time: 160171 milliseconds.
###
