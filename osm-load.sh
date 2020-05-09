#!/bin/bash

# just got recommended to chage priviledges on the pgpass file
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
psql osm_testing -U postgres
osm_testing=> CREATE INDEX idx_nodes_tags ON nodes USING GIN(tags);
osm_testing=> CREATE INDEX idx_ways_tags ON ways USING GIN(tags);
osm_testing=> CREATE INDEX idx_relations_tags ON relations USING GIN(tags);

# load the data
osmosis --read-pbf file="montenegro-latest.osm.pbf" --write-pgsql host="localhost" database="osm_testing" user="postgres" password="postgres"

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
