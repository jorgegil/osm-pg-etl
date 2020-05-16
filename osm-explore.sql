-- Exploring and Processing OSM data in PostgrSQL

-- Define functions
CREATE OR REPLACE FUNCTION get_ints_from_text(TEXT) RETURNS int[] AS $$
  select array_remove(regexp_split_to_array($1,'[^0-9]+','i'),'')::int[];
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION _final_median(anyarray) RETURNS float8 AS $$
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)
  ) q2;
$$ LANGUAGE SQL IMMUTABLE;

CREATE AGGREGATE median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);

--DROP SCHEMA IF EXISTS tags_summary;
CREATE SCHEMA tags_summary;

-- filtering ways that represent roads
-- highway is the main tag for roads
--DROP TABLE IF EXISTS tags_summary.highway_values;
CREATE TABLE tags_summary.highway_values AS
    SELECT (tags -> 'highway') AS highway,  -- this gets the tag values into the attribute
           count(*) AS count
    FROM ways
    WHERE (tags ? 'highway') = True -- filter ways with tag 'highway', any value
    GROUP BY highway ORDER BY count DESC
;

-- Summarising tags in the ways
-- but ways come with other tags that also define what kind of way it is, what mode can use it, etc.
--DROP TABLE IF EXISTS tags_summary.highway_other_tags;
CREATE TABLE tags_summary.highway_other_tags AS
    SELECT tag, count(*) AS count FROM
        (SELECT (each(tags)).key AS tag
            FROM ways
            WHERE (tags ? 'highway') = True
        ) AS stat
        GROUP BY tag
        ORDER BY count DESC, tag
;
-- analyse highways of type 'service', and the tag service
--DROP TABLE IF EXISTS tags_summary.highway_service;
CREATE TABLE tags_summary.highway_service AS
    SELECT tag, count(*) AS count FROM
        (SELECT (tags -> 'service') AS tag
            FROM ways
            WHERE (tags -> 'highway') = 'service'
        ) AS stat
        GROUP BY tag
        ORDER BY count DESC, tag
;
-- analyse highways values of most other tags
--DROP TABLE IF EXISTS tags_summary.highway_tags_values;
CREATE TABLE tags_summary.highway_tags_values AS
    SELECT tag, value, count(*) AS count FROM
        (SELECT (each(tags)).key AS tag, (each(tags)).value AS value
            FROM ways
            WHERE (tags ? 'highway') = True
        ) AS stat
    WHERE tag NOT IN ('created_by','wikidata','width','wikipedia','note','old_ref','length','description')
    AND position('name' in tag) = 0
    AND position('source' in tag) = 0
    AND position('destination' in tag) = 0
    AND position('addr' in tag) = 0
    GROUP BY tag, value
    ORDER BY tag, count DESC
;


-- create a graph table
CREATE SCHEMA graphs;

-- In this step one has to filter out what we don't want in the graph (if single mode)
-- In this example we ger a simple road network
DROP TABLE IF EXISTS graphs.car_network CASCADE;
CREATE TABLE graphs.car_network AS
    SELECT nodes[1] AS start_node, nodes[array_upper(nodes, 1)] as end_node, id as edge_id, tags,
           ST_Length(linestring) AS length,  (get_ints_from_text((tags -> 'maxspeed')))[1] AS speed_limit, (tags -> 'highway') AS highway, linestring
    FROM ways
    WHERE
        -- what to include from the highway tags
        (tags -> 'highway') IN ('motorway','primary','tertiary','secondary','primary_link','tertiary_link','secondary_link',
                                    'trunk','residential','unclassified','living_street')
;
DELETE FROM graphs.car_network WHERE
        -- what to exclude from other tags
        (tags -> 'bicycle') IN ('designated')
        OR (tags -> 'foot') IN ('designated')
        OR (tags -> 'bus') IN ('designated')
        OR (tags -> 'footway') IN ('sidewalk','crossing')
        OR (tags -> 'motor_vehicle') IN ('no','private')
        OR (tags -> 'access') IN ('no','private')
        OR (tags -> 'service') IN ('parking_aisle','parking')
;
-- Create a table to summarise irregular maxspeed values of highways
DROP TABLE IF EXISTS graphs.highway_maxspeed_values;
CREATE TABLE graphs.highway_maxspeed_values AS
    SELECT highway, speed_limit, count(*) AS count FROM
        (SELECT highway AS highway, speed_limit AS speed_limit
            FROM graphs.car_network
        ) AS stat
    GROUP BY highway, speed_limit
    ORDER BY highway, count DESC
;

-- Create a table to summarise median maxspeed values of highways for replacing NULLs in car_network
DROP TABLE IF EXISTS graphs.highway_maxspeed_median;
CREATE TABLE graphs.highway_maxspeed_median AS
    SELECT highway, median(speed_limit) FROM graphs.car_network GROUP BY highway
;

-- Update car_network where there are NULLs with the median value of their highway category
UPDATE graphs.car_network
    SET speed_limit = graphs.highway_maxspeed_median.median
    FROM graphs.highway_maxspeed_median
    WHERE speed_limit is NULL
    AND graphs.highway_maxspeed_median.highway = graphs.car_network.highway
;

-- Create network nodes table for further routing with iGraph
DROP TABLE IF EXISTS graphs.car_network_nodes;
CREATE TABLE graphs.car_network_nodes AS
SELECT id AS node_id, ST_AsText(ST_Transform(geom, 4326)) AS node_coord
        FROM nodes WHERE id IN (SELECT DISTINCT start_node FROM graphs.car_network UNION SELECT end_node FROM graphs.car_network)
;

-- We can include more features and add boolean attributes for each mode to allow selecting different subgraphs
-- In this case it's clearer if we create a table and insert the elements of each mode.
CREATE TABLE graphs.car_network (
    id BIGINT,
    tags hstore,
    geom geometry (LineString,4326),
    start_node BIGINT,
    end_node BIGINT,
    length INTEGER,
    speed_limit INTEGER,
    car BOOLEAN,
    pedestrian BOOLEAN,
    cycling BOOLEAN
);