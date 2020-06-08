-- Exploring and Processing OSM data in PostgreSQL

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


-- Explore tags in railway (public transport)
DROP TABLE IF EXISTS tags_summary.railway_ways_values CASCADE;
CREATE TABLE tags_summary.railway_ways_values AS
    SELECT (tags -> 'railway') AS railway,  -- this gets the tag values into the attribute
           count(*) AS count
    FROM ways
    WHERE (tags ? 'railway') = True -- filter ways with tag 'highway', any value
    GROUP BY railway ORDER BY count DESC
;

DROP TABLE IF EXISTS tags_summary.railway_nodes_values CASCADE;
CREATE TABLE tags_summary.railway_nodes_values AS
    SELECT (tags -> 'railway') AS railway,  -- this gets the tag values into the attribute
           count(*) AS count
    FROM nodes
    WHERE (tags ? 'railway') = True -- filter ways with tag 'highway', any value
    GROUP BY railway ORDER BY count DESC
;


-- Summarising ways topology
CREATE SCHEMA topology_summary;

-- Identify way nodes that are shared by more than one linestring of highways
DROP TABLE IF EXISTS topology_summary.ways_shared_nodes;
CREATE TABLE topology_summary.ways_shared_nodes AS
    SELECT a.node_id, a.count
    FROM (
        SELECT node_id, count(*) count
        FROM way_nodes
        WHERE way_id IN (SELECT id FROM ways WHERE (tags ? 'highway') = True)
        GROUP BY node_id
        ) AS a
    WHERE a.count > 1
;
-- even better, using the final road network
DROP TABLE IF EXISTS topology_summary.ways_shared_nodes;
CREATE TABLE topology_summary.ways_shared_nodes AS
    SELECT a.node_id, a.count
    FROM (
        SELECT node_id, count(*) count
        FROM way_nodes
        WHERE way_id IN (SELECT edge_id FROM graphs.car_network)
        GROUP BY node_id
        ) AS a
    WHERE a.count > 1
;

-- Identify ways sequence maximum: total number of nodes
DROP TABLE IF EXISTS topology_summary.ways_length;
CREATE TABLE topology_summary.ways_length AS
    SELECT DISTINCT ON (way_id) way_id, sequence_id AS length, node_id
    FROM way_nodes
    WHERE way_id IN (SELECT id FROM ways WHERE (tags ? 'highway') = True)
    AND sequence_id > 1
    ORDER BY way_id, sequence_id DESC
;

-- Identify ways with split nodes - not at the start nor end
DROP TABLE IF EXISTS topology_summary.ways_split_nodes;
CREATE TABLE topology_summary.ways_split_nodes AS
    SELECT way_id, node_id, sequence_id
    FROM way_nodes AS n
    WHERE way_id IN (SELECT id FROM ways WHERE (tags ? 'highway') = True)
    AND sequence_id > 0
    AND node_id IN (SELECT node_id FROM topology_summary.ways_shared_nodes)
    AND EXISTS (SELECT 1 FROM topology_summary.ways_length AS w WHERE n.way_id = w.way_id AND n.sequence_id < w.length)
;

-- Identify ways nodes merge limits: used in approach 2
DROP TABLE IF EXISTS topology_summary.ways_merge_limits;
CREATE TABLE topology_summary.ways_merge_limits AS
    SELECT way_id, lag(sequence_id, 1, 0) OVER (PARTITION BY way_id ORDER BY sequence_id) AS bottom_limit, sequence_id AS top_limit
    FROM way_nodes AS n
    WHERE way_id IN (SELECT id FROM ways WHERE (tags ? 'highway') = True)
    AND sequence_id > 0
    AND way_id IN (SELECT way_id FROM topology_summary.ways_split_nodes)
    AND node_id IN (SELECT node_id FROM topology_summary.ways_shared_nodes) -- any of the intermediary nodes
;
-- or the last node if it's not a shared node (dead ends)
INSERT INTO topology_summary.ways_merge_limits (way_id, bottom_limit, top_limit)
    SELECT limits.way_id, limits.top_limit, length.length
    FROM (SELECT DISTINCT ON (way_id) way_id, top_limit
        FROM topology_summary.ways_merge_limits
        ORDER BY way_id, top_limit DESC
        ) AS limits,
         topology_summary.ways_length AS length
    WHERE limits.way_id = length.way_id
    AND limits.top_limit < length.length
;
