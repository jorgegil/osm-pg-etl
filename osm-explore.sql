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
