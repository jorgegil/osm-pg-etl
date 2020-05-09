-- Exploring and Processing OSM data in PostgrSQL



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
           ST_Length(linestring) AS length,  split_part((tags -> 'maxspeed'), ' ',1)::INTEGER AS speed_limit, linestring
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