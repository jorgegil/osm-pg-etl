-- Exploring and Processing OSM data in PostgreSQL


--DROP SCHEMA IF EXISTS tags_summary;
CREATE SCHEMA tags_summary;

-- filtering ways that represent roads
-- highway is the main tag for roads
DROP TABLE IF EXISTS tags_summary.highway_values;
CREATE TABLE tags_summary.highway_values AS
    SELECT (tags -> 'highway') AS highway,  -- this gets the tag values into the attribute
           count(*) AS count
    FROM ways
    WHERE (tags ? 'highway') = True -- filter ways with tag 'highway', any value
    GROUP BY highway ORDER BY count DESC
;

-- Summarising tags in the ways
-- but ways come with other tags that also define what kind of way it is, what mode can use it, etc.
DROP TABLE IF EXISTS tags_summary.highway_other_tags;
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
DROP TABLE IF EXISTS tags_summary.highway_service;
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
DROP TABLE IF EXISTS tags_summary.highway_tags_values;
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


-- Explore the road direction attribute
-- highway = motorway is always directed.
--DROP TABLE IF EXISTS tags_summary.highway_oneway_values;
CREATE TABLE tags_summary.highway_oneway_values AS
    SELECT highway, oneway, count(*) AS count FROM
        (SELECT (tags -> 'highway') AS highway, (tags -> 'oneway') AS oneway
            FROM ways
            WHERE (tags ? 'oneway') = True
            AND (tags ? 'highway') = True
        ) AS stat
    GROUP BY highway,oneway
    ORDER BY highway,oneway, count DESC
;


-- Explore pois tags in nodes
DROP TABLE IF EXISTS tags_summary.amenity_nodes_values;
CREATE TABLE tags_summary.amenity_nodes_values AS
    SELECT amenity, count(*) AS count FROM
        (SELECT (tags -> 'amenity') AS amenity
            FROM nodes
            WHERE (tags ? 'amenity') = TRUE
        ) AS stat
    GROUP BY amenity
    ORDER BY amenity, count DESC
;
DROP TABLE IF EXISTS tags_summary.shop_nodes_values;
CREATE TABLE tags_summary.shop_nodes_values AS
    SELECT shop, count(*) AS count FROM
        (SELECT (tags -> 'shop') AS shop
            FROM nodes
            WHERE (tags ? 'shop') = TRUE
        ) AS stat
    GROUP BY shop
    ORDER BY shop, count DESC
;
DROP TABLE IF EXISTS tags_summary.leisure_nodes_values;
CREATE TABLE tags_summary.leisure_nodes_values AS
    SELECT leisure, count(*) AS count FROM
        (SELECT (tags -> 'leisure') AS leisure
            FROM nodes
            WHERE (tags ? 'leisure') = TRUE
        ) AS stat
    GROUP BY leisure
    ORDER BY leisure, count DESC
;
DROP TABLE IF EXISTS tags_summary.craft_nodes_values;
CREATE TABLE tags_summary.craft_nodes_values AS
    SELECT craft, count(*) AS count FROM
        (SELECT (tags -> 'craft') AS craft
            FROM nodes
            WHERE (tags ? 'craft') = TRUE
        ) AS stat
    GROUP BY craft
    ORDER BY craft, count DESC
;
DROP TABLE IF EXISTS tags_summary.sport_nodes_values;
CREATE TABLE tags_summary.sport_nodes_values AS
    SELECT sport, count(*) AS count FROM
        (SELECT (tags -> 'sport') AS sport
            FROM nodes
            WHERE (tags ? 'sport') = TRUE
        ) AS stat
    GROUP BY sport
    ORDER BY sport, count DESC
;
DROP TABLE IF EXISTS tags_summary.tourism_nodes_values;
CREATE TABLE tags_summary.tourism_nodes_values AS
    SELECT tourism, count(*) AS count FROM
        (SELECT (tags -> 'tourism') AS tourism
            FROM nodes
            WHERE (tags ? 'tourism') = TRUE
        ) AS stat
    GROUP BY tourism
    ORDER BY tourism, count DESC
;
DROP TABLE IF EXISTS tags_summary.office_nodes_values;
CREATE TABLE tags_summary.office_nodes_values AS
    SELECT office, count(*) AS count FROM
        (SELECT (tags -> 'office') AS office
            FROM nodes
            WHERE (tags ? 'office') = TRUE
        ) AS stat
    GROUP BY office
    ORDER BY office, count DESC
;


-- Explore pois tags in ways
DROP TABLE IF EXISTS tags_summary.amenity_ways_values;
CREATE TABLE tags_summary.amenity_ways_values AS
    SELECT amenity, count(*) AS count FROM
        (SELECT (tags -> 'amenity') AS amenity
            FROM ways
            WHERE (tags ? 'amenity') = TRUE
        ) AS stat
    GROUP BY amenity
    ORDER BY amenity, count DESC
;
DROP TABLE IF EXISTS tags_summary.sport_ways_values;
CREATE TABLE tags_summary.sport_ways_values AS
    SELECT sport, count(*) AS count FROM
        (SELECT (tags -> 'sport') AS sport
            FROM ways
            WHERE (tags ? 'sport') = TRUE
        ) AS stat
    GROUP BY sport
    ORDER BY sport, count DESC
;
DROP TABLE IF EXISTS tags_summary.leisure_ways_values;
CREATE TABLE tags_summary.leisure_ways_values AS
    SELECT leisure, count(*) AS count FROM
        (SELECT (tags -> 'leisure') AS leisure
            FROM ways
            WHERE (tags ? 'leisure') = TRUE
        ) AS stat
    GROUP BY leisure
    ORDER BY leisure, count DESC
;
DROP TABLE IF EXISTS tags_summary.landuse_ways_values;
CREATE TABLE tags_summary.landuse_ways_values AS
    SELECT landuse, count(*) AS count FROM
        (SELECT (tags -> 'landuse') AS landuse
            FROM ways
            WHERE (tags ? 'landuse') = TRUE
        ) AS stat
    GROUP BY landuse
    ORDER BY landuse, count DESC
;

-- Explore pois tags in relations
DROP TABLE IF EXISTS tags_summary.amenity_relations_values;
CREATE TABLE tags_summary.amenity_relations_values AS
    SELECT amenity, count(*) AS count FROM
        (SELECT (tags -> 'amenity') AS amenity
            FROM relations
            WHERE (tags ? 'amenity') = TRUE
        ) AS stat
    GROUP BY amenity
    ORDER BY amenity, count DESC
;
DROP TABLE IF EXISTS tags_summary.leisure_relations_values;
CREATE TABLE tags_summary.leisure_relations_values AS
    SELECT leisure, count(*) AS count FROM
        (SELECT (tags -> 'leisure') AS leisure
            FROM relations
            WHERE (tags ? 'leisure') = TRUE
        ) AS stat
    GROUP BY leisure
    ORDER BY leisure, count DESC
;
DROP TABLE IF EXISTS tags_summary.sport_relations_values;
CREATE TABLE tags_summary.sport_relations_values AS
    SELECT sport, count(*) AS count FROM
        (SELECT (tags -> 'sport') AS sport
            FROM relations
            WHERE (tags ? 'sport') = TRUE
        ) AS stat
    GROUP BY sport
    ORDER BY sport, count DESC
;
DROP TABLE IF EXISTS tags_summary.landuse_relations_values;
CREATE TABLE tags_summary.landuse_relations_values AS
    SELECT landuse, count(*) AS count FROM
        (SELECT (tags -> 'landuse') AS landuse
            FROM relations
            WHERE (tags ? 'landuse') = TRUE
        ) AS stat
    GROUP BY landuse
    ORDER BY landuse, count DESC
;


