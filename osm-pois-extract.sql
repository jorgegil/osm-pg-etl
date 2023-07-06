-- get pois from OSM nodes
-- must get and insert them per distinct key because there are main keys and type keys, and the main keys are unique
DROP TABLE IF EXISTS graphs.pois_nodes CASCADE;
CREATE TABLE graphs.pois_nodes AS SELECT
    id AS node_id, geom, 'leisure' AS key, (tags -> 'leisure') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'leisure') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'sport' AS key, (tags -> 'sport') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'sport') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'shop' AS key, (tags -> 'shop') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'shop') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'office' AS key, (tags -> 'office') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'office') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'amenity' AS key, (tags -> 'amenity') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'amenity') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'craft' AS key, (tags -> 'craft') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'craft') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'tourism' AS key, (tags -> 'tourism') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'tourism') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'emergency' AS key, (tags -> 'emergency') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'emergency') IS NOT NULL
;
INSERT INTO graphs.pois_nodes SELECT
    id AS node_id, geom, 'historic' AS key, (tags -> 'historic') AS values, (tags -> 'name') AS name, tags
    FROM nodes
    WHERE (tags -> 'historic') IS NOT NULL
;

-- get pois from OSM ways
DROP TABLE if eXISTS graphs.pois_ways CASCADE;
CREATE TABLE graphs.pois_ways AS SELECT
    id AS way_id, linestring AS geom, 'leisure' AS key, (tags -> 'leisure') AS values, (tags -> 'name') AS name, tags
    FROM ways
    WHERE (tags -> 'leisure') IS NOT NULL AND (tags -> 'highway') IS NULL
;
INSERT INTO graphs.pois_ways SELECT
    id AS way_id, linestring AS geom, 'landuse' AS key, (tags -> 'landuse') AS values, (tags -> 'name') AS name, tags
    FROM ways
    WHERE (tags -> 'landuse') IS NOT NULL AND (tags -> 'highway') IS NULL
;
INSERT INTO graphs.pois_ways SELECT
    id AS way_id, linestring AS geom, 'sport' AS key, (tags -> 'sport') AS values, (tags -> 'name') AS name, tags
    FROM ways
    WHERE (tags -> 'sport') IS NOT NULL AND (tags -> 'highway') IS NULL
;
INSERT INTO graphs.pois_ways SELECT
    id AS way_id, linestring AS geom, 'amenity' AS key, (tags -> 'amenity') AS values, (tags -> 'name') AS name, tags
    FROM ways
    WHERE (tags -> 'amenity') IS NOT NULL AND (tags -> 'highway') IS NULL
;