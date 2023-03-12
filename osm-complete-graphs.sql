-- Producing road network graphs from OSM data in PostgreSQL

-- create a graph table
CREATE SCHEMA IF NOT EXISTS graphs;

-- In this step we filter out what we don't want in the graph (if single mode)
-- In this example we get a complete network: attributes will be added for mode (Gil, 2015)
DROP TABLE IF EXISTS graphs.complete_network CASCADE;
CREATE TABLE graphs.complete_network AS
    SELECT nodes[1] AS start_node, nodes[array_upper(nodes, 1)] AS end_node, id AS edge_id, tags, nodes,
           ST_Length(ST_Transform(linestring, 3347)) AS length, (tags -> 'highway') AS highway,
           (get_ints_from_text((tags -> 'maxspeed')))[1] AS speed_limit, (tags -> 'service') AS service,
           (tags -> 'access') AS access,
           (tags -> 'oneway') AS oneway, linestring AS geom
    FROM ways WHERE (tags -> 'highway') IS NOT NULL;
;

-- analyse the speed values on highways
CREATE SCHEMA IF NOT EXISTS tags_summary;

-- Create a table to summarise irregular maxspeed values of highways
DROP TABLE IF EXISTS tags_summary.highway_maxspeed_values;
CREATE TABLE tags_summary.highway_maxspeed_values AS
    SELECT highway, speed_limit, count(*) AS count FROM
        (SELECT highway AS highway, speed_limit AS speed_limit
            FROM graphs.complete_network
        ) AS stat
    GROUP BY highway, speed_limit
    ORDER BY highway, count DESC
;

-- Create a table to summarise median maxspeed values of highways for replacing NULLs in car_network
DROP TABLE IF EXISTS tags_summary.highway_maxspeed_median;
CREATE TABLE tags_summary.highway_maxspeed_median AS
    SELECT highway, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY speed_limit) AS median
    FROM graphs.complete_network
    GROUP BY highway
;

-- Update car_network where there are NULLs with the median value of their highway category
UPDATE graphs.complete_network
    SET speed_limit = tags_summary.highway_maxspeed_median.median
    FROM tags_summary.highway_maxspeed_median
    WHERE speed_limit is NULL
    AND tags_summary.highway_maxspeed_median.highway = graphs.complete_network.highway
;

-- Create network nodes table for further routing with iGraph
DROP TABLE IF EXISTS graphs.complete_network_nodes;
CREATE TABLE graphs.complete_network_nodes AS
SELECT id AS node_id, ST_AsText(ST_Transform(geom, 4326)) AS node_coord
        FROM nodes WHERE id IN (SELECT DISTINCT start_node FROM graphs.complete_network UNION SELECT end_node FROM graphs.complete_network)
;



-- the complete network can be used for all modes, but needs to filtered using attributes per mode
-- add different mode attributes
ALTER TABLE graphs.complete_network ADD COLUMN car SMALLINT;
ALTER TABLE graphs.complete_network ADD COLUMN pedestrian SMALLINT;
ALTER TABLE graphs.complete_network ADD COLUMN bicycle SMALLINT;

UPDATE graphs.complete_network SET
    car = NULL, pedestrian = NULL, bicycle = NULL
;

-- update the mode attribute for not accessible segments
UPDATE graphs.complete_network SET
    car = 0, pedestrian = 0, bicycle = 0 WHERE
    access IN ('no','private','permit', 'customers', 'forestry', 'agricultural', 'military', 'emergency', 'bus', 'delivery', 'restricted')
;
UPDATE graphs.complete_network SET
    car = 0, pedestrian = 0, bicycle = 0 WHERE
    service IN ('drive-through', 'emergency_access', 'bus')
;
UPDATE graphs.complete_network SET
    car = 0, pedestrian = 0, bicycle = 0 WHERE
    highway IN ('disused', 'dismantled', 'razed', 'rest_area', 'corridor','construction','abandoned','raceway',
                'proposed','rest_area','planned','platform')
;

-- update the mode attribute for the car network
-- car restricted segments
UPDATE graphs.complete_network SET
    car = 0 WHERE
        (tags -> 'highway') IN ('steps', 'footway', 'pedestrian', 'cycleway', 'bridleway','elevator','sidewalk')
;
UPDATE graphs.complete_network SET
    car = 0 WHERE
        (tags -> 'bicycle') IN ('designated')
        OR (tags -> 'cycleway') IN ('designated')
        OR (tags -> 'foot') IN ('designated')
        OR (tags -> 'footway') IN ('sidewalk','crossing','yes')
        OR (tags -> 'motor_vehicle') IN ('no','private')
        OR (tags -> 'service') IN ('parking_aisle','parking')
        OR (tags -> 'bus') IN ('designated')
;
-- car dedicated segments
UPDATE graphs.complete_network SET
    car = 1 WHERE
        car IS NULL AND
        (tags -> 'highway') IN ('motorway','primary','tertiary','secondary','primary_link','tertiary_link','secondary_link',
                                    'trunk','trunk_link','motorway_link','motorway_junction')
;

-- update the mode attribute for the pedestrian network
-- pedestrian restricted segments
UPDATE graphs.complete_network SET
    pedestrian = 0 WHERE
        (tags -> 'highway') IN ('motorway','trunk','trunk_link','motorway_link','motorway_junction')
;
UPDATE graphs.complete_network SET
    pedestrian = 0 WHERE
        (tags -> 'bus') IN ('designated')
;
-- pedestrian dedicated segments
UPDATE graphs.complete_network SET
    pedestrian = 1 WHERE
        pedestrian IS NULL AND
        (tags -> 'highway') IN ('steps', 'footway', 'pedestrian', 'bridleway','elevator','sidewalk','living_street')
;
UPDATE graphs.complete_network SET
    pedestrian = 1 WHERE
        pedestrian IS NULL AND
        (tags -> 'foot') IN ('designated')
        OR (tags -> 'footway') IN ('sidewalk','crossing','yes')
;


-- update the mode attribute for the pedestrian network
-- pedestrian restricted segments
UPDATE graphs.complete_network SET
    bicycle = 0 WHERE
        (tags -> 'highway') IN ('motorway','trunk','trunk_link','motorway_link','motorway_junction')
;
UPDATE graphs.complete_network SET
    bicycle = 0 WHERE
        (tags -> 'bus') IN ('designated')
;
-- pedestrian dedicated segments
UPDATE graphs.complete_network SET
    bicycle = 1 WHERE
        bicycle IS NULL AND
        (tags -> 'highway') IN ('cycleway','living_street')
;
UPDATE graphs.complete_network SET
    bicycle = 1 WHERE
        bicycle IS NULL AND
        (tags -> 'bicycle') IN ('designated')
        OR (tags -> 'cycleway') IN ('designated')
;

