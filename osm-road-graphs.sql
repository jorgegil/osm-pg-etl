-- Producing road network graphs from OSM data in PostgreSQL

-- create a graph table
CREATE SCHEMA graphs;

-- In this step we filter out what we don't want in the graph (if single mode)
-- In this example we get a simple road network
DROP TABLE IF EXISTS graphs.car_network CASCADE;
CREATE TABLE graphs.car_network AS
    SELECT nodes[1] AS start_node, nodes[array_upper(nodes, 1)] AS end_node, id AS edge_id, tags, nodes,
           ST_Length(ST_Transform(linestring, 3347)) AS length,  (get_ints_from_text((tags -> 'maxspeed')))[1] AS speed_limit,
           (tags -> 'highway') AS highway, (tags -> 'oneway') AS oneway, linestring AS geom
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

-- analyse the speed values on highways
-- Create a table to summarise irregular maxspeed values of highways
DROP TABLE IF EXISTS tags_summary.highway_maxspeed_values;
CREATE TABLE tags_summary.highway_maxspeed_values AS
    SELECT highway, speed_limit, count(*) AS count FROM
        (SELECT highway AS highway, speed_limit AS speed_limit
            FROM graphs.car_network
        ) AS stat
    GROUP BY highway, speed_limit
    ORDER BY highway, count DESC
;

-- Create a table to summarise median maxspeed values of highways for replacing NULLs in car_network
DROP TABLE IF EXISTS tags_summary.highway_maxspeed_median;
CREATE TABLE tags_summary.highway_maxspeed_median AS
    SELECT highway, median(speed_limit) FROM graphs.car_network GROUP BY highway
;

-- Update car_network where there are NULLs with the median value of their highway category
UPDATE graphs.car_network
    SET speed_limit = tags_summary.highway_maxspeed_median.median
    FROM tags_summary.highway_maxspeed_median
    WHERE speed_limit is NULL
    AND tags_summary.highway_maxspeed_median.highway = graphs.car_network.highway
;

-- Create network nodes table for further routing with iGraph
DROP TABLE IF EXISTS graphs.car_network_nodes;
CREATE TABLE graphs.car_network_nodes AS
SELECT id AS node_id, ST_AsText(ST_Transform(geom, 4326)) AS node_coord
        FROM nodes WHERE id IN (SELECT DISTINCT start_node FROM graphs.car_network UNION SELECT end_node FROM graphs.car_network)
;


-- However, in OSM the links often do not split at intersections. We have to split the linestrings where nodes are shared by more than one way
-- First identify the shared nodes between different roads
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
-- Identify ways sequence maximum and node
DROP TABLE IF EXISTS topology_summary.ways_length;
CREATE TABLE topology_summary.ways_length AS
    SELECT DISTINCT ON (way_id) way_id, sequence_id AS length, node_id
    FROM way_nodes
    WHERE way_id IN (SELECT edge_id FROM graphs.car_network)
    AND sequence_id > 1
    ORDER BY way_id, sequence_id DESC
;
-- Identify ways with shared nodes to split - not at the start nor end
DROP TABLE IF EXISTS topology_summary.ways_split_nodes;
CREATE TABLE topology_summary.ways_split_nodes AS
    SELECT way_id, node_id, sequence_id
    FROM way_nodes AS n
    WHERE way_id IN (SELECT edge_id FROM graphs.car_network)
    AND sequence_id > 0
    AND node_id IN (SELECT node_id FROM topology_summary.ways_shared_nodes)
    AND EXISTS (SELECT 1 FROM topology_summary.ways_length AS w WHERE n.way_id = w.way_id AND n.sequence_id < w.length)
;
-- Identify ways nodes merge limits: used in approach 2
DROP TABLE IF EXISTS topology_summary.ways_merge_limits;
CREATE TABLE topology_summary.ways_merge_limits AS
    SELECT way_id, lag(sequence_id, 1, 0) OVER (PARTITION BY way_id ORDER BY sequence_id) AS bottom_limit, sequence_id AS top_limit
    FROM way_nodes AS n
    WHERE sequence_id > 0
    AND way_id IN (SELECT way_id FROM topology_summary.ways_split_nodes)
    AND node_id IN (SELECT node_id FROM topology_summary.ways_shared_nodes) -- any of the intermediary nodes
;
-- add the last node if it's not a shared node (dead ends)
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

-- Create a table with the ways nodes sequence, grouped according to the shared nodes
-- so that each way can be turned into multiple linestrings
DROP TABLE IF EXISTS topology_summary.nodes_to_merge;
CREATE TABLE topology_summary.nodes_to_merge AS
    SELECT a.way_id, b.top_limit AS group_id ,a.sequence_id, a.node_id
    FROM (SELECT * FROM way_nodes WHERE EXISTS (SELECT 1 FROM graphs.car_network as road WHERE way_id = road.edge_id)) AS a,
         topology_summary.ways_merge_limits AS b
    WHERE a.way_id = b.way_id
    AND a.sequence_id >= b.bottom_limit
    AND a.sequence_id <= b.top_limit
;
-- merge ways into new linestrings, getting attributes from original roads
DROP TABLE IF EXISTS graphs.car_network_merged CASCADE;
CREATE TABLE graphs.car_network_merged(
   sid SERIAL NOT NULL PRIMARY KEY,
   start_node BIGINT,
   end_node BIGINT,
   edge_id BIGINT,
   tags hstore,
   nodes BIGINT[],
   length DOUBLE PRECISION,
   speed_limit INTEGER,
   highway TEXT,
   oneway TEXT,
   geom geometry(Linestring, 4326)
);
INSERT INTO graphs.car_network_merged (edge_id, nodes, geom)
	SELECT ways.way_id, array_agg(ways.node_id ORDER BY ways.sequence_id), ST_MakeLine( array_agg(nodes.geom ORDER BY ways.sequence_id))
	FROM (SELECT * FROM topology_summary.nodes_to_merge ORDER BY sequence_id) AS ways, nodes
	WHERE ways.node_id = nodes.id
    GROUP BY ways.way_id, ways.group_id
;
UPDATE graphs.car_network_merged AS ways
    SET tags = road.tags,
        speed_limit  = road.speed_limit,
        highway = road.highway,
        oneway = road.oneway,
        start_node = ways.nodes[1],
        end_node = ways.nodes[array_upper(ways.nodes, 1)],
        length = ST_Length(ways.geom::geography)/1000
    FROM graphs.car_network AS road
    WHERE ways.edge_id = road.edge_id
;
-- then append remaining roads that were not merged
INSERT INTO graphs.car_network_merged(start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom)
	SELECT start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom
	FROM graphs.car_network as road
	WHERE NOT EXISTS (SELECT 1 FROM graphs.car_network_merged AS a WHERE road.edge_id = a.edge_id)
;

-- Create network nodes table for further routing with iGraph
DROP TABLE IF EXISTS graphs.car_network_merged_nodes;
CREATE TABLE graphs.car_network_merged_nodes AS
    SELECT id AS node_id, ST_AsText(ST_Transform(geom, 4326)) AS node_coord
    FROM nodes WHERE id IN (SELECT DISTINCT start_node FROM graphs.car_network UNION SELECT end_node FROM graphs.car_network)
;


-- testing the weighted median functions
SELECT _weighted_median('graphs.car_network_merged','length','speed_limit');
SELECT * FROM public.weighted_median_by_group('graphs.car_network_merged','length','speed_limit','highway');


-- The car_network_merged table has a list of start and end nodes, which works like an edge list from which to create an undirected graph.
-- To create a directed graph we need to identify the direction restriction on some routes, and correct or extend the edge list for those cases
-- The nodes table remain unchanged, it's the same for directed and undirected graphs
DROP TABLE IF EXISTS graphs.car_network_directed CASCADE;
CREATE TABLE graphs.car_network_directed(
   sid SERIAL NOT NULL PRIMARY KEY,
   start_node BIGINT,
   end_node BIGINT,
   edge_id BIGINT,
   tags hstore,
   nodes BIGINT[],
   length DOUBLE PRECISION,
   speed_limit INTEGER,
   highway TEXT,
   oneway TEXT,
   geom geometry(Linestring, 4326)
);
-- first insert all links that are two ways
INSERT INTO graphs.car_network_directed(start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom)
	SELECT start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom
	FROM graphs.car_network_merged as road
    WHERE (oneway IS NULL OR oneway = 'no') AND highway != 'motorway'
;
-- add the reverse direction to the links
INSERT INTO graphs.car_network_directed(start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom)
	SELECT end_node, start_node, edge_id, tags, array_reverse(nodes), length, speed_limit, highway, oneway, geom
	FROM graphs.car_network_merged as road
    WHERE (oneway IS NULL OR oneway = 'no') AND highway != 'motorway'
;
-- insert the directed links (not reversed)
INSERT INTO graphs.car_network_directed(start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom)
	SELECT start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom
	FROM graphs.car_network_merged as road
    WHERE oneway = 'yes' OR (highway = 'motorway' AND oneway != '-1')
;
-- insert and revert the directed links that are drawn in reverse
INSERT INTO graphs.car_network_directed(start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, oneway, geom)
	SELECT end_node, start_node, edge_id, tags, array_reverse(nodes), length, speed_limit, highway, oneway, geom
	FROM graphs.car_network_merged as road
    WHERE oneway = '-1'
;

-- alternatively we only create a graph with the essential attributes
DROP TABLE IF EXISTS graphs.car_graph_directed CASCADE;
CREATE TABLE graphs.car_graph_directed(
   sid SERIAL NOT NULL PRIMARY KEY,
   start_node BIGINT,
   end_node BIGINT,
   length DOUBLE PRECISION,
   speed_limit INTEGER
);
-- first insert all links that are two ways
INSERT INTO graphs.car_graph_directed(start_node, end_node, length, speed_limit)
	SELECT start_node, end_node, length, speed_limit
	FROM graphs.car_network_merged as road
    WHERE (oneway IS NULL OR oneway = 'no') AND highway != 'motorway'
;
-- add the reverse direction to the links
INSERT INTO graphs.car_graph_directed(start_node, end_node, length, speed_limit)
	SELECT end_node, start_node, length, speed_limit
	FROM graphs.car_network_merged as road
    WHERE (oneway IS NULL OR oneway = 'no') AND highway != 'motorway'
;
-- insert the directed links (not reversed)
INSERT INTO graphs.car_graph_directed(start_node, end_node, length, speed_limit)
	SELECT start_node, end_node, length, speed_limit
	FROM graphs.car_network_merged as road
    WHERE oneway = 'yes' OR (highway = 'motorway' AND oneway != '-1')
;
-- insert and revert the directed links that are drawn in reverse
INSERT INTO graphs.car_graph_directed(start_node, end_node, length, speed_limit)
	SELECT end_node, start_node, length, speed_limit
	FROM graphs.car_network_merged as road
    WHERE oneway = '-1'
;

