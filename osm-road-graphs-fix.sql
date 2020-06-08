-- Producing road network graphs from OSM data in PostgreSQL



-- There are different approaches to split the linestrings where nodes are shared by more than one way:
-- 1. Create a series of "blades" (multipoints) of shared nodes, then use a geometry split command
--  a) aggregate nodes from each way that has one or more shared nodes.
--  b) identify intersecting nodes and aggregate
-- 2. Reconstruct the linestrings for each way that has one or more of those nodes. There are several directions to reconstruct the nodes sequence:
--  a) find the shared nodes in each way
--  b) find the upper and lower limit of each segment, at shared nodes and end points
--  c) Group the nodes of ways_nodes in sequence, and stop/start a new linestring at limits
-- Size of the data sets can determine which is the best approach.
-- Node based reconstruction is topological and therefore more precise and robust.


-- prepare topology
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



-- Approach 1.
-- a) Aggregate split nodes per way id
DROP TABLE IF EXISTS topology_summary.nodes_blades;
CREATE TABLE topology_summary.nodes_blades AS
    SELECT a.way_id, ST_Multi(ST_Union(b.geom)) AS geom, array_agg(a.node_id) nodes
    FROM topology_summary.ways_split_nodes AS a, nodes AS b
    WHERE a.node_id = b.id
    GROUP BY a.way_id
;

-- b) Extract blade nodes based on geometry intersection
-- Does not use any existing topology info, can be used on other OSM extracts, e.g. overpass API
-- Does not work with large maps.
DROP TABLE IF EXISTS topology_summary.nodes_blades_alt CASCADE;
CREATE TABLE topology_summary.nodes_blades_alt AS
	SELECT ST_Multi(ST_Union(blade.geom)) AS geom, blade.way_id
	FROM (
         SELECT (ST_Dump(ST_Intersection(a.linestring, b.linestring))).geom AS geom, a.id way_id
         FROM (SELECT * FROM ways WHERE (tags ? 'highway') = True) AS a,
              (SELECT * FROM ways WHERE (tags ? 'highway') = True) AS b
         WHERE a.id != b.id
           AND (ST_Touches(a.linestring, b.linestring)
             OR ST_Crosses(a.linestring, b.linestring))
		 ) AS blade
	GROUP BY blade.way_id
;
DELETE FROM topology_summary.nodes_blades_alt WHERE ST_NumGeometries(geom)<=2;

-- a) and b) We can apply these "blades" to the previously defined road network
-- The downside is that we lose the topology and we need to create it with pgrouting
DROP TABLE IF EXISTS topology_summary.car_network_split CASCADE;
CREATE TABLE graphs.car_network_split AS
	SELECT (ST_Dump(ST_Split(road.geom, blade.geom))).geom AS geom, edge_id, tags, speed_limit, highway
	FROM graphs.car_network as road, topology_summary.nodes_blades as blade
	WHERE road.edge_id = blade.way_id
;
-- then append remaining roads that were not split
INSERT INTO topology_summary.car_network_split(geom, edge_id, tags, speed_limit, highway)
	SELECT geom, edge_id, tags, speed_limit, highway
	FROM graphs.car_network as road
	WHERE road.edge_id NOT IN (SELECT edge_id FROM graphs.car_network_split)
;
ALTER TABLE graphs.car_network_split ADD COLUMN sid serial NOT NULL PRIMARY KEY;
ALTER TABLE graphs.car_network_split ADD COLUMN length DOUBLE PRECISION;
UPDATE graphs.car_network_split SET length = ST_Length(geom::geography)/1000;

-- create topology for routing
-- creates roads_vertices_pgr table with nodes
DROP TABLE IF EXISTS topology_summary.car_network_split_vertices_pgr CASCADE;
ALTER TABLE graphs.car_network_split ADD COLUMN source INTEGER, ADD COLUMN target INTEGER;
UPDATE graphs.car_network_split SET source = NULL, target = NULL;
SELECT pgr_createTopology('graphs.car_network_split', 0.0001, 'geom', 'sid');
-- can be slow on big data and must be split on very large tables. Does not take advantage of existing topology information.


-- Approach 2.
-- Create a table with the ways nodes sequence, grouped accoprding to the shared nodes
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
   geom geometry(Linestring, 4326)
);
INSERT INTO graphs.car_network_merged (edge_id, nodes, geom)
	SELECT ways.way_id, array_agg(ways.node_id ORDER BY ways.sequence_id), ST_MakeLine( array_agg(nodes.geom ORDER BY ways.sequence_id))
	FROM (SELECT * FROM topology_summary.nodes_to_merge ORDER BY sequence_id) AS ways, nodes
	WHERE ways.node_id = nodes.id
    GROUP BY ways.way_id, ways.group_id
    --ORDER BY ways.sequence_id
;
UPDATE graphs.car_network_merged AS ways
    SET tags = road.tags,
        speed_limit  = road.speed_limit,
        highway = road.highway,
        start_node = ways.nodes[1],
        end_node = ways.nodes[array_upper(ways.nodes, 1)],
        length = ST_Length(ways.geom::geography)/1000
    FROM graphs.car_network AS road
    WHERE ways.edge_id = road.edge_id
;
-- then append remaining roads that were not merged
INSERT INTO graphs.car_network_merged(start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, geom)
	SELECT start_node, end_node, edge_id, tags, nodes, length, speed_limit, highway, geom
	FROM graphs.car_network as road
	WHERE NOT EXISTS (SELECT 1 FROM graphs.car_network_merged AS a WHERE road.edge_id = a.edge_id)
;

-- Create network nodes table for further routing with iGraph
DROP TABLE IF EXISTS graphs.car_network_merged_nodes;
CREATE TABLE graphs.car_network_merged_nodes AS
    SELECT id AS node_id, ST_AsText(ST_Transform(geom, 4326)) AS node_coord
    FROM nodes WHERE id IN (SELECT DISTINCT start_node FROM graphs.car_network UNION SELECT end_node FROM graphs.car_network)
;

