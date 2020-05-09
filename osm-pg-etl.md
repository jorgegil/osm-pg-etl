# Exploring and Processing OSM data in PostgrSQL

## Creating a database
The first step is to create a new database for OpenStreetMap and load a pbf file of some dump

## Exploring the OSM tags
Then we need to explore the contents of the tags, focusing on highways. The best is to summarise, tags, values and counts in tables.

## Creating a graph table
Finally, we need to create a graph table, i.e. a table where the first and last node of the link are in separate attributes.
We should include additional attributes necessary for analysis and visualisation.

An alternative is to create a netwrok table for GIS visualisation, with a lot more attributes, including all tags and geometry. 
Then extract graphs from that with a minimal set f attributes, i.e. node and edge ids, and a cost column.

## Converting to Graph object
Final step in python is to convert the data table with the edges list to a igraph Graph object.
Assuming we get the table from postgresql into a data frame, we iterate through the data frame extracting tuples, into a list or directly to the Graph method.

We can’t use the default igraph parameters for it to work, but instead: index=False, name=None. 
The index because we already have a lot of indices in the data and igraph creates its own indices. We also don’t want the names to get regular tuples.
https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html

The second step is run the iterator through the TupleList method of the Graph class of igraph.
https://igraph.org/python/doc/igraph-pysrc.html#Graph.TupleList

Igraph requires vertices indexed sequentially and starting with 0, and those are created automatically. They won't match existing vertex or edge ids. 
So, the ids in the database are stored as vertex and edge attributes in the graph for later use. 
These names are very important to join the analysis results back with the features (nodes and edges) of the original network in the database.
This is a short code example:

```python
import igraph as ig

# network is a tuples list, with start node id, end node id, edge id, and cost
network = [(1,2,34,10.4),(1,5,35,0.05),(2,8,36,4.2),(3,5,37,9.05),(8,5,38,5.3),
(6,8,39,1.9)]

G = ig.Graph.TupleList(network,directed=False, vertex_name_attr='nodeid',edge_attrs=['edgeid','cost'])

# looking at the results to see the difference between index and nodeid label...
G.vertex_attributes()
G.vs['nodeid']
G.vs[0].index
G.vs[0]['nodeid']

# G is not weighted
G.edge_attributes()
G.is_weighted()
G.es[0].index
G.es[0]["edgeid"]
G.es[0]["cost"]

# other parameters can be explored to produce different types of graphs
```


### Notes:
A nice tutorial to get started: https://www.r-spatial.org/2017/07/14/large_scale_osm_in_r

Osmosis latest release: https://github.com/openstreetmap/osmosis/releases

For more basic info on PostgreSQL: https://www.codementor.io/@engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb
