# Exploring and Processing OSM data in PostgrSQL

## Creating a database
The first step is to create a new database for OpenStreetMap, which needs PostGIS and the OSM schema loaded. 
Then wwe load a pbf file of some country or region obtained from a complete OSM dump, for example from Geofabrik, using osmosis. 
This is preferable to obtaining a simplified file, such as shape file or overpass osm file. 
These formats do not follow the original topological structure and discard the topology information of nodes and ways.
The topology is necessary for a complete and correct reconstruction of navigable networks.

## Exploring the OSM tags
Then we need to explore the contents of the tags, focusing on highways. The best is to summarise, tags, values and counts in tables.
The main tag is highway, with many different values. But other tags are also present in highway features, and influence and complement these,
for example service, foot, access, bicycle, sidewalk, cycleway, footway, steps, bridge, to name the most common ones.
All these must be looked into to undertand what they are representing, and to what extend they are on not covered by values
in the highway tag. For example, the *highway* tag value *cycleway* can correspond in all cases to a tag *cycleway*, or maybe not.
We mustn't forget that a common *highway* tag value is unclassified. 

## Creating a graph table

TODO: undirected and directed graph (see https://wiki.openstreetmap.org/wiki/Key:oneway)

Finally, we need to create a graph table, i.e. a table where the first and last node of the link are in separate attributes.
We should include additional attributes necessary for analysis and GIS visualisation, including all tags and geometry.

1. Select the relevant links for the transport mode being represented, using selections of tags to add and to exclude;
2. Break the geometry of links that continue over intersections. Here the topological approach is preferred;
3. Calculate the length of the new links, and update the start/end nodes;
4. Add any other attributes to the new links;
5. Complete the graph adding the links that did not have to be split.

For analysis in igraph we can extract from the database graphs with a minimal set of attributes, 
i.e. node and edge ids, and cost column (s) only.

## Converting to Graph object

TODO: undirected and directed graphs

Final step in python is to convert the data table with the edges list to a igraph Graph object.
Assuming we get the table from postgresql into a data frame, we iterate through the data frame extracting tuples, 
into a list or directly to the Graph method.

We can’t use the default igraph parameters for it to work, but instead: index=False, name=None. 
The index because we already have a lot of indices in the data and igraph creates its own indices. 
We also don’t want the names to get regular tuples.
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

## Importing results back into the database
To efficiently output analysis results from python to postgresql one needs to iterate over the graph edges and/or vertices to extract all relevant attributes.
The best is to use the VertexSeq and EdgeSeq classes, or vs and es properties of the Graph object, 
which are iterators to get to all the values from the graph (original id and analysis results). 
References for the different cases:
https://igraph.org/python/doc/igraph.VertexSeq-class.html

https://igraph.org/python/doc/igraph.EdgeSeq-class.html

https://stackoverflow.com/questions/30986498/getting-vertex-list-from-python-igraph/53619896

In the loop one can feed the values to a pandas data frame for further processing in python, or send them straight to the database.
The normal method from pandas to sql using sqlalchemy is not particularly fast.
Also, using psycopg2 is not fast because we loop one record at a time and do multiple inserts.
Even if we don’t commit after each operation it is slow.
The situation is even worse if we do update statements.

**Fast Solution**
The fastest method is by “piping” CSV text without saving to disk, directly into a table in the database using copy and some clever psycopg2 tricks.
This blog post does an amazing job of explaining the different alternatives and comparing the performance in terms of memory and speed.

https://hakibenita.com/fast-load-data-python-postgresql

The end result is truly impressive!
‘Copy Data From a String Iterator’ is the ultimate solution.
And here is the code: https://gist.github.com/hakib/7e723d2c113b947f7920bf55737e4d16

The COPY and INSERT commands require a pre-created table using CREATE.
The example provided above uses hard coded column names, and that is SQL best practice.
To use COPY or INSERT without column names you have to make sure that all columns in the data frame match exactly all columns in the table in the database.
This is the same approach as above, without column names, specific to pandas data frames since it uses the to_csv trick to great effect: 
(See second response - "Faster option” by Aseem)

https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
 
## Alternative approach to obtain routable network for pgrouting
The OSM data can be pre-processed with osm2po to get the correct topology for the road network:
http://osm2po.de/

Here a basic example of usage:
https://anitagraser.com/2011/12/15/an-osm2po-quickstart/

It's important to edit the config file to produce a sql output:
https://gis.stackexchange.com/questions/175428/how-to-make-osm2po-5-1-write-an-sql-file

```shell script
######################################################################
#
# POSTPROCESSORS
#
######################################################################
# uncomment and edit these lines
postp.0.class = de.cm.osm2po.plugins.postp.PgRoutingWriter

postp.pipeOut = false
```

Possibly also edit the config file to tweak the handling of different tags, in the 'Way Tag Resolver' section of the config file.
The default configuration only extracts a very simple road network of the main roads.
I don't have specific experience with that, needs to be figured out.

Once the configuration is tweaked Command for processing the osm data from overpass:
java -Xmx6g -jar osm2po-core-5.2.43-signed.jar prefix=vic tileSize=x vic-overpass-api-map.osm

Command for processing the osm download from geofabrik:
java -Xmx6g -jar osm2po-core-5.2.43-signed.jar prefix=es tileSize=x spain-latest.osm.pbf


### Notes:
A nice tutorial to get started: https://www.r-spatial.org/2017/07/14/large_scale_osm_in_r

Osmosis latest release: https://github.com/openstreetmap/osmosis/releases

For more basic info on PostgreSQL: https://www.codementor.io/@engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb