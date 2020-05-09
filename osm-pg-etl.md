# Exploring and Processing OSM data in PostgrSQL

The first step is to create a new database for OpenStreetMap and load a pbf file of some dump

Then we need to explore the contents of the tags, focusing on highways. The best is to summarise, tags, values and counts in tables.

Finally, we need to create a graph table, i.e. a table where the first and last node of the link are in separate attributes.
We should include additional attributes necessary for analysis and visualisation.

An alternative is to create a netwrok table for GIS visualisation, with a lot more attributes, including all tags and geometry. Then extract graphs from that with a minimal set f attributes, i.e. node and edge ids, and a cost column.


### Notes:
A nice tutorial to get started: https://www.r-spatial.org/2017/07/14/large_scale_osm_in_r

Osmosis latest release: https://github.com/openstreetmap/osmosis/releases

For more basic info on PostgreSQL: https://www.codementor.io/@engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb
