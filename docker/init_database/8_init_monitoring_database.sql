-----------------------------------
----------- Monitoring ------------
-----------------------------------

/*
Table for the tile geometry.
Link the tiles to their geometry.
This table is automatically populated using the geojson geometry files and the ‘feed_tile_geometry.py’ script.
*/
CREATE TABLE hrwsi.tile_geometry (

    tile TEXT PRIMARY KEY,
    geom GEOMETRY(MultiPolygonZ, 4326)
);