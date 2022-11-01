# posting-lines
Takes line segments stored on a postgis database and calculates the geometry, length and speed over ground (knots- based on seconds duration column). 

Uses psycopg2 module to access postgresql/postgis server via python.

Currently 'things to note':
- Output is in epsg 3005 which is local for the West Coast British Columbia, Canada. Alter accordingly for local area.
Create table statement in the script is specifically for the temp database (geom is in epsg 4326 to properly accomodate lat/longs). 
Create the real db with with epsg 3005.

- Duration column assumes seconds to calculate into knots (and length in meters- which is the default for epsg 3005).
