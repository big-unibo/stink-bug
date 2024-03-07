#!/bin/bash

# Define input and output paths
INPUT_FILE="/app/input/CUBE_dim_trap.csv"
OUTPUT_FILE="/app/output/traps.shp"

# Run the ogr2ogr command with escaped parentheses in the SQL query
ogr2ogr -s_srs EPSG:4326 -t_srs EPSG:3857 -oo X_POSSIBLE_NAMES=Lon* -oo Y_POSSIBLE_NAMES=Lat* \
-f "ESRI Shapefile" "$OUTPUT_FILE" "$INPUT_FILE" -sql 'SELECT gid,monitoring_started as start_mon,monitoring_ended as end_mon,longitude,latitude,district,name,ms_id,"svp (manual)" as svp_man,"svp (auto)" as svp_auto,validity,area from CUBE_dim_trap'