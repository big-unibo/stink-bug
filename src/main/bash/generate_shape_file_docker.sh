#!/bin/bash
docker build -t ogr2ogr-container src/main/bash
docker run -v $(pwd)/datasets/CUBE:/app/input -v $(pwd)/outputs/shapefile:/app/output ogr2ogr-container