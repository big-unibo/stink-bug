#!/bin/bash
docker build -t ogr2ogr-container src/main/bash
docker run -v $(pwd)/datasets/DFM:/app/input -v $(pwd)/outputs/shapefile:/app/output ogr2ogr-container