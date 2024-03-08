# Run docker container with ogr2ogr command for export shape file (from __BSMB directory__)
- `docker build -t ogr2ogr-container src/main/bash`
- run:
  - WINDOWS:
    - `docker run -v %cd%/datasets/CUBE:/app/input -v %cd%/datasets/shapefiles:/app/output ogr2ogr-container`
  - LINUX
    - `docker run -v $(pwd)/datasets/CUBE:/app/input -v $(pwd)/datasets/shapefiles:/app/output ogr2ogr-container`
- The shape file is saved in datasets/shapefiles directory