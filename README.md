# BIG - Stink bug

[![build](https://github.com/big-unibo/experimental-project/actions/workflows/build.yml/badge.svg)](https://github.com/big-unibo/experimental-project/actions/workflows/build.yml)

## Project structure

    datasets/   -- where datasets are stored
    outputs/    -- where graphical results are stored
    src/        -- source code

## Published datasets
The datasets used in this project are correlated with metadata that follows the [Dublin Core standard](https://www.dublincore.org/).
Each dataset is correlated with a `.json` metadata file that is saved in the **dataset folder**.
Huge datasets (shapefiles) that are not inside this repository
are available at the [link](https://big.csr.unibo.it/downloads/stink-bug/datasets/shapefiles/).

In particular, the datasets are:
- `CASE` dataset: it contains the data about the data collected using the CASE application and are stored in the `datasets/CASE` folder.
- `Environment registry` dataset: it contains the data about the environment and are stored in the `datasets/Environment registry` folder.
- `Satellite images` dataset: it contains the satellite images
  and are stored in the [link](https://big.csr.unibo.it/downloads/stink-bug/datasets/shapefiles/satellite_images/),
  metadata are in the `datasets/satellite_images` folder.
- `Weather data` dataset: it contains the weather data and are stored in the [link](https://big.csr.unibo.it/downloads/stink-bug/datasets/shapefiles/weather/),
  metadata are in the `datasets/weather` folder.
- `CUBE` dataset: it contains the multidimensional cube generated from the previous datasets and are stored in the `datasets/CUBE` folder.
- `traps` dataset: it contains the shapefile generated from the `CUBE` dataset and are stored in the `datasets/shapefiles/traps` folder.

## Source code structure
The code is structured in three main parts:

1. **Conversion from harbor dataset to multidimensional cube.**
   This part takes as input the data located in the CASE, environment registry, satellite images,
   and weather datasets,
   to generate the multidimensional cube and save it in the `dataset/CUBE` folder.
   The code used is all in the folder `src/main/scala/it/unibo/big`,
   for running the code, you need to run the `src/main/scala/it/unibo/big/GenerateCUBE.scala` class.
2. **Generation of output graphs.**
   This part takes as input the data located in the `dataset/CUBE` folder,
   and generates the graphs that are saved in the `outputs` folder.
   The code used is all in the folder `src/main/python`, for run the code you need to:
    - build the docker container `docker build -t graph-container src/main/python`
    - run the docker container:
        - WINDOWS:
            - `docker run -v %cd%/src/main/python:/app -v %cd%/datasets/CUBE:/app/datasets/CUBE -v %cd%/outputs/graphs:/app/graphs graph-generator`
        - LINUX
            - `docker run -v $(pwd)/src/main/python:/app -v $(pwd)/datasets:/app/datasets -v $(pwd)/outputs/graphs:/app/graphs graph-generator`
3. **Generation of shape file dataset.**
   This part takes as input the data located in the `dataset/CUBE` folder, and generates the shape file dataset that is
   saved in the `dataset/shapefiles/traps` folder.
   The code used is in the folder `src/main/bash`, for running the code, you need to:

   - build the docker container `docker build -t ogr2ogr-container src/main/bash`
   - run the docker container:
       - WINDOWS:
           - `docker run -v %cd%/datasets/CUBE:/app/input -v %cd%/datasets/shapefiles:/app/output ogr2ogr-container`
       - LINUX
           - `docker run -v $(pwd)/datasets/CUBE:/app/input -v $(pwd)/datasets/shapefiles:/app/output ogr2ogr-container`

