# Cimice.Net - Datasets and code

[![build](https://github.com/big-unibo/experimental-project/actions/workflows/build.yml/badge.svg)](https://github.com/big-unibo/experimental-project/actions/workflows/build.yml)

This repository exposes the data collected within the regional project PSR 2014-2020 Op. 16.1.01 - Go Pei-Agri Focus Area 4B Pr. "Cimice.Net" (during the field seasons 2020 and 2021) and its prosecution, supported by the Emilia-Romagna Producers Organizations with the coordination of Ri.Nova (during the field season 2022).

Further details about these data are available in the following paper:

> *Chiara Forresi, Enrico Gallinucci, Matteo Golfarelli, Lara Maistrello, Michele Preti, Giacomo Vaccari*. A Data Platform for Real-Time Monitoring and Analysis of the Brown Marmorated Stink Bug in Northern Italy. **Submitted for publication at Ecological Informatics**.

Compliance with [FAIR principles](https://www.go-fair.org/fair-principles/) is ensured by:
- XXX
- YYY

## Project structure

    datasets/   -- where datasets are stored
    outputs/    -- where graphical results are stored
    src/        -- source code

## Datasets

The datasets are described by metadata that follow the [Dublin Core standard](https://www.dublincore.org/).
Each data file is associated with a `.json` metadata file in the same folder.
Big files that do not fit within this repository
are available at the [link](https://big.csr.unibo.it/downloads/stink-bug/datasets/shapefiles/).

In particular, the datasets folder contains the following:

- **CASE**: the data collected using the CASE application.
- **Environment registry**: a mockup of the environment data (which are not currently publishable) from Consorzio CER (Canale Emiliano-Romagnolo), showing which information is accessed by the [code](#code).
- **Satellite images**: the rasters of satellite images collected from the ESA (European Space Agency); due to size limitations, this repository contains only the metadata, while the files are available at [this link](https://big.csr.unibo.it/downloads/stink-bug/datasets/rasters/satellite_images/).
- **Weather**: the weather data collected from ARPAE (Agency for Prevention, Environment, and Water); due to size limitations, this repository contains only the metadata, while the files are available at [this link](https://big.csr.unibo.it/downloads/stink-bug/datasets/shapefiles/weather/).
- **Cube**: the multidimensional cube generated from the previous datasets; the dimension of the traps is also available as a shapefile.


## Code

The code is structured in three main parts:

1. **Multidimensional cube generation.**
   This part takes as input the data located in the CASE, environment registry, satellite images,
   and weather datasets,
   to generate the multidimensional cube and save it in the `dataset/cube` folder.
   The code used is all in the folder `src/main/scala/it/unibo/big`,
   for running the code, you need to run the `src/main/scala/it/unibo/big/GenerateCUBE.scala` class.
   This code needs to be executed on a machine with Spark (or a Spark cluster),
   using the versions specified in the `build.gradle` file and with a Java 8 JDK that supports TSLv1.3. 
2. **Analytical processes.**
   This part takes as input the data located in the `dataset/cube` folder,
   and generates the graphs that are saved in the `outputs` folder.
   The code used is all in the folder `src/main/python`, for run the code you need to:
    - build the docker container `docker build -t graph-container src/main/python`
    - run the docker container:
        - WINDOWS:
            - `docker run -v %cd%/src/main/python:/app -v %cd%/datasets/cube:/app/datasets/cube -v %cd%/outputs/graphs:/app/graphs graph-generator`
        - LINUX
            - `docker run -v $(pwd)/src/main/python:/app -v $(pwd)/datasets:/app/datasets -v $(pwd)/outputs/graphs:/app/graphs graph-generator`
3. **Traps shapefile generation.**
   This part takes as input the data located in the `dataset/cube` folder, and generates the shapefile dataset that is
   saved in the `dataset/shapefiles/traps` folder.
   The code used is in the folder `src/main/bash`, for running the code, you need to:

    - build the docker container `docker build -t ogr2ogr-container src/main/bash`
    - run the docker container:
        - WINDOWS:
            - `docker run -v %cd%/datasets/cube:/app/input -v %cd%/datasets/shapefiles:/app/output ogr2ogr-container`
        - LINUX
            - `docker run -v $(pwd)/datasets/cube:/app/input -v $(pwd)/datasets/shapefiles:/app/output ogr2ogr-container`

