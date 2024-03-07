# Use docker for generate graphs and save in output directory
- `docker build -t graph-container src/main/python`
- run:
  - WINDOWS:
    - `docker run -v %cd%/src/main/python:/app -v %cd%/datasets/CUBE:/app/datasets/CUBE -v %cd%/outputs/graphs:/app/graphs graph-generator`
  - LINUX
    - `docker run -v $(pwd)/src/main/python:/app -v $(pwd)/datasets:/app/datasets -v $(pwd)/outputs/graphs:/app/graphs graph-generator`