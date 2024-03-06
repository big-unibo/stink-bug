FROM ubuntu:latest

# Install required packages
RUN apt-get update && apt-get install -y \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the shell script into the container
COPY generate_shape_file.sh .

# Make the script executable
RUN chmod +x generate_shape_file.sh

# Entrypoint for the container
ENTRYPOINT ["./generate_shape_file.sh"]
