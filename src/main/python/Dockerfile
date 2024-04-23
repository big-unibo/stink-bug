# Use the official Python image as a base image
FROM python:3.9-slim

# Install build dependencies
RUN apt-get update && \
    apt-get install -y dvipng ghostscript texlive-fonts-recommended texlive-latex-base texlive-latex-extra texlive-latex-recommended texlive-publishers texlive-science texlive-xetex cm-super gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the Python files and requirements.txt into the container
COPY . .

RUN pip install --upgrade pip
# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Command to execute the Python script
CMD ["python", "main.py"]