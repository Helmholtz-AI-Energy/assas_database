FROM python:3.11-slim

# Install dependencies
RUN apt-get update && apt-get install -y git && apt-get clean

# Install Python dependencies
#COPY requirements.txt /app/requirements.txt
#RUN pip install --no-cache-dir -r /app/requirements.txt

# Install Ruff and pytest explicitly
RUN pip install --no-cache-dir ruff pytest

# Set the working directory
WORKDIR /app

# Copy the repository files into the container
COPY . /app