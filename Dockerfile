# Use a base image
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV ASTEC_ROOT=/opt/astec_installer

# Install dependencies
RUN apt-get update && apt-get install -y \
    git \
    openssh-client \
    wget \
    python3 \
    python3-pip \
    git-lfs \
    && rm -rf /var/lib/apt/lists/*

ARG PAT_ASSAS

RUN git config --global url."https://${PAT_ASSAS}@github.com/".insteadOf "git@github.com:"

# Clone the main repository
WORKDIR /app
COPY . /app

# Initialize and update the submodule
RUN git submodule update --init --recursive

# Pull LFS files for the submodule
RUN git lfs install && git lfs pull

# Move the submodule to the desired location
RUN mkdir -p $ASTEC_ROOT && cp -r test/astec_installer/* $ASTEC_ROOT

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Set the working directory
#WORKDIR /app

# Copy the repository files into the container
#COPY . /app