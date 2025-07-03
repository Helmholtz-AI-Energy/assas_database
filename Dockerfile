# Use a base image
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV ASTEC_ROOT=/opt/astec_installer

# Install dependencies and Python 3.11
RUN apt-get update && apt-get install -y \
    software-properties-common \
    git \
    openssh-client \
    wget \
    git-lfs \
    && add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && apt-get install -y \
    python3.11 \
    python3.11-venv \
    python3.11-distutils \
    && rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.11
RUN wget https://bootstrap.pypa.io/get-pip.py && \
    python3.11 get-pip.py && \
    rm get-pip.py

# Add SSH key from build argument
ARG SSH_PRIVATE_KEY
RUN mkdir -p /root/.ssh && \
    echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa && \
    ssh-keyscan -t rsa github.com >> /root/.ssh/known_hosts

# Clone the main repository
#WORKDIR /app
#COPY . /app
#RUN echo "Listing files in /app:" && ls -l /app/

# Initialize and update the submodule
RUN git submodule update --init --recursive

# Pull LFS files for the submodule
RUN git lfs install && git lfs pull

# Copy the ASTEC installer into the container
COPY ./test/astec_installer/astecV3.1.2_linux64.tgz /tmp/

# Move the submodule to the desired location
#RUN mkdir -p $ASTEC_ROOT && cp -r -v ./test/astec_installer/* $ASTEC_ROOT && ls -l $ASTEC_ROOT

# Copy the ASTEC installer into the container
#COPY ./test/astec_installer/astecV3.1.2_linux64.tgz /tmp/

# Unzip and install ASTEC
#RUN tar -xzf /tmp/astecV3.1.2_linux64.tgz -C $ASTEC_ROOT && \
#    rm /tmp/astecV3.1.2_linux64.tgz && \
#    chmod +x $ASTEC_ROOT/install.sh && \
#    $ASTEC_ROOT/install.sh

# Install Python dependencies
#COPY requirements.txt .
#RUN pip3 install --no-cache-dir -r requirements.txt

# Set the working directory
#WORKDIR /app

# Copy the repository files into the container
#COPY . /app