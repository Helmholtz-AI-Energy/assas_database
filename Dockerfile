# Use a base image
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV ASTEC_ROOT=/opt/astec_installer
ENV REPO_PATH=/app

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

# Clone the repository and initialize submodules
#RUN git clone --recurse-submodules git@github.com:ke4920/assas_database.git $REPO_PATH
RUN git clone git@github.com:ke4920/assas_database.git /app
WORKDIR /app

RUN mkdir -p ${ASTEC_ROOT}
RUN apt-get update && apt-get install -y sshpass
RUN sshpass -p "R.adio_!1234" scp -r -o StrictHostKeyChecking=no ke4920@os-login.lsdf.kit.edu:/lsdf/kit/scc/projects/ASSAS/backup_mongodb ${ASTEC_ROOT}
RUN ls -l /opt/astec_installer/backup_mongodb

RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip install ruff

# Install Python dependencies
#WORKDIR $REPO_PATH
#RUN git checkout origin/feature_improve_netcdf4_conversion
#RUN git submodule update --init --recursive
#RUN git lfs install
#RUN git lfs pull
#RUN ls -l test
#RUN ls -l test/astec_installer
#RUN ls -l /app
#RUN ls -l /app/test
#RUN ls -l /app/test/astec_installer

#COPY requirements.txt .
#COPY test/test_data/* /app/test/test_data/
#RUN pip3 install --no-cache-dir -r requirements.txt
#RUN pip install ruff

# Install C shell for ASTEC installer
#RUN apt-get update && apt-get install -y csh

# Move the submodule to the desired location
#RUN mkdir -p $ASTEC_ROOT && cp -r -v test/astec_installer/* $ASTEC_ROOT && ls -l $ASTEC_ROOT
#RUN tar -xzf $ASTEC_ROOT/astecV3.1.2_linux64.tgz -C $ASTEC_ROOT 
#RUN ls -l $ASTEC_ROOT
#RUN ls -l $ASTEC_ROOT/astecV3.1.2_linux64
#RUN ls -l $ASTEC_ROOT/astecV3.1.2_linux64/astecV3.1.2-install-linux
#RUN file $ASTEC_ROOT/astecV3.1.2_linux64/astecV3.1.2-install-linux
#RUN chmod +x $ASTEC_ROOT/astecV3.1.2_linux64/astecV3.1.2-install-linux
#RUN csh $ASTEC_ROOT/astecV3.1.2_linux64/astecV3.1.2-install-linux

# Copy the ASTEC installer into the container
#COPY ./test/astec_installer/astecV3.1.2_linux64.tgz /tmp/

# Unzip and install ASTEC
#RUN tar -xzf /app/test/astec_installer/astecV3.1.2_linux64.tgz -C $ASTEC_ROOT && \
#    rm /tmp/astecV3.1.2_linux64.tgz && \
#    chmod +x $ASTEC_ROOT/install.sh && \
#    $ASTEC_ROOT/install.sh

#RUN ls -l $ASTEC_ROOT

# Install Python dependencies
#COPY requirements.txt .
#RUN pip3 install --no-cache-dir -r requirements.txt

# Set the working directory
#WORKDIR /app

# Copy the repository files into the container
#COPY . ./app