FROM ubuntu:24.04

# INSTALL DEPENDENCIES 
RUN apt-get update && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    curl \
    wget \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    docker.io

# CREATE PYTHON VENV
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# ACTIVATE VENV
RUN . /venv/bin/activate

# INSTALL REQUIREMENTS
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app

CMD ["bash"]