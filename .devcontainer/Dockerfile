FROM python:3.11-slim

WORKDIR /workspace

# Install system dependencies
RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y git
RUN apt-get install -y docker.io
RUN apt-get install -y vim
RUN rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY src/ .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install .[test]
RUN pip install -e .[dev]