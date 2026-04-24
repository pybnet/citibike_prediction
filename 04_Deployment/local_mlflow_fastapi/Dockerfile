# Base image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TRANSFORMERS_CACHE=/tmp/hf_cache

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install system dependencies and Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        curl \
        build-essential \
        && pip install --upgrade pip \
        && pip install -r requirements.txt \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Copy app code
COPY . .

# Expose ports
EXPOSE 8000 7860