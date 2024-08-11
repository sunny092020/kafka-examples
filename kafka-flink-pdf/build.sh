#!/bin/bash

# Ensure the script stops on the first error
set -e

# Build Docker images using Docker Compose
echo "Building Docker images with Docker Compose..."
docker-compose build

echo "Docker images built successfully."
