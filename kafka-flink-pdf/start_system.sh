#!/bin/bash

# Ensure the script stops on the first error
set -e

# Start Kafka, Zookeeper, and Flink clusters
echo "Starting Kafka, Zookeeper, and Flink clusters..."
docker-compose up

# Give some time for Kafka, Zookeeper, and Flink to fully start
echo "Waiting for services to start..."
sleep 15

echo "Services started successfully."
