#!/bin/bash

# Ensure the script stops on the first error
set -e

# Run the Kafka producer
echo "Running Kafka producer..."
docker-compose run --rm producer

# Submit Flink jobs
echo "Submitting Flink jobs..."
docker-compose run --rm flink_jobs flink run -py split_job.py
docker-compose run --rm flink_jobs flink run -py map_job.py
docker-compose run --rm flink_jobs flink run -py pdf_printer_job.py
docker-compose run --rm flink_jobs flink run -py combine_pdf_job.py

echo "Pipeline completed successfully."

# Optional: Display the location of the final PDF output
echo "The final combined PDF is available in the container's /tmp/ directory."
