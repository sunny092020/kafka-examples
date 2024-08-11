#!/bin/bash

# Ensure the script stops on the first error
set -e

# Submit Flink jobs
echo "Submitting Flink jobs..."
docker-compose exec flink_jobs flink run -py split_job.py
docker-compose exec flink_jobs flink run -py map_job.py
docker-compose exec flink_jobs flink run -py pdf_printer_job.py
docker-compose exec flink_jobs flink run -py combine_pdf_job.py

echo "Pipeline completed successfully."

# Optional: Display the location of the final PDF output
echo "The final combined PDF is available in the container's /tmp/ directory."
