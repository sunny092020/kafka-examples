import argparse
import os
import json
import logging
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_message(template, data):
    try:
        # Connect to the internal Docker network broker
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=60000,  # Increase the block time to 60 seconds
            retries=5,           # Retry sending messages if they fail
            retry_backoff_ms=500 # Backoff time between retries
        )

        message = {
            'template': template,
            'data': data
        }

        producer.send('html_to_pdf', message)
        producer.flush()
        producer.close()
        logging.info("Message sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send message: {e}")

def main(template_path, title, content):
    if os.path.exists(template_path):
        try:
            with open(template_path, 'r') as file:
                template = file.read()
            data = {
                'title': title,
                'content': content
            }
            send_message(template, data)
        except IOError as e:
            logging.error(f"Error reading template file: {e}")
    else:
        logging.error(f"Template file not found at {template_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send HTML template to Kafka.')
    parser.add_argument('--template', type=str, required=True, help='Path to the HTML template file')
    parser.add_argument('--title', type=str, required=True, help='Title for the HTML content')
    parser.add_argument('--content', type=str, required=True, help='Content for the HTML template')
    args = parser.parse_args()

    main(args.template, args.title, args.content)
