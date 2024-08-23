import os
import json
from kafka import KafkaProducer

def send_data_for_multiple_pages():
    """Sends input data for multiple pages and a common HTML template to Kafka"""

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=os.environ["KAFKA_BROKER"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Data for multiple pages
    pages_data = [
        {
            "title": "Page 1 Title",
            "content": "This is content for page 1."
        },
        {
            "title": "Page 2 Title",
            "content": "This is content for page 2."
        },
        {
            "title": "Page 3 Title",
            "content": "This is content for page 3."
        }
    ]

    # HTML template with placeholders for dynamic data
    html_template = """
    <html>
        <head>
            <title>{{ title }}</title>
        </head>
        <body>
            <h1>{{ title }}</h1>
            <p>{{ content }}</p>
        </body>
    </html>
    """

    # Combine the data for multiple pages and the template into one message
    combined_message = {
        "pages_data": pages_data,
        "template": html_template
    }

    # Send the combined message to the Kafka input topic
    producer.send(os.environ["KAFKA_INPUT_TOPIC"], combined_message)
    producer.flush()

if __name__ == "__main__":
    send_data_for_multiple_pages()
