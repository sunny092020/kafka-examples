import os
import json
from kafka import KafkaProducer

def send_data_and_template():
    """Sends combined input data and HTML template to Kafka"""

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=os.environ["KAFKA_BROKER"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Input data and HTML template combined into one message
    combined_message = {
        "input_data": {
            "title": "Sample Title",
            "content": "This is sample content for the template."
        },
        "template": """
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
    }

    # Send the combined message to the Kafka input topic
    producer.send(os.environ["KAFKA_INPUT_TOPIC"], combined_message)
    producer.flush()

if __name__ == "__main__":
    send_data_and_template()
