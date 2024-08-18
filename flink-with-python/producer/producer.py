import os
import json
from kafka import KafkaProducer

def send_data_and_template():
    """Sends input data and HTML template to Kafka"""

    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=os.environ["KAFKA_BROKER"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Input data that will be mapped to the template
    input_data = {
        "title": "Sample Title",
        "content": "This is sample content for the template."
    }

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

    # Send input data and HTML template to separate Kafka topics
    producer.send(os.environ["KAFKA_INPUT_TOPIC"], input_data)
    producer.send(os.environ["KAFKA_TEMPLATE_TOPIC"], {"template": html_template})

    producer.flush()

if __name__ == "__main__":
    send_data_and_template()
