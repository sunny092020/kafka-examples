import os
import json
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic_if_not_exists(topic_name, num_partitions=1, replication_factor=1):
    """Create a Kafka topic if it does not exist."""
    admin_client = KafkaAdminClient(bootstrap_servers=os.environ["KAFKA_BROKER"])
    
    topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")

def send_data_for_multiple_pages():
    """Sends input data for multiple pages and a common HTML template to Kafka"""

    topic_name = "pdf_printer_topic"
    
    # Create topic if it does not exist
    create_topic_if_not_exists(topic_name)
    
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
    producer.send(topic_name, combined_message)
    producer.flush()

if __name__ == "__main__":
    send_data_for_multiple_pages()
