from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

data = {
    "html_template": "<html><body>{content}</body></html>",
    "pages": [
        {"page_id": 1, "content": "This is page 1 content"},
        {"page_id": 2, "content": "This is page 2 content"},
        # Add more pages as needed
    ]
}

producer.send('html_template_with_pages', data)
producer.flush()
