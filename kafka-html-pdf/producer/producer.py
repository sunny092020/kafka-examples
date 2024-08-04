from kafka import KafkaProducer
import json
import os

def send_message(template, data):
    # Connect to the internal Docker network broker
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    message = {
        'template': template,
        'data': data
    }

    producer.send('html_to_pdf', message)
    producer.flush()
    producer.close()

# Example usage
if __name__ == "__main__":
    template_path = '/app/templates/example_template.html'
    
    if os.path.exists(template_path):
        template = open(template_path).read()
        data = {
            'title': 'Hello World',
            'content': 'This is a PDF generated from HTML.'
        }

        send_message(template, data)
    else:
        print(f"Template file not found at {template_path}")
