from kafka import KafkaProducer
import json

def send_message(template, data):
    producer = KafkaProducer(
        bootstrap_servers='kafka:29092',
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
    template = open('/app/templates/example_template.html').read()
    data = {
        'title': 'Hello World',
        'content': 'This is a PDF generated from HTML.'
    }

    send_message(template, data)
