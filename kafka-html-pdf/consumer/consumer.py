from kafka import KafkaConsumer
import json
from jinja2 import Template
from weasyprint import HTML

def consume_messages():
    # Connect to the internal Docker network broker
    consumer = KafkaConsumer(
        'html_to_pdf',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        template_str = message.value['template']
        data = message.value['data']

        # Bind data to template
        template = Template(template_str)
        html_content = template.render(data)

        # Convert HTML to PDF
        pdf_file = HTML(string=html_content).write_pdf()

        # Save PDF to a file
        with open('/app/data/output.pdf', 'wb') as f:
            f.write(pdf_file)

        print("PDF generated and saved as /app/data/output.pdf")

if __name__ == "__main__":
    consume_messages()
