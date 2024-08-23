import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from jinja2 import Template
import pdfkit  # Library to convert HTML to PDF (make sure to install it)

def process_message(message):
    """Processes the message, renders HTML, and converts it to PDF"""
    data = json.loads(message)
    input_data = data["input_data"]
    template_str = data["template"]

    # Load template using Jinja2
    template = Template(template_str)

    # Render HTML using input data
    rendered_html = template.render(input_data)

    # Convert rendered HTML to PDF
    pdf_output_path = os.path.join(os.environ.get("PDF_OUTPUT_DIR", "/pdf_output"), "output.pdf")
    pdfkit.from_string(rendered_html, pdf_output_path)

    return pdf_output_path

def flink_consumer_to_pdf():
    """Flink task that reads input data and template from Kafka, renders HTML, and converts it to PDF"""

    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Flink Kafka connectors
    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # Define the Kafka source for combined input data and HTML template
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"])
        .set_topics(os.environ["KAFKA_INPUT_TOPIC"])
        .set_group_id("flink_input_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Read from the Kafka source
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Input Source"
    )

    # Process each message in the stream
    data_stream.map(process_message, output_type=Types.STRING())

    # Execute the job
    env.execute("flink_consumer_to_pdf")


if __name__ == "__main__":
    flink_consumer_to_pdf()
