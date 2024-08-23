import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from jinja2 import Template
import pdfkit  # Library to convert HTML to PDF (make sure to install it)


class DataMapFunction(MapFunction):
    """
    A Flink MapFunction that maps data to a template and returns the rendered HTML.
    """
    def open(self, runtime_context):
        # Load the template when the function starts
        self.template_str = None

    def map(self, message):
        # Parse the incoming message
        data = json.loads(message)

        # Split data into pages_data and template_str
        pages_data = data.get("pages_data", [])
        if self.template_str is None:
            self.template_str = data.get("template")

        # Initialize a list to hold the rendered HTML for all pages
        all_pages_html = ""

        # Load template using Jinja2
        template = Template(self.template_str)

        # Render HTML for each page using the template
        for page_data in pages_data:
            rendered_html = template.render(page_data)
            all_pages_html += rendered_html + "<div style='page-break-after: always;'></div>"

        return all_pages_html


class PDFGenerationFunction(MapFunction):
    """
    A Flink MapFunction that generates PDF from the rendered HTML.
    """
    def map(self, rendered_html):
        # Convert rendered HTML to PDF
        pdf_output_path = os.path.join(os.environ.get("PDF_OUTPUT_DIR", "/pdf_output"), "output.pdf")
        pdfkit.from_string(rendered_html, pdf_output_path)

        return pdf_output_path


def flink_consumer_to_pdf():
    """Flink task that reads input data and template from Kafka, processes HTML using DataMapFunction, and converts it to a single PDF"""

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

    # First map task: Render HTML for all pages
    rendered_html_stream = data_stream.map(DataMapFunction(), output_type=Types.STRING())

    # Second map task: Convert the rendered HTML to PDF
    pdf_stream = rendered_html_stream.map(PDFGenerationFunction(), output_type=Types.STRING())

    # Execute the job
    env.execute("flink_consumer_to_pdf")


if __name__ == "__main__":
    flink_consumer_to_pdf()
