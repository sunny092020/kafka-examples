import os
import json
from pyflink.datastream import StreamExecutionEnvironment, CoMapFunction
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from jinja2 import Template
import pdfkit  # Library to convert HTML to PDF (make sure to install it)

class RenderToPDFCoMapFunction(CoMapFunction):
    def __init__(self):
        self.template_data = None  # Hold the template data
        self.input_data = None  # Hold the input data

    def map1(self, input_data):
        # Process the input data stream
        self.input_data = json.loads(input_data)
        return self._try_render_pdf()

    def map2(self, template_data):
        # Process the template stream
        self.template_data = json.loads(template_data)
        return self._try_render_pdf()

    def _try_render_pdf(self):
        # If both input_data and template_data are available, render and convert to PDF
        if self.input_data and self.template_data:
            # Load template using Jinja2
            template_str = self.template_data["template"]
            template = Template(template_str)

            # Render HTML using input data
            rendered_html = template.render(self.input_data)

            # Convert rendered HTML to PDF
            pdf_output_path = os.path.join(os.environ.get("PDF_OUTPUT_DIR", "/pdf_output"), "output.pdf")
            pdfkit.from_string(rendered_html, pdf_output_path)

            return pdf_output_path

        # Return None if one of the streams hasn't been fully processed yet
        return None


def flink_consumer_to_pdf():
    """Flink task that reads input data and template from Kafka, renders HTML, and converts it to PDF"""

    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Flink Kafka connectors
    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # Define the Kafka source for input data and HTML template
    kafka_input_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"])
        .set_topics(os.environ["KAFKA_INPUT_TOPIC"])
        .set_group_id("flink_input_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    kafka_template_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"])
        .set_topics(os.environ["KAFKA_TEMPLATE_TOPIC"])
        .set_group_id("flink_template_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Read from both Kafka sources
    input_data_stream = env.from_source(
        kafka_input_source, WatermarkStrategy.no_watermarks(), "Kafka Input Source"
    )

    template_stream = env.from_source(
        kafka_template_source, WatermarkStrategy.no_watermarks(), "Kafka Template Source"
    )

    # Join input data and template data streams using a CoMapFunction
    connected_stream = input_data_stream.connect(template_stream)

    # Apply the CoMapFunction to join and process the streams
    connected_stream.map(RenderToPDFCoMapFunction(), output_type=Types.STRING())

    # Execute the job
    env.execute("flink_consumer_to_pdf")


if __name__ == "__main__":
    flink_consumer_to_pdf()
