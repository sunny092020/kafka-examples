import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from jinja2 import Template
import time
from datetime import datetime

class DataMapFunction(MapFunction):
    """
    A Flink MapFunction that maps data to a template and returns the rendered HTML.
    """
    def map(self, message):
        # Get the current time for logging
        print("Start map:", datetime.now().time())
        print(f"Processing page data: {message}")

        # Deserialize the JSON message
        message = json.loads(message)
        page_data = message["page_data"]
        template_str = message["template"]

        # Load template using Jinja2
        template = Template(template_str)

        # Render HTML for the page using input data
        rendered_html = template.render(page_data)

        # Simulate processing time
        time.sleep(3)
        print("End map:", datetime.now().time())

        return rendered_html


def flink_consumer_to_pdf():
    """Flink task that reads input data and template from Kafka, processes HTML, generates PDFs for multiple pages, and joins them into a single PDF"""

    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(3)

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

    # Helper function to create a stream for a specific page
    def create_page_stream(page_no):
        def key_selector(value):
            value = json.loads(value)
            pages_data = value.get("pages_data", [])
            page_data = pages_data[page_no]
            template = value.get("template")
            return json.dumps({"page_data": page_data, "template": template})

        # Key the stream by extracting page data for the specified page number
        keyed_stream = data_stream.map(key_selector, output_type=Types.STRING())
        
        # Process the keyed stream with the DataMapFunction to render HTML for this page
        rendered_html_stream = keyed_stream.map(DataMapFunction(), output_type=Types.STRING())
        return rendered_html_stream

    # Create streams for each page (here we assume three pages)
    rendered_html_stream_1 = create_page_stream(0)
    rendered_html_stream_2 = create_page_stream(1)
    rendered_html_stream_3 = create_page_stream(2)

    # You can now continue to process these separate streams independently
    # (e.g., convert to PDFs, aggregate results, etc.)

    # Execute the job
    env.execute("flink_consumer_to_pdf")


if __name__ == "__main__":
    flink_consumer_to_pdf()
