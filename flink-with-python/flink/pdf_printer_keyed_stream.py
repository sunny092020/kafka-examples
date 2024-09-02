import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from jinja2 import Template
import time
from datetime import datetime
import pdfkit
from PyPDF2 import PdfMerger


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
    

class PDFGenerationFunction(MapFunction):
    """
    A Flink MapFunction that generates PDF from the rendered HTML.
    """
    def map(self, rendered_html):
        print("Start print:", datetime.now().time())
        print(f"Generating PDF for rendered HTML: {rendered_html}")
        time.sleep(3)
        # Generate a unique filename for each page
        pdf_output_path = os.path.join(os.environ.get("PDF_OUTPUT_DIR", "/pdf_output"), f"page_{hash(rendered_html)}.pdf")
        
        # Convert rendered HTML to PDF and save it as a single page PDF
        pdfkit.from_string(rendered_html, pdf_output_path)
        print("End print:", datetime.now().time())

        return pdf_output_path


class PDFMergerFunction(FlatMapFunction):
    """
    A FlatMapFunction that merges individual PDF pages into a single PDF file.
    """
    def __init__(self, expected_page_count):
        self.pdf_paths = []
        self.expected_page_count = expected_page_count  # Set the expected page count

    def flat_map(self, value):
        # Collect the individual PDF file paths
        self.pdf_paths.append(value)

        # If all PDFs have been processed, merge them into a single PDF
        if len(self.pdf_paths) == self.expected_page_count:
            print("Merging PDF pages into a single PDF...")
            pdf_output_dir = os.environ.get("PDF_OUTPUT_DIR", "/pdf_output")
            merged_pdf_path = os.path.join(pdf_output_dir, "merged_output.pdf")

            # Initialize a PdfMerger
            merger = PdfMerger()

            # Add each individual PDF file to the merger
            for pdf_path in self.pdf_paths:
                merger.append(pdf_path)

                # Remove the individual PDF file after merging
                os.remove(pdf_path)

            # Write the merged PDF to a file
            merger.write(merged_pdf_path)
            merger.close()
            print(f"PDF merged successfully into {merged_pdf_path}")

            # Yield the path of the merged PDF as the result
            yield merged_pdf_path


def flink_consumer_to_pdf():
    """Flink task that reads input data and template from Kafka, processes HTML, generates PDFs for multiple pages, and joins them into a single PDF"""

    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Flink Kafka connectors
    env.add_jars("file:///jars/flink-sql-connector-kafka-3.0.1-1.18.jar")

    # Define the Kafka source for combined input data and HTML template
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.environ["KAFKA_BROKER"])
        .set_topics("pdf_printer_topic")
        .set_group_id("flink_input_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Read from the Kafka source
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Input Source"
    )

    # Extract number of pages from the first message
    message_example = json.loads(data_stream.execute_and_collect().__next__())
    num_pages = len(message_example.get("pages_data", []))

    # Lists to hold the streams for each page
    rendered_html_streams = []
    pdf_page_streams = []

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

    # Generate streams for each page
    for i in range(num_pages):
        rendered_html_stream = create_page_stream(i)
        rendered_html_stream.start_new_chain()
        rendered_html_streams.append(rendered_html_stream)

        pdf_page_stream = rendered_html_stream.map(PDFGenerationFunction(), output_type=Types.STRING())
        pdf_page_streams.append(pdf_page_stream)

    # Union all the PDF streams into one and merge them into a single PDF
    merged_pdf_stream = pdf_page_streams[0].union(*pdf_page_streams[1:]) \
                                            .flat_map(PDFMergerFunction(num_pages), output_type=Types.STRING())

    # Execute the job
    env.execute("flink_consumer_to_pdf")

if __name__ == "__main__":
    flink_consumer_to_pdf()
