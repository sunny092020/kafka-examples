import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from jinja2 import Template
import pdfkit
from PyPDF2 import PdfMerger


class InputSplitter(FlatMapFunction):
    """
    A Flink FlatMapFunction that splits the input data into individual pages.
    """
class InputSplitter(FlatMapFunction):
    """
    A Flink FlatMapFunction that splits the input data into individual pages.
    """
    def flat_map(self, message):
        print(f"Received message: {message}")
        data = json.loads(message)
        pages_data = data.get("pages_data", [])
        template_str = data.get("template")

        # Emit each page individually with the template using yield
        for page_data in pages_data:
            yield {"page_data": page_data, "template": template_str}

class DataMapFunction(MapFunction):
    """
    A Flink MapFunction that maps data to a template and returns the rendered HTML.
    """
    def map(self, message):
        page_data = message["page_data"]
        template_str = message["template"]

        # Load template using Jinja2
        template = Template(template_str)

        # Render HTML for the page using input data
        rendered_html = template.render(page_data)

        return rendered_html


class PDFGenerationFunction(MapFunction):
    """
    A Flink MapFunction that generates PDF from the rendered HTML.
    """
    def map(self, rendered_html):
        # Generate a unique filename for each page
        pdf_output_path = os.path.join(os.environ.get("PDF_OUTPUT_DIR", "/pdf_output"), f"page_{hash(rendered_html)}.pdf")
        
        # Convert rendered HTML to PDF and save it as a single page PDF
        pdfkit.from_string(rendered_html, pdf_output_path)

        return pdf_output_path


class PDFJoiner(MapFunction):
    """
    A Flink MapFunction that joins individual PDF pages into a single PDF file.
    """
    def map(self, pdf_page_paths):
        merger = PdfMerger()

        # Merge all the PDF pages into one
        for pdf_path in pdf_page_paths:
            merger.append(pdf_path)

        # Output path for the final joined PDF
        final_pdf_path = os.path.join(os.environ.get("PDF_OUTPUT_DIR", "/pdf_output"), "final_output.pdf")
        
        # Write out the merged PDF
        merger.write(final_pdf_path)
        merger.close()

        return final_pdf_path


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

    # Step 1: Split the input data into pages
    split_pages_stream = data_stream.flat_map(InputSplitter(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Step 2: Render HTML for each page independently using DataMapFunction
    rendered_html_stream = split_pages_stream.map(DataMapFunction(), output_type=Types.STRING())

    # Step 3: Convert the rendered HTML to individual PDF pages using PDFGenerationFunction
    pdf_page_stream = rendered_html_stream.map(PDFGenerationFunction(), output_type=Types.STRING())

    # Step 4: Join all the individual PDF pages into a single PDF file using PDFJoiner
    final_pdf_stream = pdf_page_stream.map(PDFJoiner(), output_type=Types.STRING())

    # Execute the job
    env.execute("flink_consumer_to_pdf")


if __name__ == "__main__":
    flink_consumer_to_pdf()
