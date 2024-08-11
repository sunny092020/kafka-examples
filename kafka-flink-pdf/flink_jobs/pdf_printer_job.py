import pdfkit

def html_to_pdf(data):
    parsed_data = json.loads(data)
    page_html = parsed_data['html']
    pdf_file_path = f'/tmp/page_{parsed_data["page_id"]}.pdf'
    pdfkit.from_string(page_html, pdf_file_path)
    return pdf_file_path

env = StreamExecutionEnvironment.get_execution_environment()
env.add_source(FlinkKafkaConsumer(
    'mapped_html_pages',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
)).map(html_to_pdf).add_sink(FlinkKafkaProducer(
    'pdf_pages_output',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
))

env.execute('HTML to PDF Job')
