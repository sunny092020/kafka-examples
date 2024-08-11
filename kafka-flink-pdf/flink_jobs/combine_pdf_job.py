from PyPDF2 import PdfMerger

def combine_pdfs(data):
    pdf_files = [json.loads(data)['pdf_file'] for data in data_list]
    merger = PdfMerger()
    for pdf in pdf_files:
        merger.append(pdf)
    output_pdf_path = '/tmp/combined_output.pdf'
    merger.write(output_pdf_path)
    merger.close()
    return output_pdf_path

env = StreamExecutionEnvironment.get_execution_environment()
env.add_source(FlinkKafkaConsumer(
    'pdf_pages_output',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
)).reduce(combine_pdfs).add_sink(FlinkKafkaProducer(
    'final_combined_pdf',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
))

env.execute('Combine PDFs Job')
