from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json

def split_pages(data):
    parsed_data = json.loads(data)
    html_template = parsed_data['html_template']
    for page in parsed_data['pages']:
        yield json.dumps({'html_template': html_template, 'page': page})

env = StreamExecutionEnvironment.get_execution_environment()
env.add_source(FlinkKafkaConsumer(
    'html_template_with_pages',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
)).flat_map(split_pages).add_sink(FlinkKafkaProducer(
    'split_pages_output',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
))

env.execute('Split Pages Job')
