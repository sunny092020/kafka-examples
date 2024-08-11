def map_to_html(data):
    parsed_data = json.loads(data)
    html_template = parsed_data['html_template']
    page_content = parsed_data['page']['content']
    page_html = html_template.replace('{content}', page_content)
    parsed_data['page']['html'] = page_html
    return json.dumps(parsed_data['page'])

env = StreamExecutionEnvironment.get_execution_environment()
env.add_source(FlinkKafkaConsumer(
    'split_pages_output',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
)).map(map_to_html).add_sink(FlinkKafkaProducer(
    'mapped_html_pages',
    SimpleStringSchema(),
    {'bootstrap.servers': 'kafka:9092'}
))

env.execute('Map to HTML Job')
