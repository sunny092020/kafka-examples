FROM openjdk:8-jre-slim

# Install Python and necessary tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Install Flink
RUN wget https://archive.apache.org/dist/flink/flink-1.13.2/flink-1.13.2-bin-scala_2.11.tgz && \
    tar -xvzf flink-1.13.2-bin-scala_2.11.tgz && \
    mv flink-1.13.2 /opt/flink && \
    ln -s /opt/flink/bin/flink /usr/local/bin/flink && \
    rm flink-1.13.2-bin-scala_2.11.tgz

# Download the Flink Kafka connector JAR
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_2.11/1.13.2/flink-connector-kafka_2.11-1.13.2.jar -P /opt/flink/lib/

# Download the Kafka client JAR
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.0/kafka-clients-2.8.0.jar -P /opt/flink/lib/

# Copy application code
COPY . .

# Keep the container running
CMD ["tail", "-f", "/dev/null"]
