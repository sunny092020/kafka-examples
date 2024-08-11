FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py producer.py

# Start a shell or long-running process
CMD ["tail", "-f", "/dev/null"]
