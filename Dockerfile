# Dockerfile for mock handle server
FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir . flask

EXPOSE 5000

CMD ["python", "src/piddiplatsch/testing/mock_handle_server.py"]
