FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PORT=8080

# Worker Pool: not a web server — runs a Pub/Sub subscriber loop.
# The health HTTP server (port $PORT) handles Cloud Run liveness probes only.
CMD ["python", "-m", "src.main"]
