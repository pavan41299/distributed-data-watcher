FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY watcher/ ./watcher/

RUN mkdir -p /app/inbox /app/processed /app/error /app/data

CMD ["python", "-m", "watcher", "--inbox", "/app/inbox", "--processed", "/app/processed", "--error", "/app/error", "--db", "/app/data/telemetry.sqlite3"]
