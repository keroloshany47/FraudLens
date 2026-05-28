FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fraudlens_exporter.py .

EXPOSE 8000

# Unbuffered output so logs appear immediately in `docker compose logs`
ENV PYTHONUNBUFFERED=1

CMD ["python", "fraudlens_exporter.py"]
