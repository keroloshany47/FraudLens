FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    python3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    dbt-core==1.8.0 \
    dbt-postgres==1.8.0

COPY . .

# No ENTRYPOINT — let docker-compose control the command
CMD ["tail", "-f", "/dev/null"]
