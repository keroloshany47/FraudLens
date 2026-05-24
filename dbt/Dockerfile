FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN pip install --no-cache-dir \
    dbt-postgres==1.8.0 \
    dbt-utils==1.2.0

COPY . .

ENTRYPOINT ["dbt"]
CMD ["--help"]
