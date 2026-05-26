import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, udf, hour, to_timestamp, lit, when
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, IntegerType, FloatType
)

sys.path.insert(0, "/opt/spark-apps/jobs/utils")
from geo_utils import haversine_km
from fraud_scorer import compute_risk_score, build_alert_reason
from dlq_handler import send_to_dlq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fraudlens.spark")

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",      "raw_transactions")
TOPIC_ENRICHED    = os.getenv("KAFKA_TOPIC_ENRICHED",  "enriched_transactions")
TOPIC_ALERTS      = os.getenv("KAFKA_TOPIC_ALERTS",    "fraud_alerts")
POSTGRES_URL      = "jdbc:postgresql://postgres:5432/fraudlens"
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "fraudlens")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraudlens_secret")
CHECKPOINT_BASE   = "/opt/spark-apps/checkpoints"

RAW_SCHEMA = StructType([
    StructField("trans_num",             StringType(),  True),
    StructField("trans_date_trans_time", StringType(),  True),
    StructField("unix_time",             LongType(),    True),
    StructField("amt",                   DoubleType(),  True),
    StructField("cc_num",                LongType(),    True),
    StructField("merchant",              StringType(),  True),
    StructField("category",              StringType(),  True),
    StructField("merch_lat",             DoubleType(),  True),
    StructField("merch_long",            DoubleType(),  True),
    StructField("lat",                   DoubleType(),  True),
    StructField("long",                  DoubleType(),  True),
    StructField("city_pop",              IntegerType(), True),
    StructField("is_fraud",              IntegerType(), True),
    StructField("first",                 StringType(),  True),
    StructField("last",                  StringType(),  True),
    StructField("gender",                StringType(),  True),
    StructField("dob",                   StringType(),  True),
    StructField("job",                   StringType(),  True),
    StructField("street",                StringType(),  True),
    StructField("city",                  StringType(),  True),
    StructField("state",                 StringType(),  True),
    StructField("zip",                   StringType(),  True),
    StructField("produced_at",           StringType(),  True),
])

haversine_udf = udf(haversine_km,         FloatType())
risk_score_udf = udf(compute_risk_score,  FloatType())
alert_reason_udf = udf(build_alert_reason, StringType())

JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}


def write_to_oltp(df, epoch_id: int):
    rows = df.collect()
    if not rows:
        return

    try:
        df.select(
            col("trans_num").alias("trans_id"),
            col("cc_num").alias("cc_num"),
            col("merchant").alias("merchant_name"),
            col("category"),
            col("trans_at"),
            col("unix_time"),
            col("amt").alias("amount"),
            col("is_fraud"),
            col("lat"), col("long"),
            col("merch_lat"), col("merch_long"),
            col("city_pop"),
            col("first"), col("last"), col("gender"),
            col("dob"), col("job"),
            col("street"), col("city"), col("state"), col("zip"),
            lit("stream").alias("source"),
        ).write.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "stream_staging") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()

        df.select(
            col("trans_num").alias("trans_id"),
            col("trans_at"),
            col("unix_time"),
            col("amt").alias("amount"),
            col("is_fraud"),
            lit("stream").alias("source"),
        ).write.format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "transactions") \
            .option("driver", "org.postgresql.Driver") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("append") \
            .save()

        fraud_df = df.filter(
            (col("is_fraud") == 1) | (col("risk_score") >= 0.7)
        )

        if fraud_df.count() > 0:
            fraud_df.select(
                col("trans_num").alias("trans_id"),
                col("risk_score"),
                col("distance_km"),
                col("alert_reason"),
            ).write.format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", "fraud_alerts") \
                .option("driver", "org.postgresql.Driver") \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .mode("append") \
                .save()

        log.info("Batch %d: wrote %d rows (%d fraud alerts)",
                 epoch_id, len(rows), fraud_df.count())

    except Exception as exc:
        log.error("Batch %d failed: %s — routing to DLQ", epoch_id, exc)
        send_to_dlq([r.asDict() for r in rows], str(exc))


def main():
    spark = SparkSession.builder \
        .appName("FraudLens-StreamProcessor") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session started")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_RAW) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), RAW_SCHEMA).alias("d")
    ).select("d.*")

    enriched = parsed \
        .withColumn(
            "trans_at",
            to_timestamp(col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss")
        ) \
        .withColumn(
            "distance_km",
            haversine_udf(col("lat"), col("long"), col("merch_lat"), col("merch_long"))
        ) \
        .withColumn(
            "risk_score",
            risk_score_udf(col("amt"), col("distance_km"), col("category"))
        ) \
        .withColumn(
            "alert_reason",
            alert_reason_udf(col("amt"), col("distance_km"), col("category"))
        ) \
        .withColumn("hour_of_day", hour(col("trans_at"))) \
        .filter(col("trans_num").isNotNull())

    query = enriched.writeStream \
        .trigger(processingTime="5 seconds") \
        .foreachBatch(write_to_oltp) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/oltp_writer") \
        .start()

    log.info("Streaming query started — awaiting termination")
    query.awaitTermination()


if __name__ == "__main__":
    main()
