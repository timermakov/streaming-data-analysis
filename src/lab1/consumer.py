"""Spark Structured Streaming consumer: reads Avro from Kafka, writes Parquet."""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, to_timestamp

from src.lab1.config import (
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    OUTPUT_PATH,
)
from src.lab1.schema import AVRO_SCHEMA_JSON

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
    "org.apache.spark:spark-avro_2.13:4.1.1",
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ECommerceStreamConsumer")
        .master("local[*]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.streaming.checkpointLocation", str(CHECKPOINT_PATH))
        .getOrCreate()
    )


def build_pipeline(spark: SparkSession):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_avro(col("value"), AVRO_SCHEMA_JSON).alias("data"))
        .select("data.*")
    )

    transformed = (
        parsed
        .withColumn(
            "InvoiceDateParsed",
            to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"),
        )
        .withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
    )

    return transformed


def start_streaming(df):
    query = (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", str(OUTPUT_PATH))
        .option("checkpointLocation", str(CHECKPOINT_PATH))
        .partitionBy("Country")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


def main():
    logger.info(
        "Starting Spark consumer: topic=%s, servers=%s",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS,
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = build_pipeline(spark)
    query = start_streaming(df)

    logger.info("Streaming started, writing Parquet to %s", OUTPUT_PATH)
    query.awaitTermination()


if __name__ == "__main__":
    main()
