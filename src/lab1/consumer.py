"""Spark Structured Streaming consumer: reads Avro from Kafka, writes Parquet."""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, to_timestamp

from src.lab1.config import Lab1Config, load_config
from src.common.ecommerce_avro import AVRO_SCHEMA_JSON

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
    "org.apache.spark:spark-avro_2.13:4.1.1",
])


def create_spark_session(config: Lab1Config) -> SparkSession:
    return (
        SparkSession.builder
        .appName("ECommerceStreamConsumer")
        .master("local[*]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.streaming.checkpointLocation", str(config.paths.checkpoint_path))
        .getOrCreate()
    )


def build_pipeline(spark: SparkSession, config: Lab1Config):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", config.kafka.topic)
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


def start_streaming(df, config: Lab1Config):
    query = (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", str(config.paths.output_path))
        .option("checkpointLocation", str(config.paths.checkpoint_path))
        .partitionBy("Country")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


def main():
    config = load_config()
    logger.info(
        "Starting Spark consumer: topic=%s, servers=%s",
        config.kafka.topic,
        config.kafka.bootstrap_servers,
    )

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    df = build_pipeline(spark, config)
    query = start_streaming(df, config)

    logger.info("Streaming started, writing Parquet to %s", config.paths.output_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
