"""Utility to verify Lab2 output by counting rows in Parquet files."""

import logging
from pathlib import Path

from pyspark.sql import SparkSession

from src.lab2.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("Lab2ParquetVerifier").master("local[*]").getOrCreate()


def count_parquet_rows(parquet_path: Path) -> int:
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet path does not exist: {parquet_path}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        return spark.read.parquet(str(parquet_path)).count()
    finally:
        spark.stop()


def main():
    config = load_config()
    total = count_parquet_rows(Path(config.paths.output_path))
    logger.info("Parquet rows count: %d", total)


if __name__ == "__main__":
    main()
