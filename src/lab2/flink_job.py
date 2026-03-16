"""PyFlink DataStream job: Kafka (Avro bytes) -> Parquet."""

import logging
import time
from pathlib import Path
from typing import Iterable

from pyflink.common import Configuration, Types
from pyflink.common.serialization import DeserializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import (
    CheckpointingMode,
    RuntimeExecutionMode,
    StreamExecutionEnvironment,
)
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.formats.parquet import ParquetBulkWriters
from pyflink.datastream.functions import MapFunction

from src.common.ecommerce_avro import deserialize
from src.lab2.config import Lab2Config, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


class RawBytesDeserializationSchema(DeserializationSchema):
    """Pass-through deserialization schema for Kafka values."""

    def deserialize(self, message: bytes) -> bytes:
        return message

    def is_end_of_stream(self, next_element: bytes) -> bool:
        del next_element
        return False

    def get_produced_type(self):
        return Types.PRIMITIVE_ARRAY(Types.BYTE())


class AvroToRowMap(MapFunction):
    """Decode Avro payload and map to a typed row."""

    def __init__(self, slowdown_seconds: float):
        self._slowdown_seconds = slowdown_seconds

    def map(self, value: bytes):
        if self._slowdown_seconds > 0:
            time.sleep(self._slowdown_seconds)

        record = deserialize(value)
        return (
            record["InvoiceNo"],
            record["StockCode"],
            record["Description"],
            int(record["Quantity"]),
            record["InvoiceDate"],
            float(record["UnitPrice"]),
            record["CustomerID"],
            record["Country"],
        )


def create_row_type():
    return Types.ROW_NAMED(
        [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.LONG(),
            Types.STRING(),
        ],
    )


def ensure_directories(paths: Iterable[Path]):
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def create_environment(config: Lab2Config) -> StreamExecutionEnvironment:
    flink_config = Configuration()
    flink_config.set_string(
        "state.checkpoints.dir",
        _as_file_uri(config.paths.checkpoint_path),
    )
    flink_config.set_string(
        "state.savepoints.dir",
        _as_file_uri(config.paths.savepoint_path),
    )

    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(
        config.flink.checkpoint_interval_ms,
        CheckpointingMode.EXACTLY_ONCE,
    )

    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(
        config.flink.min_pause_between_checkpoints_ms
    )
    checkpoint_config.set_checkpoint_timeout(config.flink.checkpoint_timeout_ms)
    checkpoint_config.set_max_concurrent_checkpoints(
        config.flink.max_concurrent_checkpoints
    )
    checkpoint_config.set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    return env


def build_pipeline(env: StreamExecutionEnvironment, config: Lab2Config):
    row_type = create_row_type()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.kafka.bootstrap_servers)
        .set_topics(config.kafka.topic)
        .set_group_id(config.kafka.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(RawBytesDeserializationSchema())
        .build()
    )

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")
    rows = stream.map(
        AvroToRowMap(config.flink.slowdown_seconds),
        output_type=row_type,
    ).name("avro-to-row")

    sink = (
        FileSink.for_bulk_format(
            _as_file_uri(config.paths.output_path),
            ParquetBulkWriters.for_row_type(row_type),
        )
        .build()
    )
    rows.sink_to(sink).name("parquet-sink")


def main():
    job_config = load_config()
    ensure_directories(
        [
            job_config.paths.output_path,
            job_config.paths.checkpoint_path,
            job_config.paths.savepoint_path,
        ]
    )
    env = create_environment(job_config)
    build_pipeline(env, job_config)

    logger.info(
        (
            "Starting Flink job (topic=%s, bootstrap=%s, checkpoint every %d ms, "
            "checkpoint dir=%s, savepoint dir=%s)"
        ),
        job_config.kafka.topic,
        job_config.kafka.bootstrap_servers,
        job_config.flink.checkpoint_interval_ms,
        job_config.paths.checkpoint_path,
        job_config.paths.savepoint_path,
    )
    env.execute("lab2-kafka-flink-parquet")


if __name__ == "__main__":
    main()
