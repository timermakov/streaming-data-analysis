import logging
import time
from pathlib import Path
from typing import Iterable

from pyflink.common import Configuration
from pyflink.datastream import (
    CheckpointingMode,
    RuntimeExecutionMode,
    StreamExecutionEnvironment,
)
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention
from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment
from pyflink.table.udf import udf

from src.lab2.config import Lab2Config, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _as_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


@udf(result_type=DataTypes.STRING())
def delay_string(value: str, slowdown_seconds: float) -> str:
    if slowdown_seconds > 0:
        time.sleep(slowdown_seconds)
    return value


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
    checkpoint_config.set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    )
    return env


def create_table_environment(
    env: StreamExecutionEnvironment,
    config: Lab2Config,
) -> StreamTableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=settings,
    )
    table_env.create_temporary_function("delay_string", delay_string)
    table_env.get_config().set(
        "table.exec.source.idle-timeout",
        "5 s",
    )
    table_env.get_config().set(
        "pipeline.name",
        "lab2-kafka-flink-parquet",
    )
    return table_env


def create_source_table(table_env: StreamTableEnvironment, config: Lab2Config):
    source_ddl = f"""
    CREATE TABLE ecommerce_source (
        InvoiceNo STRING NOT NULL,
        StockCode STRING NOT NULL,
        Description STRING,
        Quantity INT NOT NULL,
        InvoiceDate STRING NOT NULL,
        UnitPrice DOUBLE NOT NULL,
        CustomerID BIGINT,
        Country STRING NOT NULL
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{config.kafka.topic}',
        'properties.bootstrap.servers' = '{config.kafka.bootstrap_servers}',
        'properties.group.id' = '{config.kafka.group_id}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'avro',
        'avro.encoding' = 'binary'
    )
    """
    table_env.execute_sql(source_ddl)


def create_sink_table(table_env: StreamTableEnvironment, config: Lab2Config):
    sink_ddl = f"""
    CREATE TABLE ecommerce_sink (
        InvoiceNo STRING NOT NULL,
        StockCode STRING NOT NULL,
        Description STRING,
        Quantity INT NOT NULL,
        InvoiceDate STRING NOT NULL,
        UnitPrice DOUBLE NOT NULL,
        CustomerID BIGINT,
        Country STRING NOT NULL
    ) PARTITIONED BY (Country) WITH (
        'connector' = 'filesystem',
        'path' = '{_as_file_uri(config.paths.output_path)}',
        'format' = 'parquet'
    )
    """
    table_env.execute_sql(sink_ddl)


def submit_insert_job(table_env: StreamTableEnvironment, config: Lab2Config):
    slowdown_seconds = config.flink.slowdown_seconds
    query = f"""
    INSERT INTO ecommerce_sink
    SELECT
        delay_string(InvoiceNo, CAST({slowdown_seconds} AS DOUBLE)) AS InvoiceNo,
        StockCode,
        Description,
        Quantity,
        InvoiceDate,
        UnitPrice,
        CustomerID,
        Country
    FROM ecommerce_source
    """
    return table_env.execute_sql(query)


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
    table_env = create_table_environment(env, job_config)
    create_source_table(table_env, job_config)
    create_sink_table(table_env, job_config)

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
    result = submit_insert_job(table_env, job_config)
    job_client = result.get_job_client()
    if job_client:
        logger.info("Submitted Flink job with id=%s", job_client.get_job_id())


if __name__ == "__main__":
    main()
