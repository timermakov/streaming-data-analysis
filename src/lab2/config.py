"""Lab2 configuration loaded from YAML."""

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(__file__).with_name("config.yaml")


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    group_id: str


@dataclass(frozen=True)
class PathConfig:
    csv_path: Path
    output_path: Path
    checkpoint_path: Path
    savepoint_path: Path


@dataclass(frozen=True)
class ProducerConfig:
    batch_size: int
    send_delay_seconds: float


@dataclass(frozen=True)
class FlinkConfig:
    checkpoint_interval_ms: int
    min_pause_between_checkpoints_ms: int
    checkpoint_timeout_ms: int
    max_concurrent_checkpoints: int
    slowdown_seconds: float


@dataclass(frozen=True)
class Lab2Config:
    kafka: KafkaConfig
    paths: PathConfig
    producer: ProducerConfig
    flink: FlinkConfig


def load_config(config_path: Path = CONFIG_PATH) -> Lab2Config:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    kafka = KafkaConfig(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            data["kafka"]["bootstrap_servers"],
        ),
        topic=os.getenv("KAFKA_TOPIC", data["kafka"]["topic"]),
        group_id=os.getenv("KAFKA_GROUP_ID", data["kafka"]["group_id"]),
    )
    paths = PathConfig(
        csv_path=PROJECT_ROOT / data["paths"]["csv_path"],
        output_path=PROJECT_ROOT / data["paths"]["output_path"],
        checkpoint_path=PROJECT_ROOT / data["paths"]["checkpoint_path"],
        savepoint_path=PROJECT_ROOT / data["paths"]["savepoint_path"],
    )
    producer = ProducerConfig(
        batch_size=int(data["producer"]["batch_size"]),
        send_delay_seconds=float(data["producer"]["send_delay_seconds"]),
    )
    flink = FlinkConfig(
        checkpoint_interval_ms=int(data["flink"]["checkpoint_interval_ms"]),
        min_pause_between_checkpoints_ms=int(data["flink"]["min_pause_between_checkpoints_ms"]),
        checkpoint_timeout_ms=int(data["flink"]["checkpoint_timeout_ms"]),
        max_concurrent_checkpoints=int(data["flink"]["max_concurrent_checkpoints"]),
        slowdown_seconds=float(os.getenv("SLOWDOWN_SECONDS", data["flink"]["slowdown_seconds"])),
    )
    return Lab2Config(kafka=kafka, paths=paths, producer=producer, flink=flink)
