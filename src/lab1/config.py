"""Lab1 configuration loaded from YAML."""

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


@dataclass(frozen=True)
class PathConfig:
    csv_path: Path
    output_path: Path
    checkpoint_path: Path


@dataclass(frozen=True)
class ProducerConfig:
    batch_size: int
    send_delay_seconds: float


@dataclass(frozen=True)
class Lab1Config:
    kafka: KafkaConfig
    paths: PathConfig
    producer: ProducerConfig


def load_config(config_path: Path = CONFIG_PATH) -> Lab1Config:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    kafka = KafkaConfig(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            data["kafka"]["bootstrap_servers"],
        ),
        topic=os.getenv("KAFKA_TOPIC", data["kafka"]["topic"]),
    )
    paths = PathConfig(
        csv_path=PROJECT_ROOT / data["paths"]["csv_path"],
        output_path=PROJECT_ROOT / data["paths"]["output_path"],
        checkpoint_path=PROJECT_ROOT / data["paths"]["checkpoint_path"],
    )
    producer = ProducerConfig(
        batch_size=int(os.getenv("PRODUCER_BATCH_SIZE", data["producer"]["batch_size"])),
        send_delay_seconds=float(
            os.getenv("PRODUCER_SEND_DELAY", data["producer"]["send_delay_seconds"])
        ),
    )
    return Lab1Config(kafka=kafka, paths=paths, producer=producer)
