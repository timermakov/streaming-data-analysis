"""Lab3 configuration loaded from YAML."""

from __future__ import annotations

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
class ProducerConfig:
    events_count: int
    send_delay_seconds: float
    buffer_size: int
    out_of_order_probability: float
    late_probability: float
    late_release_every: int
    event_interval_ms: int
    user_id_min: int
    user_id_max: int
    event_types: tuple[str, ...]


@dataclass(frozen=True)
class FlinkConfig:
    window_size_seconds: int
    watermark_out_of_orderness_seconds: int
    allowed_lateness_seconds: int


@dataclass(frozen=True)
class Lab3Config:
    kafka: KafkaConfig
    producer: ProducerConfig
    flink: FlinkConfig


def _validate_probability(name: str, value: float) -> float:
    if value < 0 or value > 1:
        raise ValueError(f"{name} must be in range [0, 1], got {value}")
    return value


def load_config(config_path: Path = CONFIG_PATH) -> Lab3Config:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    kafka = KafkaConfig(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            data["kafka"]["bootstrap_servers"],
        ),
        topic=os.getenv("KAFKA_TOPIC", data["kafka"]["topic"]),
        group_id=os.getenv("KAFKA_GROUP_ID", data["kafka"]["group_id"]),
    )

    producer = ProducerConfig(
        events_count=int(os.getenv("LAB3_EVENTS_COUNT", data["producer"]["events_count"])),
        send_delay_seconds=float(data["producer"]["send_delay_seconds"]),
        buffer_size=int(data["producer"]["buffer_size"]),
        out_of_order_probability=_validate_probability(
            "out_of_order_probability",
            float(data["producer"]["out_of_order_probability"]),
        ),
        late_probability=_validate_probability(
            "late_probability",
            float(data["producer"]["late_probability"]),
        ),
        late_release_every=int(data["producer"]["late_release_every"]),
        event_interval_ms=int(data["producer"]["event_interval_ms"]),
        user_id_min=int(data["producer"]["user_id_min"]),
        user_id_max=int(data["producer"]["user_id_max"]),
        event_types=tuple(data["producer"]["event_types"]),
    )

    if producer.buffer_size < 1:
        raise ValueError("buffer_size must be >= 1")
    if producer.late_release_every < 1:
        raise ValueError("late_release_every must be >= 1")
    if producer.event_interval_ms < 1:
        raise ValueError("event_interval_ms must be >= 1")
    if producer.events_count < 1:
        raise ValueError("events_count must be >= 1")
    if producer.user_id_min > producer.user_id_max:
        raise ValueError("user_id_min must be <= user_id_max")
    if not producer.event_types:
        raise ValueError("event_types must not be empty")

    flink = FlinkConfig(
        window_size_seconds=int(data["flink"]["window_size_seconds"]),
        watermark_out_of_orderness_seconds=int(
            data["flink"]["watermark_out_of_orderness_seconds"]
        ),
        allowed_lateness_seconds=int(data["flink"]["allowed_lateness_seconds"]),
    )

    if flink.window_size_seconds < 1:
        raise ValueError("window_size_seconds must be >= 1")
    if flink.watermark_out_of_orderness_seconds < 0:
        raise ValueError("watermark_out_of_orderness_seconds must be >= 0")
    if flink.allowed_lateness_seconds < 0:
        raise ValueError("allowed_lateness_seconds must be >= 0")

    return Lab3Config(kafka=kafka, producer=producer, flink=flink)

