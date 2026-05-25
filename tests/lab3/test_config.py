import tempfile
from pathlib import Path

import pytest

from src.lab3.config import load_config


def test_load_config_defaults():
    config = load_config()
    assert config.kafka.topic == "lab3-events"
    assert 0 <= config.producer.out_of_order_probability <= 1
    assert 0 <= config.producer.late_probability <= 1
    assert config.flink.window_size_seconds > 0


def test_load_config_rejects_invalid_probability():
    bad_config = """
kafka:
  bootstrap_servers: localhost:9092
  topic: lab3-events
  group_id: lab3-flink-consumer
producer:
  events_count: 10
  send_delay_seconds: 0.0
  buffer_size: 5
  out_of_order_probability: 1.5
  late_probability: 0.1
  late_release_every: 2
  event_interval_ms: 1000
  user_id_min: 1
  user_id_max: 3
  event_types: [click]
flink:
  window_size_seconds: 10
  watermark_out_of_orderness_seconds: 3
  allowed_lateness_seconds: 2
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as f:
        f.write(bad_config)
        path = Path(f.name)

    try:
        with pytest.raises(ValueError):
            load_config(path)
    finally:
        path.unlink(missing_ok=True)

