from src.lab2.config import load_config


def test_checkpoint_defaults_are_in_required_range():
    config = load_config()
    assert 5000 <= config.flink.checkpoint_interval_ms <= 10000
    assert config.flink.max_concurrent_checkpoints >= 1
