from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.lab2.verify_parquet import count_parquet_rows


def test_count_parquet_rows_raises_if_path_missing():
    with pytest.raises(FileNotFoundError):
        count_parquet_rows(Path("missing-parquet-path"))


@patch("src.lab2.verify_parquet.create_spark_session")
def test_count_parquet_rows_returns_count(mock_create_spark_session, tmp_path):
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    spark = MagicMock()
    mock_create_spark_session.return_value = spark
    spark.read.parquet.return_value.count.return_value = 42

    total = count_parquet_rows(parquet_dir)
    assert total == 42

    spark.read.parquet.assert_called_once_with(str(parquet_dir))
    spark.stop.assert_called_once()
