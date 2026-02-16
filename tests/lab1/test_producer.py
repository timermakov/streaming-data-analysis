from unittest.mock import MagicMock, patch, call
import tempfile
import os

from src.lab1.producer import produce_messages, create_producer


CSV_CONTENT = (
    "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
    "536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom\n"
    "536366,22633,HAND WARMER UNION JACK,6,12/1/2010 8:28,1.85,17850,United Kingdom\n"
)


def test_produce_messages_sends_all_rows():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write(CSV_CONTENT)
        csv_path = f.name

    try:
        mock_producer = MagicMock()
        total = produce_messages(
            producer=mock_producer,
            csv_path=csv_path,
            topic="test-topic",
            batch_size=100,
            send_delay=0,
        )

        assert total == 2
        assert mock_producer.produce.call_count == 2
        assert mock_producer.flush.call_count >= 1

        first_call = mock_producer.produce.call_args_list[0]
        assert first_call.args[0] == "test-topic"
        assert first_call.kwargs["key"] == b"536365"
        assert isinstance(first_call.kwargs["value"], bytes)
    finally:
        os.unlink(csv_path)


def test_produce_messages_respects_batch_size():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write(CSV_CONTENT)
        csv_path = f.name

    try:
        mock_producer = MagicMock()
        total = produce_messages(
            producer=mock_producer,
            csv_path=csv_path,
            topic="test-topic",
            batch_size=1,
            send_delay=0,
        )

        assert total == 2
        # batch_size=1 → 2 chunks → flush called at least twice (plus final)
        assert mock_producer.flush.call_count >= 2
    finally:
        os.unlink(csv_path)
