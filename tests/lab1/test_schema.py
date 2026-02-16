import math

from src.lab1.schema import csv_row_to_record, deserialize, serialize


SAMPLE_ROW = {
    "InvoiceNo": "536365",
    "StockCode": "85123A",
    "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
    "Quantity": 6,
    "InvoiceDate": "12/1/2010 8:26",
    "UnitPrice": 2.55,
    "CustomerID": 17850.0,
    "Country": "United Kingdom",
}


def test_serialize_deserialize_roundtrip():
    record = csv_row_to_record(SAMPLE_ROW)
    data = serialize(record)
    restored = deserialize(data)

    assert restored["InvoiceNo"] == "536365"
    assert restored["StockCode"] == "85123A"
    assert restored["Description"] == "WHITE HANGING HEART T-LIGHT HOLDER"
    assert restored["Quantity"] == 6
    assert restored["UnitPrice"] == 2.55
    assert restored["CustomerID"] == 17850
    assert restored["Country"] == "United Kingdom"


def test_nullable_fields():
    row = {**SAMPLE_ROW, "CustomerID": float("nan"), "Description": float("nan")}
    record = csv_row_to_record(row)

    assert record["CustomerID"] is None
    assert record["Description"] is None

    data = serialize(record)
    restored = deserialize(data)

    assert restored["CustomerID"] is None
    assert restored["Description"] is None


def test_csv_row_to_record_types():
    record = csv_row_to_record(SAMPLE_ROW)

    assert isinstance(record["InvoiceNo"], str)
    assert isinstance(record["Quantity"], int)
    assert isinstance(record["UnitPrice"], float)
    assert isinstance(record["CustomerID"], int)
