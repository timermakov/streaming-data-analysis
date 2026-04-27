"""Shared E-Commerce Avro schema and serialization utilities."""

import io
import json
import math
from typing import Any

import fastavro

AVRO_SCHEMA_DICT = {
    "type": "record",
    "name": "ECommerceTransaction",
    "namespace": "streaming_data_analysis",
    "fields": [
        {"name": "InvoiceNo", "type": "string"},
        {"name": "StockCode", "type": "string"},
        {"name": "Description", "type": ["null", "string"], "default": None},
        {"name": "Quantity", "type": "int"},
        {"name": "InvoiceDate", "type": "string"},
        {"name": "UnitPrice", "type": "double"},
        {"name": "CustomerID", "type": ["null", "long"], "default": None},
        {"name": "Country", "type": "string"},
    ],
}

AVRO_SCHEMA = fastavro.parse_schema(AVRO_SCHEMA_DICT)
AVRO_SCHEMA_JSON = json.dumps(AVRO_SCHEMA_DICT)


def _is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def csv_row_to_record(row: dict[str, Any]) -> dict[str, Any]:
    """Convert a pandas-originated row dict into an Avro-compatible record."""
    customer_id = row.get("CustomerID")
    if customer_id is None or _is_nan(customer_id):
        customer_id = None
    else:
        customer_id = int(customer_id)

    description = row.get("Description")
    if _is_nan(description):
        description = None

    return {
        "InvoiceNo": str(row["InvoiceNo"]),
        "StockCode": str(row["StockCode"]),
        "Description": description,
        "Quantity": int(row["Quantity"]),
        "InvoiceDate": str(row["InvoiceDate"]),
        "UnitPrice": float(row["UnitPrice"]),
        "CustomerID": customer_id,
        "Country": str(row["Country"]),
    }


def serialize(record: dict[str, Any]) -> bytes:
    """Serialize record to Avro bytes without container file headers."""
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, AVRO_SCHEMA, record)
    return buffer.getvalue()


def deserialize(data: bytes) -> dict[str, Any]:
    """Deserialize Avro bytes into a Python dictionary."""
    buffer = io.BytesIO(data)
    return fastavro.schemaless_reader(buffer, AVRO_SCHEMA)
