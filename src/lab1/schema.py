import io
import json
import math

import fastavro

AVRO_SCHEMA_DICT = {
    "type": "record",
    "name": "ECommerceTransaction",
    "namespace": "lab1",
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


def csv_row_to_record(row: dict) -> dict:
    """Convert a pandas-originated dict row to an Avro-compatible record."""
    customer_id = row.get("CustomerID")
    if customer_id is None or (isinstance(customer_id, float) and math.isnan(customer_id)):
        customer_id = None
    else:
        customer_id = int(customer_id)

    description = row.get("Description")
    if isinstance(description, float) and math.isnan(description):
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


def serialize(record: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, AVRO_SCHEMA, record)
    return buf.getvalue()


def deserialize(data: bytes) -> dict:
    buf = io.BytesIO(data)
    return fastavro.schemaless_reader(buf, AVRO_SCHEMA)
