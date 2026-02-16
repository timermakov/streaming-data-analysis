import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-transactions")

CSV_PATH = PROJECT_ROOT / "data" / "lab1" / "E-Commerce Data.csv"
OUTPUT_PATH = PROJECT_ROOT / "output" / "lab1"
CHECKPOINT_PATH = PROJECT_ROOT / "checkpoints" / "lab1"

PRODUCER_BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "500"))
PRODUCER_SEND_DELAY = float(os.getenv("PRODUCER_SEND_DELAY", "0.1"))
