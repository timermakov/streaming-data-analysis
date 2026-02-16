"""Kafka Producer: reads CSV and sends Avro-serialized records to Kafka."""

import argparse
import logging
import signal
import sys
import time

import pandas as pd
from confluent_kafka import KafkaError, Producer

from src.lab1.config import (
    CSV_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_BATCH_SIZE,
    PRODUCER_SEND_DELAY,
)
from src.lab1.schema import csv_row_to_record, serialize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

_shutdown = False


def _signal_handler(sig, frame):
    global _shutdown
    logger.info("Shutdown signal received, finishing current batch...")
    _shutdown = True


def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 50,
        "batch.num.messages": 1000,
        "compression.type": "snappy",
    })


def produce_messages(
    producer: Producer,
    csv_path: str,
    topic: str,
    batch_size: int,
    send_delay: float,
) -> int:
    total_sent = 0

    for chunk in pd.read_csv(csv_path, chunksize=batch_size, encoding="latin-1"):
        if _shutdown:
            break

        for _, row in chunk.iterrows():
            if _shutdown:
                break

            record = csv_row_to_record(row.to_dict())
            data = serialize(record)

            producer.produce(
                topic,
                value=data,
                key=record["InvoiceNo"].encode("utf-8"),
                callback=delivery_callback,
            )
            total_sent += 1

        producer.flush()
        logger.info("Sent %d records so far", total_sent)

        if send_delay > 0 and not _shutdown:
            time.sleep(send_delay)

    producer.flush()
    return total_sent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka Avro Producer for E-Commerce data")
    parser.add_argument("--csv-path", type=str, default=str(CSV_PATH))
    parser.add_argument("--bootstrap-servers", type=str, default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", type=str, default=KAFKA_TOPIC)
    parser.add_argument("--batch-size", type=int, default=PRODUCER_BATCH_SIZE)
    parser.add_argument("--delay", type=float, default=PRODUCER_SEND_DELAY)
    return parser.parse_args()


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    args = parse_args()

    logger.info(
        "Starting producer: topic=%s, servers=%s, batch=%d, delay=%.2fs",
        args.topic, args.bootstrap_servers, args.batch_size, args.delay,
    )

    producer = create_producer(args.bootstrap_servers)

    total = produce_messages(
        producer=producer,
        csv_path=args.csv_path,
        topic=args.topic,
        batch_size=args.batch_size,
        send_delay=args.delay,
    )

    logger.info("Done. Total records sent: %d", total)


if __name__ == "__main__":
    main()
