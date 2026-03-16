"""Kafka producer for Lab2 using shared E-Commerce Avro format."""

import logging
import signal
import time

import pandas as pd
from confluent_kafka import Producer

from src.common.ecommerce_avro import csv_row_to_record, serialize
from src.lab2.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

_shutdown = False


def _signal_handler(sig, frame):
    del sig, frame
    global _shutdown
    logger.info("Shutdown signal received, finishing current batch...")
    _shutdown = True


def _delivery_callback(err, msg):
    del msg
    if err:
        logger.error("Delivery failed: %s", err)


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 50,
            "batch.num.messages": 1000,
            "compression.type": "snappy",
        }
    )


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
            payload = serialize(record)

            producer.produce(
                topic,
                value=payload,
                key=record["InvoiceNo"].encode("utf-8"),
                callback=_delivery_callback,
            )
            total_sent += 1

        producer.flush()
        logger.info("Sent %d records so far", total_sent)

        if send_delay > 0 and not _shutdown:
            time.sleep(send_delay)

    producer.flush()
    return total_sent


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    config = load_config()
    producer = create_producer(config.kafka.bootstrap_servers)
    logger.info(
        "Starting producer for topic=%s on %s",
        config.kafka.topic,
        config.kafka.bootstrap_servers,
    )
    total = produce_messages(
        producer=producer,
        csv_path=str(config.paths.csv_path),
        topic=config.kafka.topic,
        batch_size=config.producer.batch_size,
        send_delay=config.producer.send_delay_seconds,
    )
    logger.info("Done. Total records sent: %d", total)


if __name__ == "__main__":
    main()
