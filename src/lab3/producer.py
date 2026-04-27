"""Kafka producer for Lab3 with normal, out-of-order and late modes."""

from __future__ import annotations

import argparse
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
import random
import signal
import time
from typing import Any
from typing import Iterable, Literal
from uuid import uuid4

try:
    from confluent_kafka import Producer as KafkaProducer
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    KafkaProducer = Any  # type: ignore[assignment]

from src.lab3.config import Lab3Config, ProducerConfig, load_config
from src.lab3.models import StreamEvent, format_event_time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ProducerMode = Literal["normal", "out_of_order", "late"]
_shutdown = False


def _signal_handler(sig: int, frame) -> None:
    del sig, frame
    global _shutdown
    logger.info("Shutdown signal received, finishing buffered messages...")
    _shutdown = True


def _delivery_callback(err, msg) -> None:
    del msg
    if err:
        logger.error("Delivery failed: %s", err)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    if KafkaProducer is Any:
        raise RuntimeError(
            "confluent_kafka is not installed. Install dependencies with `poetry install`."
        )

    return KafkaProducer(
        {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 25,
            "batch.num.messages": 500,
            "compression.type": "snappy",
        }
    )


@dataclass
class DispatchState:
    regular_buffer: list[StreamEvent]
    delayed_buffer: deque[StreamEvent]
    emitted_since_late_release: int


def create_event(index: int, producer_cfg: ProducerConfig, rng: random.Random) -> StreamEvent:
    event_dt = datetime.now(UTC) + timedelta(milliseconds=index * producer_cfg.event_interval_ms)
    event = StreamEvent(
        event_id=str(uuid4()),
        user_id=str(rng.randint(producer_cfg.user_id_min, producer_cfg.user_id_max)),
        event_type=rng.choice(producer_cfg.event_types),
        event_time=format_event_time(event_dt),
    )
    return event


def _pop_from_regular_buffer(
    state: DispatchState,
    mode: ProducerMode,
    out_of_order_probability: float,
    rng: random.Random,
) -> StreamEvent | None:
    if not state.regular_buffer:
        return None

    if (
        mode == "out_of_order" or mode == "late"
    ) and len(state.regular_buffer) > 1 and rng.random() < out_of_order_probability:
        idx = rng.randrange(1, len(state.regular_buffer))
        return state.regular_buffer.pop(idx)

    return state.regular_buffer.pop(0)


def _pop_next_event(
    state: DispatchState,
    mode: ProducerMode,
    producer_cfg: ProducerConfig,
    rng: random.Random,
) -> StreamEvent | None:
    if (
        mode == "late"
        and state.delayed_buffer
        and state.emitted_since_late_release >= producer_cfg.late_release_every
    ):
        state.emitted_since_late_release = 0
        return state.delayed_buffer.popleft()

    return _pop_from_regular_buffer(
        state=state,
        mode=mode,
        out_of_order_probability=producer_cfg.out_of_order_probability,
        rng=rng,
    )


def _dispatch_event(
    producer: KafkaProducer,
    topic: str,
    event: StreamEvent,
) -> None:
    producer.produce(
        topic=topic,
        key=event.user_id.encode("utf-8"),
        value=event.to_json().encode("utf-8"),
        callback=_delivery_callback,
    )


def _assign_to_buffers(
    event: StreamEvent,
    state: DispatchState,
    mode: ProducerMode,
    producer_cfg: ProducerConfig,
    rng: random.Random,
) -> None:
    if mode == "late" and rng.random() < producer_cfg.late_probability:
        state.delayed_buffer.append(event)
        return
    state.regular_buffer.append(event)


def _flush_buffers(
    producer: KafkaProducer,
    topic: str,
    state: DispatchState,
    mode: ProducerMode,
    producer_cfg: ProducerConfig,
    rng: random.Random,
    sent_total: int,
) -> int:
    while state.regular_buffer or state.delayed_buffer:
        if _shutdown:
            break
        event = _pop_next_event(
            state=state,
            mode=mode,
            producer_cfg=producer_cfg,
            rng=rng,
        )
        if event is None and state.delayed_buffer:
            event = state.delayed_buffer.popleft()
        if event is None:
            continue

        _dispatch_event(producer, topic, event)
        state.emitted_since_late_release += 1
        sent_total += 1

    producer.flush()
    return sent_total


def produce_messages(
    producer: KafkaProducer,
    config: Lab3Config,
    mode: ProducerMode,
    rng: random.Random | None = None,
) -> int:
    rng = rng or random.Random()
    producer_cfg = config.producer
    state = DispatchState(regular_buffer=[], delayed_buffer=deque(), emitted_since_late_release=0)

    sent_total = 0
    logger.info(
        "Start producer mode=%s topic=%s events=%d",
        mode,
        config.kafka.topic,
        producer_cfg.events_count,
    )

    for idx in range(producer_cfg.events_count):
        if _shutdown:
            break

        event = create_event(index=idx, producer_cfg=producer_cfg, rng=rng)
        _assign_to_buffers(event, state, mode, producer_cfg, rng)

        if len(state.regular_buffer) >= producer_cfg.buffer_size:
            next_event = _pop_next_event(
                state=state,
                mode=mode,
                producer_cfg=producer_cfg,
                rng=rng,
            )
            if next_event is not None:
                _dispatch_event(producer, config.kafka.topic, next_event)
                state.emitted_since_late_release += 1
                sent_total += 1

        if (idx + 1) % producer_cfg.buffer_size == 0:
            producer.poll(0)

        if producer_cfg.send_delay_seconds > 0:
            time.sleep(producer_cfg.send_delay_seconds)

    sent_total = _flush_buffers(
        producer=producer,
        topic=config.kafka.topic,
        state=state,
        mode=mode,
        producer_cfg=producer_cfg,
        rng=rng,
        sent_total=sent_total,
    )
    logger.info("Done producer mode=%s sent=%d", mode, sent_total)
    return sent_total


def create_dispatch_plan(
    events: Iterable[StreamEvent],
    mode: ProducerMode,
    producer_cfg: ProducerConfig,
    rng: random.Random,
) -> list[str]:
    state = DispatchState(regular_buffer=[], delayed_buffer=deque(), emitted_since_late_release=0)
    emitted_ids: list[str] = []

    for event in events:
        _assign_to_buffers(event, state, mode, producer_cfg, rng)
        if len(state.regular_buffer) >= producer_cfg.buffer_size:
            next_event = _pop_next_event(state, mode, producer_cfg, rng)
            if next_event is not None:
                emitted_ids.append(next_event.event_id)
                state.emitted_since_late_release += 1

    while state.regular_buffer or state.delayed_buffer:
        next_event = _pop_next_event(state, mode, producer_cfg, rng)
        if next_event is None and state.delayed_buffer:
            next_event = state.delayed_buffer.popleft()
        if next_event is None:
            continue
        emitted_ids.append(next_event.event_id)
        state.emitted_since_late_release += 1

    return emitted_ids


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lab3 Kafka producer with timing disorder modes.")
    parser.add_argument(
        "--mode",
        choices=["normal", "out_of_order", "late"],
        default="late",
        help="Event dispatch mode",
    )
    return parser.parse_args()


def main() -> None:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    args = parse_args()
    config = load_config()
    producer = create_producer(config.kafka.bootstrap_servers)
    produce_messages(
        producer=producer,
        config=config,
        mode=args.mode,
    )


if __name__ == "__main__":
    main()

