from datetime import UTC, datetime
import random

from src.lab3.config import ProducerConfig
from src.lab3.models import StreamEvent, format_event_time, parse_event_time_to_millis
from src.lab3.producer import create_dispatch_plan


def _build_events(size: int) -> list[StreamEvent]:
    base_time = datetime(2026, 1, 1, tzinfo=UTC)
    events = []
    for idx in range(size):
        events.append(
            StreamEvent(
                event_id=f"evt-{idx}",
                user_id="42",
                event_type="click",
                event_time=format_event_time(base_time),
            )
        )
    return events


def _producer_cfg(**overrides) -> ProducerConfig:
    base = ProducerConfig(
        events_count=12,
        send_delay_seconds=0.0,
        buffer_size=4,
        out_of_order_probability=0.0,
        late_probability=0.0,
        late_release_every=3,
        event_interval_ms=1000,
        user_id_min=1,
        user_id_max=10,
        event_types=("click", "view"),
    )
    return ProducerConfig(**(base.__dict__ | overrides))


class SequenceRng:
    def __init__(self, values: list[float]):
        self._values = values
        self._idx = 0

    def random(self) -> float:
        if self._idx >= len(self._values):
            return self._values[-1]
        value = self._values[self._idx]
        self._idx += 1
        return value


def test_create_dispatch_plan_normal_keeps_order():
    events = _build_events(10)
    plan = create_dispatch_plan(
        events=events,
        mode="normal",
        producer_cfg=_producer_cfg(),
        rng=random.Random(7),
    )
    assert plan == [event.event_id for event in events]


def test_create_dispatch_plan_out_of_order_shuffles_sequence():
    events = _build_events(12)
    plan = create_dispatch_plan(
        events=events,
        mode="out_of_order",
        producer_cfg=_producer_cfg(out_of_order_probability=1.0),
        rng=random.Random(11),
    )
    original_ids = [event.event_id for event in events]
    assert len(plan) == len(original_ids)
    assert set(plan) == set(original_ids)
    assert plan != original_ids


def test_create_dispatch_plan_late_mode_delays_subset():
    events = _build_events(12)
    # First two events are marked as late, next events are regular.
    rng = SequenceRng([0.1, 0.1, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9])
    plan = create_dispatch_plan(
        events=events,
        mode="late",
        producer_cfg=_producer_cfg(
            out_of_order_probability=0.0,
            late_probability=0.5,
            late_release_every=2,
        ),
        rng=rng,
    )
    assert len(plan) == len(events)
    assert set(plan) == {event.event_id for event in events}
    assert plan.index("evt-0") > plan.index("evt-2")
    assert plan.index("evt-1") > plan.index("evt-3")


def test_parse_event_time_to_millis_handles_utc_rfc3339():
    timestamp = "2026-03-02T10:15:30.123Z"
    millis = parse_event_time_to_millis(timestamp)
    assert millis == 1772446530123

