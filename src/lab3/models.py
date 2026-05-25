"""Domain models for Lab 3 events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json


@dataclass(frozen=True)
class StreamEvent:
    event_id: str
    user_id: str
    event_type: str
    event_time: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "event_id": self.event_id,
                "user_id": self.user_id,
                "event_type": self.event_type,
                "event_time": self.event_time,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @staticmethod
    def from_json(payload: str) -> "StreamEvent":
        data = json.loads(payload)
        return StreamEvent(
            event_id=str(data["event_id"]),
            user_id=str(data["user_id"]),
            event_type=str(data["event_type"]),
            event_time=str(data["event_time"]),
        )

    def event_time_millis(self) -> int:
        return parse_event_time_to_millis(self.event_time)


def parse_event_time_to_millis(event_time: str) -> int:
    """Parse ISO-8601 timestamp to epoch milliseconds."""
    normalized = event_time.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def format_event_time(dt: datetime) -> str:
    """Format datetime in RFC3339 UTC for Kafka payload."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    utc_dt = dt.astimezone(UTC)
    return utc_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

