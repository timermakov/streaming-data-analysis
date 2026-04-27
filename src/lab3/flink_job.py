"""PyFlink DataStream job for Lab3 event-time tumbling windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging

from pyflink.common import Duration, RuntimeExecutionMode, Time, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TimeWindow, TumblingEventTimeWindows

from src.lab3.config import Lab3Config, load_config
from src.lab3.models import parse_event_time_to_millis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedEvent:
    event_id: str
    user_id: str
    event_type: str
    event_time: str
    event_time_millis: int

    @staticmethod
    def from_payload(payload: str) -> "ParsedEvent":
        data = json.loads(payload)
        event_time = str(data["event_time"])
        return ParsedEvent(
            event_id=str(data["event_id"]),
            user_id=str(data["user_id"]),
            event_type=str(data["event_type"]),
            event_time=event_time,
            event_time_millis=parse_event_time_to_millis(event_time),
        )


def _millis_to_utc_iso(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ParsedEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: ParsedEvent, record_timestamp: int) -> int:
        del record_timestamp
        return value.event_time_millis


class WindowCountProcessFunction(ProcessWindowFunction):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements,
    ):
        del key
        count = 0
        for _ in elements:
            count += 1
        payload = {
            "window_start": _millis_to_utc_iso(context.window().start),
            "window_end": _millis_to_utc_iso(context.window().end),
            "count": count,
        }
        yield json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def create_environment(config: Lab3Config) -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(5000)
    logger.info(
        "Flink environment: window=%ss watermark_lag=%ss allowed_lateness=%ss",
        config.flink.window_size_seconds,
        config.flink.watermark_out_of_orderness_seconds,
        config.flink.allowed_lateness_seconds,
    )
    return env


def create_source(config: Lab3Config) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.kafka.bootstrap_servers)
        .set_topics(config.kafka.topic)
        .set_group_id(config.kafka.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_pipeline(env: StreamExecutionEnvironment, config: Lab3Config) -> None:
    source = create_source(config)
    watermark = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(config.flink.watermark_out_of_orderness_seconds)
        ).with_timestamp_assigner(ParsedEventTimestampAssigner())
    )

    stream = env.from_source(
        source=source,
        watermark_strategy=watermark,
        source_name="lab3-kafka-source",
    )
    parsed_events = stream.map(lambda payload: ParsedEvent.from_payload(payload))

    late_events_tag = OutputTag("late-events", Types.PICKLED_BYTE_ARRAY())
    windowed = (
        parsed_events.key_by(lambda _: "all-events")
        .window(TumblingEventTimeWindows.of(Time.seconds(config.flink.window_size_seconds)))
        .allowed_lateness(Time.seconds(config.flink.allowed_lateness_seconds))
        .side_output_late_data(late_events_tag)
    )

    counts = windowed.process(WindowCountProcessFunction(), output_type=Types.STRING())
    counts.print()

    late_events = counts.get_side_output(late_events_tag)
    (
        late_events.map(
            lambda event: (
                "late-event:"
                f"event_id={event.event_id},"
                f"user_id={event.user_id},"
                f"type={event.event_type},"
                f"event_time={event.event_time}"
            ),
            output_type=Types.STRING(),
        ).print()
    )


def main() -> None:
    config = load_config()
    env = create_environment(config)
    build_pipeline(env, config)
    env.execute("lab3-event-time-window-count")


if __name__ == "__main__":
    main()

