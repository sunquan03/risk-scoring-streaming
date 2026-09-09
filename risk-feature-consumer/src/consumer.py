import confluent_kafka
import structlog
from aggregator import compute_and_cache
from db import upsert_batch
from transformer import TOPIC_TRANSFORMERS, TransformError
import signal
import time
import json

log = structlog.get_logger()

REJECT_ERRORS = (
    json.JSONDecodeError,
    UnicodeDecodeError,
    TransformError,
    KeyError,
    AttributeError,
    TypeError,
    ValueError,
)


class Consumer:
    def __init__(self, topic_config, pipeline_config, tidb_pool, pg_conn_factory):
        self.topic_config = topic_config
        self.pipeline_config = pipeline_config
        self.tidb_pool = tidb_pool
        self.pg_conn_factory = pg_conn_factory

        self._transformer = TOPIC_TRANSFORMERS.get(topic_config.topic_name)
        if self._transformer is None:
            raise ValueError(f"Topic {topic_config.topic_name} not supported")
        if not topic_config.is_active:
            raise ValueError(f"Topic {topic_config.topic_name} is not active")

        settings = pipeline_config.settings
        self._consumer = confluent_kafka.Consumer({
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": topic_config.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": topic_config.max_poll_interval_ms,
            "session.timeout.ms": topic_config.session_timeout_ms,
        })
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)
        self._running = True
        self._batch_number = 0


    def run(self):
        self._consumer.subscribe([self.topic_config.topic_name])
        log.info("consumer_started", topic=self.topic_config.topic_name,
                 group=self.topic_config.consumer_group)
        try:
            while self._running:
                self._process_batch(self.topic_config.batch_size)
        finally:
            self._consumer.close()
            log.info("consumer_stopped", topic=self.topic_config.topic_name)

    def _process_batch(self, batch_size: int) -> None:
        start_time = time.monotonic()
        rows = []
        rejected = []
        client_ids = set()
        msg_cnt = 0
        last_msg = None

        for _ in range(batch_size):
            if not self._running:
                break
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("kafka_message_error", error=str(msg.error()))
                continue
            last_msg = msg
            msg_cnt += 1
            try:
                m = json.loads(msg.value().decode("utf-8"))
                row = self._transformer(m, msg.offset())
                rows.append(row)
                if row.get("client_id"):
                    client_ids.add(row["client_id"])
            except REJECT_ERRORS as exc:
                rejected.append((
                    self.topic_config.topic_name,
                    msg.partition(),
                    msg.offset(),
                    msg.value().decode("utf-8", errors="replace"),
                    type(exc).__name__,
                    str(exc),
                ))

        if msg_cnt == 0:
            return

        cache_written = 0
        try:
            if rows:
                upsert_batch(self.tidb_pool, self.topic_config.target_table, rows)
            if client_ids:
                agg_stats = compute_and_cache(self.tidb_pool, self.pipeline_config, client_ids)
                cache_written = sum(agg_stats.values())
        except Exception:
            log.exception("batch_flush_failed", topic=self.topic_config.topic_name,
                          batch=self._batch_number, rows=len(rows))
            return

        if rejected:
            self._write_dlq(rejected)

        if last_msg is not None:
            self._consumer.commit(message=last_msg, asynchronous=False)

        self._batch_number += 1
        duration_ms = int((time.monotonic() - start_time) * 1000)
        self._write_run_metrics(duration_ms=duration_ms, consumed=msg_cnt, inserted=len(rows),
                                rejected=len(rejected), cache_written=cache_written)

    def _handle_sigterm(self, signum, frame) -> None:
        self._running = False

    def _write_dlq(self, rejected: list[tuple]) -> None:
        conn = self.pg_conn_factory()
        try:
            with conn.cursor() as cur:
                query = """INSERT INTO pipeline_config.dlq_events
                            (topic_name, kafka_partition, kafka_offset,raw_payload, error_type, error_message)
                            VALUES (%s, %s, %s, %s, %s, %s)"""
                cur.executemany(query, rejected)
            conn.commit()
        except Exception:
            log.exception("dlq_write_failed", topic=self.topic_config.topic_name,
                          rejected=len(rejected))
            conn.rollback()
        finally:
            conn.close()

    def _write_run_metrics(self, consumed, inserted, rejected, cache_written, duration_ms) -> None:
        conn = self.pg_conn_factory()
        try:
            with conn.cursor() as cur:
                query = """INSERT INTO pipeline_config.pipeline_runs
                            (topic_name, batch_number, messages_consumed,messages_inserted, messages_rejected,cache_keys_written, duration_ms, finished_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())"""

                cur.execute(query, (self.topic_config.topic_name, self._batch_number, consumed,
                                    inserted, rejected, cache_written, duration_ms,))
            conn.commit()
        except Exception:
            log.exception("metrics_write_failed", topic=self.topic_config.topic_name)
            conn.rollback()
        finally:
            conn.close()
