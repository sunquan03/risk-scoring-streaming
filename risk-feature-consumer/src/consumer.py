import confluent_kafka
from config import settings
from src.aggregator import compute_and_cache
from src.db import upsert_batch
from transformer import TOPIC_TRANSFORMERS
import signal
import time
import json
from json import JSONDecodeError
from transformer import TransformError


class Consumer:
    def __init__(self, topic_config, pipeline_config, tidb_pool, pg_conn_factory):
        self.topic_config = topic_config
        self.pipeline_config = pipeline_config
        self.tidb_pool = tidb_pool
        self.pg_conn_factory = pg_conn_factory
        if not TOPIC_TRANSFORMERS[topic_config.topic_name]:
            raise ValueError(f"Topic {topic_config.topic_name} not supported")
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
        self._transformer = TOPIC_TRANSFORMERS.get(topic_config.topic_name)


    def run(self):
        self._consumer.subscribe([self.topic_config.topic_name])
        try:
            while self._running:
                self._process_batch(self.topic_config.batch_size)
        finally:
            self._consumer.close()

    def _process_batch(self, batch_size: int) -> None:
        start_time = time.monotonic()
        rows = []
        rejected = []
        client_ids = []
        msg_cnt = 0
        for _ in range(batch_size):
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            last_msg = msg
            msg_cnt += 1
            try:
                m = json.loads(msg.value().decode('utf-8'))
                row = self._transformer(m, msg.offset())
                rows.append(row)
                if "client_id" in row:
                    client_ids.append(row["client_id"])
            except (json.JSONDecodeError, TransformError, UnicodeDecodeError) as exc:
                rejected.append((
                    self._topic_cfg.topic_name,
                    msg.partition(),
                    msg.offset(),
                    msg.value().decode("utf-8", errors="replace"),
                    type(exc).__name__,
                    str(exc),
                ))
            if rows:
                try:
                    msg_inserted = upsert_batch(self.tidb_pool, self.topic_config.target_table, rows)
                except Exception as exc:
                    return

            if client_ids and rows:
                agg_stats = compute_and_cache(self.tidb_pool, self.pipeline_config, client_ids)
                cache_keys = sum(agg_stats.values())

            if rejected:
                self._write_dlq(rejected)

            if last_msg and (rows or rejected):
                self._consumer.commit(message=last_msg, asynchronous=False)

            #metrics
            self._batch_number += 1
            duration_ms = int((time.monotonic() - start_time) * 1000)
            self._write_run_metrics(duration_ms=duration_ms, consumed=msg_cnt, inserted=len(rows), rejected=len(rejected), cache_written=len(cache_keys))

    def _handle_sigterm(self, signum, frame) -> None:
        self._running = False

    def _write_dlq(self, rejected: list[tuple]) -> None:
        pass

    def _write_run_metrics(self, consumed, inserted, rejected, cache_written, duration_ms) -> None:
        pass