import json
import signal
import sys
import uuid

import confluent_kafka
import os
from config import Config
from generator import EntityPool, TOPIC_WEIGHTS, GENERATORS, build_entry_pool
import time
import random
import logging


logger = logging.getLogger("producer")



class Producer:
    def __init__(self, config: Config, pool: EntityPool):
        self._config = config
        self._pool = pool
        self._running = True
        self._sent = 0
        self._malformed = 0
        self._failed = 0
        producerConf = {
            "bootstrap.servers": config.kafka_bootstrap,
            "linger.ms": 5,
            "acks": "all",
            "enable.idempotence": True,
        }
        if config.kafka_security_protocol != "PLAINTEXT":
            producerConf["security.protocol"] = config.kafka_security_protocol
        if config.kafka_sasl_mechanism:
            producerConf["sasl.mechanism"] = config.kafka_sasl_mechanism
            producerConf["sasl.username"] = config.kafka_sasl_username
            producerConf["sasl.password"] = config.kafka_sasl_password

        self._producer = confluent_kafka.Producer(producerConf)

        self._weights = [TOPIC_WEIGHTS[t] for t in config.topics]
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)

    def handle_signal(self, signum, frame):
        logger.warning("Received shutdown signal %s", signum)
        self._running = False

    def check_delivery(self, err, msg):
        if err is not None:
            self._failed += 1
            logger.error("Delivery fail: %s %s", msg.topic(), err)

    def send_event(self):
        topic = random.choices(self._config.topics, weights=self._weights)[0]
        event = GENERATORS[topic](self._pool)

        payload = json.dumps(event).encode("utf-8")
        key = str(event.get("client_id", "")).encode("utf-8")

        if random.random() < self._config.malformed_rate:
            # random malformed data
            # todo - invalidate on purpose and then send
            self._malformed += 1
            return

        self._producer.produce(topic=topic, key=key, value=payload, on_delivery=self.check_delivery)


    def run(self):
        interval = self._config.interval_ms / 1000.0
        next_due = time.monotonic()
        try:
            while self._running:

                if self._config.max_events:
                    if self._config.max_events > self._sent:
                        break

                self.send_event()

                next_due = next_due + interval
                sleep_for = next_due - time.monotonic()

                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    next_due = time.monotonic()


                self._producer.poll(0.1)
                self._sent += 1
        finally:
            self._producer.flush()
            self._sent += 1


def main():
    cfg = Config()
    pool = build_entry_pool(cfg.seed, cfg.client_pool_size)

    Producer(cfg, pool).run()


if __name__ == "__main__":
    main()


