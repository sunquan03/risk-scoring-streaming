import json
import uuid

import confluent_kafka
import os
from config import Config
from generator import EntityPool, TOPIC_WEIGHTS, GENERATORS
import time
import random




class Producer:
    def __init__(self, config: Config, pool: EntityPool):
        self._config = config
        self._pool = pool
        self._running = True
        self._sent = 0
        producerConf = {
            "bootstrap.servers": config.kafka_bootstrap,
            "linger.ms": 5,
            "acks": "all",
            "enable.idempotence": True,
        }
        if config.kafka_sasl_mechanism:
            producerConf["sasl.mechanism"] = config.kafka_sasl_mechanism
            producerConf["sasl.username"] = config.kafka_sasl_username
            producerConf["sasl.password"] = config.kafka_sasl_password

        self._producer = confluent_kafka.Producer(producerConf)

        self._weights = [TOPIC_WEIGHTS[t] for t in config.topics]

    def send_event(self):
        topic = random.choice(self._config.topics, weights=self._weights)[0]
        event = GENERATORS[topic](self._pool)

        payload = json.dumps(event).encode("utf-8")

        key = str(event.get("client_id", "")).encode("utf-8")

        self._producer.produce(topic=topic, key=key, value=payload)


    def run(self):
        interval = self._config.interval_ms / 1000.0
        next_due = time.monotonic()
        while self._running:

            if self._config.max_events:
                if self._config.max_events > self._sent:
                    self._running = False
                    break

            self.send_event()

            next_due = next_due + interval
            sleep_for = next_due - time.monotonic()

            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_due = time.monotonic()


            self._producer.poll(0.1)


def main():
    producerConfig =  {"boostrap.servers": os.environ['BOOTSTRAP_SERVERS']}
    producer = Producer('producer', producerConfig)


