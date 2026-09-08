import confluent_kafka
import os
from config import Config
from generator import EntityPool, TOPIC_WEIGHTS
from datetime import time





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

    def run(self):
        interval = self._config.interval_ms / 1000.0
        next_due = time.monotonic()
        while self._running:

            if self._config.max_events:
                if self._config.max_events > self._sent:
                    self._running = False
                    break



            self._producer.poll(0.1)


def main():
    producerConfig =  {"boostrap.servers": os.environ['BOOTSTRAP_SERVERS']}
    producer = Producer('producer', producerConfig)


