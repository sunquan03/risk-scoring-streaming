import confluent_kafka
import os







class Producer:
    def __init__(self, topic: str, config: dict):
        self.topic = topic

        self.producer = confluent_kafka.Producer(config)

    def send_message(self, key, value):
        self.producer.produce(self.topic, key=key, value=value)


def main():
    producerConfig =  {"boostrap.servers": os.environ['BOOTSTRAP_SERVERS']}
    producer = Producer('producer', producerConfig)


