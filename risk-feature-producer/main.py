import confluent_kafka
import os

TOPIC_APPLICATIONS = "loan-applications"
TOPIC_PAYMENTS = "loan-payments"
TOPIC_OPERATIONS = "loan-operations"
TOPIC_MONEY = "client-money"

KZ_CITIES = [
    "Almaty", "Astana", "Shymkent", "Karaganda", "Aktobe", "Taraz",
    "Pavlodar", "Ust-Kamenogorsk", "Semey", "Atyrau", "Kostanay",
    "Kyzylorda", "Oral", "Petropavl", "Aktau",
]
PRODUCTS = ["CONSUMER_LOAN", "MORTGAGE", "MICRO", "AUTO_LOAN", "CREDIT_CARD"]
CHANNELS = ["MOBILE_APP", "WEB", "BRANCH", "KASPI_PAY", "CALL_CENTER"]
PAY_CHANNELS = ["KASPI_PAY", "BANK_TRANSFER", "CASH", "AUTO_DEBIT", "MOBILE_APP"]
DEVICE_TYPES = ["IOS", "ANDROID", "WEB", "POS"]
SOURCE_SYSTEMS = ["KASPI_BANK", "HALYK", "JYSAN", "FORTE", "BEREKE"]
PURPOSES = ["HOME_REPAIR", "EDUCATION", "MEDICAL", "TRAVEL", "BUSINESS", None]
REJECTION_REASONS = [
    "LOW_SCORE", "HIGH_DTI", "EXISTING_OVERDUE",
    "FRAUD_RISK", "INCOME_MISMATCH", "AGE_LIMIT",
]

PAY_EVENT_TYPES = ["PAYMENT", "PARTIAL_PAYMENT", "MISSED", "LATE_FEE", "PENALTY", "RESTRUCTURE"]
PAY_EVENT_WEIGHTS = [50, 15, 15, 8, 7, 5]

OPERATION_TYPES = ["DISBURSEMENT", "TOPUP", "EARLY_CLOSE", "REFINANCE", "WRITE_OFF", "TRANSFER"]
OPERATION_WEIGHTS = [25, 25, 15, 15, 5, 15]

MONEY_EVENT_TYPES = [
    "MONTH_END_BALANCE", "SALARY_CREDIT", "TRANSFER_IN",
    "TRANSFER_OUT", "LARGE_DEBIT", "LARGE_CREDIT",
]
MONEY_EVENT_WEIGHTS = [5, 25, 25, 25, 10, 10]

TOPIC_WEIGHTS = {
    TOPIC_APPLICATIONS: 8,
    TOPIC_PAYMENTS: 46,
    TOPIC_OPERATIONS: 11,
    TOPIC_MONEY: 35,
}


class Producer:
    def __init__(self, topic: str, config: dict):
        self.topic = topic

        self.producer = confluent_kafka.Producer(config)

    def send_message(self, key, value):
        self.producer.produce(self.topic, key=key, value=value)


def main():
    producerConfig =  {"boostrap.servers": os.environ['BOOTSTRAP_SERVERS']}
    producer = Producer('producer', producerConfig)

    while True:
