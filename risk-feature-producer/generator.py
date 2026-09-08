import random

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

ALL_TOPICS = [TOPIC_APPLICATIONS, TOPIC_OPERATIONS, TOPIC_PAYMENTS, TOPIC_MONEY]

class EntityPool:
    client_idns: list[str]
    loans_per_client: dict[str, list[str]]
    source: str

    def __init__(self, client_idns: list[str], loans_per_client: dict[str, list[str]]):
        self.client_idns = client_idns
        self.loans_per_client = loans_per_client

    def random_client(self):
        return random.choice(self.client_idns)

    def random_client_with_loans(self) -> tuple[str, str]:
        for _ in range(20):
            client_id = random.choice(self.client_idns)
            loans = self.loans_per_client.get(client_id)
            if loans:
                return client_id, random.choice(loans)


def build_entry_pool(seed: int, n_cli: int) -> EntityPool:
    rng = random.Random(seed)
    client_idns = ["".join(str(rng.randint(0, 9)) for _ in range(12)) for _ in range(n_cli)]
    loans_per_client = {cli_idn: [f"loan-{i:05d}-{j}" for j in range(rng.randint(1, 4))] for i, cli_idn in enumerate(client_idns)}
    return EntityPool(client_idns, loans_per_client)



