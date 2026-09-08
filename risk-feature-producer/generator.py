import random
from datetime import date, timedelta, datetime, timezone
import uuid

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


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _recent(max_minutes: int = 60) -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, max_minutes))

def _money(lo: float, hi: float, step: float = 1000.0) -> float:
    return float(random.randint(int(lo / step), int(hi / step)) * step)


def gen_loan_application(pool: EntityPool) -> dict:
    status = random.choices(
        ["APPROVED", "REJECTED", "PENDING", "CANCELLED"],
        weights=[45, 35, 15, 5],
    )[0]
    requested = _money(50_000, 5_000_000)
    applied_at = _recent()
    decided = status != "PENDING"

    return {
        "application_id": str(uuid.uuid4()),
        "client_id": pool.random_client(),
        "product_code": random.choice(PRODUCTS),
        "requested_amount": requested,
        "approved_amount": round(requested * random.uniform(0.7, 1.0), 2) if status == "APPROVED" else None,
        "term_months": random.choice([6, 12, 18, 24, 36, 60]),
        "annual_rate": round(random.uniform(0.14, 0.32), 4),
        "purpose": random.choice(PURPOSES),
        "channel": random.choice(CHANNELS),
        "status": status,
        "rejection_reason": random.choice(REJECTION_REASONS) if status == "REJECTED" else None,
        "score_at_decision": round(random.uniform(300, 850), 2) if decided else None,
        "city": random.choice(KZ_CITIES),
        "applied_at": _iso(applied_at),
        "decided_at": _iso(applied_at + timedelta(minutes=random.randint(1, 45))) if decided else None,
    }


def gen_loan_payment(pool: EntityPool) -> dict:
    client_id, loan_id = pool.random_client_with_loan()
    event_type = random.choices(PAY_EVENT_TYPES, weights=PAY_EVENT_WEIGHTS)[0]

    scheduled = round(random.uniform(5_000, 80_000), 2)
    actual = round(scheduled * random.uniform(0.5, 1.0), 2) if event_type != "MISSED" else None
    overdue = random.randint(1, 120) if event_type in ("MISSED", "LATE_FEE", "PENALTY") else 0

    return {
        "event_id": str(uuid.uuid4()),
        "client_id": client_id,
        "loan_id": loan_id,
        "event_type": event_type,
        "scheduled_amount": scheduled,
        "actual_amount": actual,
        "principal_part": round(actual * 0.6, 2) if actual else None,
        "interest_part": round(actual * 0.4, 2) if actual else None,
        "penalty_amount": round(random.uniform(500, 5_000), 2) if event_type in ("LATE_FEE", "PENALTY") else 0,
        "days_overdue": overdue,
        "payment_channel": random.choice(PAY_CHANNELS),
        "due_date": (date.today() - timedelta(days=random.randint(0, 60))).isoformat(),
        "event_at": _iso(_recent()),
    }


def gen_loan_operation(pool: EntityPool) -> dict:
    client_id, loan_id = pool.random_client_with_loan()
    suspicious = random.random() < 0.05

    return {
        "event_id": str(uuid.uuid4()),
        "client_id": client_id,
        "loan_id": loan_id,
        "operation_type": random.choices(OPERATION_TYPES, weights=OPERATION_WEIGHTS)[0],
        "amount": round(random.uniform(10_000, 2_000_000), 2),
        "device_id": str(uuid.uuid4()),
        "device_type": random.choice(DEVICE_TYPES),
        "ip_country": random.choices(["KZ", "RU", "US", "DE", "TR"], weights=[88, 5, 3, 2, 2])[0],
        "is_suspicious": suspicious,
        "suspicious_reason": random.choice(
            ["UNUSUAL_LOCATION", "NEW_DEVICE", "VELOCITY_BREACH"]
        ) if suspicious else None,
        "operation_at": _iso(_recent()),
    }


def gen_client_money(pool: EntityPool) -> dict:
    event_type = random.choices(MONEY_EVENT_TYPES, weights=MONEY_EVENT_WEIGHTS)[0]
    before = round(random.uniform(5_000, 2_000_000), 2)
    amount = round(random.uniform(10_000, 500_000), 2)

    is_credit = event_type in ("SALARY_CREDIT", "TRANSFER_IN", "LARGE_CREDIT")
    after = round(before + amount, 2) if is_credit else round(max(before - amount, 0), 2)

    period_month = None
    if event_type == "MONTH_END_BALANCE":
        period_month = date.today().replace(day=1).isoformat()
        after = before

    return {
        "event_id": str(uuid.uuid4()),
        "client_id": pool.random_client(),
        "event_type": event_type,
        "account_type": random.choice(["CURRENT", "SAVINGS", "DEPOSIT"]),
        "balance_before": before,
        "balance_after": after,
        "amount": amount,
        "currency": "KZT",
        "counterparty_id": str(uuid.uuid4()) if event_type.startswith("TRANSFER") else None,
        "source_system": random.choice(SOURCE_SYSTEMS),
        "event_at": _iso(_recent()),
        "period_month": period_month,
    }


GENERATORS = {
    TOPIC_APPLICATIONS: gen_loan_application,
    TOPIC_PAYMENTS: gen_loan_payment,
    TOPIC_OPERATIONS: gen_loan_operation,
    TOPIC_MONEY: gen_client_money,
}