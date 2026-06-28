from typing import Optional, TypedDict
from decimal import Decimal, InvalidOperation
from datetime import datetime, date
import uuid
import hashlib

class TransformError(Exception):
    def __init__(self, message: str, field: str = None, value: str = None) -> None:
        self.field = field
        self.value = value
        super().__init__(message)

class ApplicationDict(TypedDict, total=False):
    application_id: str
    client_id: str
    product_code: str
    requested_amount: Decimal
    approved_amount: Optional[Decimal]
    term_months: int
    annual_rate: Decimal
    purpose: Optional[str]
    channel: str
    status: str
    rejection_reason: Optional[str]
    score_at_decision: Optional[Decimal]
    city: Optional[str]
    applied_at: datetime
    decided_at: Optional[datetime]
    kafka_offset: int


def transform_loan_application(raw: dict, kafka_offset: int) -> dict:
    result: ApplicationDict = {}

    try:
        result["application_id"] = raw["application_id"]
    except KeyError:
        raise TransformError(message="Missing required field", field="application_id")
    try:
        result["client_id"] = raw["client_id"]
    except KeyError:
        raise TransformError(message="Missing required field", field="client_id")

    result["product_code"] = raw["product_code"].upper()

    try:
        result["requested_amount"] = Decimal(str(raw["requested_amount"]))
    except InvalidOperation:
        raise TransformError(message="Decimal convertion failed", field="requested_amount", value=raw.get("requested_amount"))

    result["term_months"] = int(raw["term_months"])

    try:
        result["annual_rate"] = Decimal(str(raw["annual_rate"]))
    except InvalidOperation:
        raise TransformError(message="Decimal convertion failed", field="annual_rate", value=raw.get("annual_rate"))

    result["channel"] = raw["channel"].upper()
    result["status"] = raw["status"].upper()

    # optional fields
    try:
        result["approved_amount"] = Decimal(str(raw["approved_amount"])) if raw.get("approved_amount") is not None else None
    except InvalidOperation:
        raise TransformError(message="Decimal convertion failed", field="approved_amount", value=raw.get("approved_amount"))

    result["purpose"] = raw.get("purpose")
    result["rejection_reason"] = raw.get("rejection_reason")

    try:
        result["score_at_decision"] = Decimal(str(raw["score_at_decision"])) if raw.get("score_at_decision") is not None else None
    except InvalidOperation:
        raise TransformError(message="Decimal convertion failed", field="score_at_decision", value=raw.get("score_at_decision"))

    result["city"] = raw.get("city")

    try:
        result["applied_at"] = datetime.fromisoformat(raw["applied_at"])
    except KeyError:
        raise TransformError(message="Missing required field", field="applied_at")
    except (TypeError, ValueError):
        raise TransformError(message="Datetime formatting failed", field="applied_at")

    try:
        result["decided_at"] = datetime.fromisoformat(raw["decided_at"]) if raw.get("decided_at") is not None else None
    except KeyError:
        raise TransformError(message="Missing required field", field="decided_at")
    except (TypeError, ValueError):
        raise TransformError(message="Datetime formatting failed", field="decided_at")

    result["kafka_offset"] = kafka_offset

    return result



class LoanPaymentDict(TypedDict, total=False):
    event_id: str
    client_id: str
    loan_id: str
    event_type: str
    scheduled_amount: Optional[Decimal]
    actual_amount: Optional[Decimal]
    principal_part: Optional[Decimal]
    interest_part: Optional[Decimal]
    penalty_amount: Decimal
    days_overdue: int
    payment_channel: Optional[str]
    due_date: date
    event_at: datetime
    kafka_offset: int


def transform_loan_payment(raw: dict, kafka_offset: int) -> dict:
    result: LoanPaymentDict = {}

    # event_id – default to a new UUID if not provided
    result["event_id"] = raw.get("event_id") or str(uuid.uuid4())

    # required fields
    try:
        result["client_id"] = raw["client_id"]
    except KeyError:
        raise TransformError(message="Missing required field", field="client_id")

    try:
        result["loan_id"] = raw["loan_id"]
    except KeyError:
        raise TransformError(message="Missing required field", field="loan_id")

    try:
        result["event_type"] = raw["event_type"].upper()
    except KeyError:
        raise TransformError(message="Missing required field", field="event_type")

    # optional Decimal fields
    try:
        result["scheduled_amount"] = (
            Decimal(str(raw["scheduled_amount"]))
            if raw.get("scheduled_amount") is not None
            else None
        )
    except InvalidOperation:
        raise TransformError(
            message="Decimal conversion failed",
            field="scheduled_amount",
            value=raw.get("scheduled_amount"),
        )

    try:
        result["actual_amount"] = (
            Decimal(str(raw["actual_amount"]))
            if raw.get("actual_amount") is not None
            else None
        )
    except InvalidOperation:
        raise TransformError(
            message="Decimal conversion failed",
            field="actual_amount",
            value=raw.get("actual_amount"),
        )

    try:
        result["principal_part"] = (
            Decimal(str(raw["principal_part"]))
            if raw.get("principal_part") is not None
            else None
        )
    except InvalidOperation:
        raise TransformError(
            message="Decimal conversion failed",
            field="principal_part",
            value=raw.get("principal_part"),
        )

    try:
        result["interest_part"] = (
            Decimal(str(raw["interest_part"]))
            if raw.get("interest_part") is not None
            else None
        )
    except InvalidOperation:
        raise TransformError(
            message="Decimal conversion failed",
            field="interest_part",
            value=raw.get("interest_part"),
        )

    # penalty_amount – default to Decimal("0") if missing
    try:
        penalty = raw.get("penalty_amount")
        result["penalty_amount"] = (
            Decimal(str(penalty)) if penalty is not None else Decimal("0")
        )
    except InvalidOperation:
        raise TransformError(
            message="Decimal conversion failed",
            field="penalty_amount",
            value=raw.get("penalty_amount"),
        )

    # days_overdue – default to 0 if missing
    try:
        overdue = raw.get("days_overdue")
        result["days_overdue"] = int(overdue) if overdue is not None else 0
    except (TypeError, ValueError):
        raise TransformError(
            message="Integer conversion failed",
            field="days_overdue",
            value=raw.get("days_overdue"),
        )

    channel = raw.get("payment_channel")
    if channel is not None and channel != "":
        result["payment_channel"] = channel.upper()
    else:
        result["payment_channel"] = None

    try:
        result["due_date"] = date.fromisoformat(raw["due_date"])
    except KeyError:
        raise TransformError(message="Missing required field", field="due_date")
    except (TypeError, ValueError):
        raise TransformError(
            message="Date formatting failed (expected YYYY-MM-DD)",
            field="due_date",
            value=raw.get("due_date"),
        )

    try:
        result["event_at"] = datetime.fromisoformat(raw["event_at"])
    except KeyError:
        raise TransformError(message="Missing required field", field="event_at")
    except (TypeError, ValueError):
        raise TransformError(
            message="Datetime formatting failed",
            field="event_at",
            value=raw.get("event_at"),
        )

    result["kafka_offset"] = kafka_offset
    return result


class LoanOperationsDict(TypedDict, total=False):
    event_id: str
    client_id: str
    loan_id: str
    operation_type: str
    amount: Optional[Decimal]
    device_id: Optional[str]
    device_type: Optional[str]
    ip_country: Optional[str]
    is_suspicious: int
    suspicious_reason: Optional[str]
    operation_at: datetime
    kafka_offset: int


def transform_loan_operations(raw: dict, kafka_offset: int) -> dict:
    result: LoanOperationsDict = {}

    # event_id – default to a new UUID if not provided
    result["event_id"] = raw.get("event_id") or str(uuid.uuid4())

    # required fields
    try:
        result["client_id"] = raw["client_id"]
    except KeyError:
        raise TransformError(message="Missing required field", field="client_id")

    try:
        result["loan_id"] = raw["loan_id"]
    except KeyError:
        raise TransformError(message="Missing required field", field="loan_id")

    try:
        result["operation_type"] = raw["operation_type"].upper()
    except KeyError:
        raise TransformError(message="Missing required field", field="operation_type")

    try:
        result["amount"] = (
            Decimal(str(raw["amount"]))
            if raw.get("amount") is not None
            else None
        )
    except InvalidOperation:
        raise TransformError(
            message="Decimal conversion failed",
            field="amount",
            value=raw.get("amount"),
        )

    # device_id – optional; if present, compute SHA‑256 hash and take first 32 chars
    device_raw = raw.get("device_id")
    if device_raw is not None:
        hash_hex = hashlib.sha256(str(device_raw).encode()).hexdigest()
        result["device_id"] = hash_hex[:32]
    else:
        result["device_id"] = None

    device_type = raw.get("device_type")
    if device_type is not None and device_type != "":
        result["device_type"] = device_type.upper()
    else:
        result["device_type"] = None

    ip_country = raw.get("ip_country")
    if ip_country is not None and ip_country != "":
        result["ip_country"] = ip_country.upper()
    else:
        result["ip_country"] = None

    try:
        val = raw["is_suspicious"]
    except KeyError:
        raise TransformError(message="Missing required field", field="is_suspicious")

    if isinstance(val, bool):
        result["is_suspicious"] = 1 if val else 0
    elif isinstance(val, (int, float)):

        if val in (0, 1):
            result["is_suspicious"] = int(val)
        else:
            raise TransformError(
                message="is_suspicious must be 0 or 1",
                field="is_suspicious",
                value=str(val)
            )
    elif isinstance(val, str):
        val_lower = val.lower()
        if val_lower in ("true", "1", "yes", "y"):
            result["is_suspicious"] = 1
        elif val_lower in ("false", "0", "no", "n"):
            result["is_suspicious"] = 0
        else:
            raise TransformError(
                message="Invalid string for is_suspicious (expected true/false or 0/1)",
                field="is_suspicious",
                value=val
            )
    else:
        raise TransformError(
            message="is_suspicious must be bool, int, or string",
            field="is_suspicious",
            value=str(val)
        )

    result["suspicious_reason"] = raw.get("suspicious_reason")

    try:
        result["operation_at"] = datetime.fromisoformat(raw["operation_at"])
    except KeyError:
        raise TransformError(message="Missing required field", field="operation_at")
    except (TypeError, ValueError):
        raise TransformError(
            message="Datetime formatting failed",
            field="operation_at",
            value=raw.get("operation_at"),
        )

    result["kafka_offset"] = kafka_offset

    return result

class ClientMoneyDict(TypedDict, total=False):
    event_id: str
    client_id: str
    event_type: str
    account_type: Optional[str]
    balance_before: Optional[Decimal]
    balance_after: Optional[Decimal]
    amount: Decimal
    currency: str
    counterparty_id: Optional[str]
    source_system: Optional[str]
    event_at: datetime
    period_month: Optional[date]
    kafka_offset: int


def transform_client_money(raw: dict, kafka_offset: int) -> dict:
    result: ClientMoneyDict = {}

    result["event_id"] = raw.get("event_id") or str(uuid.uuid4())

    try:
        result["client_id"] = raw["client_id"]
    except KeyError:
        raise TransformError(message="Missing required field", field="client_id")

    try:
        result["event_type"] = raw["event_type"].upper()
    except KeyError:
        raise TransformError(message="Missing required field", field="event_type")

    account_type = raw.get("account_type")
    if account_type is not None and account_type != "":
        result["account_type"] = account_type.upper()
    else:
        result["account_type"] = None

    for field in ("balance_before", "balance_after"):
        try:
            val = raw.get(field)
            result[field] = Decimal(str(val)) if val is not None else None
        except InvalidOperation:
            raise TransformError(
                message="Decimal conversion failed",
                field=field,
                value=raw.get(field)
            )

    try:
        result["amount"] = Decimal(str(raw["amount"]))
    except KeyError:
        raise TransformError(message="Missing required field", field="amount")
    except InvalidOperation:
        raise TransformError(
            message="Decimal conversion failed",
            field="amount",
            value=raw.get("amount")
        )

    currency = raw.get("currency", "KZT")
    if currency is not None and currency != "":
        result["currency"] = currency.upper()
    else:
        result["currency"] = "KZT"

    result["counterparty_id"] = raw.get("counterparty_id")

    result["source_system"] = raw.get("source_system")

    try:
        result["event_at"] = datetime.fromisoformat(raw["event_at"])
    except KeyError:
        raise TransformError(message="Missing required field", field="event_at")
    except (TypeError, ValueError):
        raise TransformError(
            message="Datetime formatting failed",
            field="event_at",
            value=raw.get("event_at")
        )

    period_str = raw.get("period_month")
    if period_str is not None and period_str != "":
        try:
            result["period_month"] = date.fromisoformat(period_str)
        except (TypeError, ValueError):
            raise TransformError(
                message="Date formatting failed (expected YYYY-MM-DD)",
                field="period_month",
                value=period_str
            )
    else:
        result["period_month"] = None

    result["kafka_offset"] = kafka_offset
    return result

TOPIC_TRANSFORMERS = {
    "loan-applications": transform_loan_application,
    "loan-payments":     transform_loan_payment,
    "loan-operations": transform_loan_operations,
    "client-money": transform_client_money,
}
