from typing import Optional, TypedDict
from decimal import Decimal, InvalidOperation
from datetime import datetime



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



TOPIC_TRANSFORMERS = {
    "loan-applications": transform_loan_application,
}
