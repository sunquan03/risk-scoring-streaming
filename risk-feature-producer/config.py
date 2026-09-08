import os
from dataclasses import dataclass, field
from .generator import ALL_TOPICS

@dataclass
class Config:
    kafka_bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
    kafka_security_protocol: str = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    kafka_sasl_mechanism: str | None = os.environ.get("KAFKA_SASL_MECHANISM")
    kafka_sasl_username: str | None = os.environ.get("KAFKA_SASL_USERNAME")
    kafka_sasl_password: str | None = os.environ.get("KAFKA_SASL_PASSWORD")

    interval_ms: int = int(os.environ.get("INTERVAL_MS", "500"))
    max_events: int = int(os.environ.get("MAX_EVENTS", "0"))
    malformed_rate: float = float(os.environ.get("MALFORMED_RATE", "0.02"))
    client_pool_size: int = int(os.environ.get("CLIENT_POOL_SIZE", "200"))
    seed: int = int(os.environ.get("SEED", "42"))
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")



    def __post_init__(self) -> None:
        raw = os.environ.get("TOPICS", "").strip()
        if raw:
            requested = [t.strip() for t in raw.split(",") if t.strip()]
            unknown = [t for t in requested if t not in ALL_TOPICS]
            if unknown:
                raise ValueError(f"unknown topic(s): {unknown}; valid: {ALL_TOPICS}")
            self.topics = requested
        else:
            self.topics = list(ALL_TOPICS)