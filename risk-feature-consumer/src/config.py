from dataclasses import dataclass
import os

@dataclass
class Settings:
    def __init__(self):
        # kafka
        self.kafka_bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        self.kafka_security_protocol: str = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        self.kafka_sasl_mechanism: str | None = os.environ.get("KAFKA_SASL_MECHANISM", None)
        self.kafka_sasl_username: str | None = os.environ.get("KAFKA_SASL_USERNAME", None)
        self.kafka_sasl_password: str | None = os.environ.get("KAFKA_SASL_PASSWORD", None)

        # TiDB
        self.tidb_host: str = os.environ.get("TIDB_HOST", "localhost")
        self.tidb_port: int = int(os.environ.get("TIDB_PORT", "4000"))
        self.tidb_user: str = os.environ.get("TIDB_USER", "root")
        self.tidb_password: str = os.environ.get("TIDB_PASSWORD", "")
        self.tidb_database: str = os.environ.get("TIDB_DATABASE", "risks")
        self.tidb_pool_size: int = int(os.environ.get("TIDB_POOL_SIZE", "10"))

        # Postgres
        self.pg_host: str = os.environ.get("PG_HOST", "localhost")
        self.pg_port: int = int(os.environ.get("PG_PORT", "5432"))
        self.pg_user: str = os.environ.get("PG_USER", "postgres")
        self.pg_password: str = os.environ.get("PG_PASSWORD", "")
        self.pg_database: str = os.environ.get("PG_DATABASE", "pipeline_configs")

        # Pipeline
        self.dlq_topic: str = os.environ.get("KAFKA_DLQ_TOPIC", "risk-pipeline-dlq")
        self.log_level: str = os.environ.get("LOG_LEVEL", "INFO")



settings = Settings()