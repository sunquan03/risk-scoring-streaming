from dataclasses import dataclass
import os
import psycopg2

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
        self.sql_dir: str = os.environ.get("SQL_DIR", "sql")


settings = Settings()


@dataclass
class TopicConfig:
    topic_name:           str
    consumer_group:       str
    target_table:         str
    batch_size:           int
    max_poll_interval_ms: int
    session_timeout_ms:   int
    is_active:            bool


@dataclass
class FeatureGroupConfig:
    group_id:             str
    display_name:         str
    cache_key_template:   str
    ttl_seconds:          int
    sql_file:             str
    recompute_on_miss:    bool
    is_active:            bool
    priority:             int


@dataclass
class PipelineConfig:
    settings:       Settings
    topics:         list[TopicConfig]
    feature_groups: list[FeatureGroupConfig]
    sql_queries:    dict[str, str]



def load_config(stgs: Settings) -> PipelineConfig:
    conn = psycopg2.connect(dbname=stgs.pg_database, user=stgs.pg_user, password=stgs.pg_password, host=stgs.pg_host,
                            port=stgs.pg_port)

    try:

        pipelineConfig = PipelineConfig(settings=stgs)

        tc_query = """
            SELECT topic_name, consumer_group, target_table, batch_size,
                   max_poll_interval_ms, session_timeout_ms, is_active
            FROM pipeline_config.kafka_topics
            WHERE is_active = TRUE
            ORDER BY topic_name
        """

        with conn.cursor() as cur:
            cur.execute(tc_query)
            rows = cur.fetchall()

            for row in rows:
                topic_config = TopicConfig(topic_name=row["topic_name"],
                                           consumer_group=row["consumer_group"],
                                           target_table=row["target_table"],
                                           batch_size=row["batch_size"],
                                           max_poll_interval_ms=row["max_poll_interval_ms"],
                                           session_timeout_ms=row["session_timeout_ms"],
                                           is_active=row["is_active"],)

                pipelineConfig.topics.append(topic_config)


        fg_query = """
            SELECT group_id, display_name, cache_key_template, ttl_seconds,
                   sql_file, recompute_on_miss, is_active, priority
            FROM pipeline_config.feature_groups
            WHERE is_active = TRUE
            ORDER BY priority, group_id
        """

        with conn.cursor() as cur:
            cur.execute(fg_query)
            rows = cur.fetchall()

            for row in rows:
                feature_group_config = FeatureGroupConfig(
                    group_id=row["group_id"],
                    display_name=row["display_name"],
                    cache_key_template=row["cache_key_template"],
                    ttl_seconds=row["ttl_seconds"],
                    sql_file=row["sql_file"],
                    recompute_on_miss=row["recompute_on_miss"],
                    is_active=row["is_active"],
                    priority=row["priority"],
                )

                pipelineConfig.feature_groups.append(feature_group_config)

        # reading queries
        for fg in pipelineConfig.feature_groups:
            path = os.path.join(stgs.sql_dir, fg.group_id)
            with open(path, mode="r", encoding="utf-8") as f:
                sql_query= f.read()
                pipelineConfig.sql_queries[fg.group_id] = sql_query

        return pipelineConfig
    except Exception:
        conn.close()
    finally:
        conn.close()
