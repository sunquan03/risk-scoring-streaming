from config import Settings, load_config
from consumer import Consumer
import argparse
import psycopg2
import sys
import structlog
from db import get_pool

def main() -> None:
    log = structlog.get_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    args = parser.parse_args()

    settings = Settings()

    pipeline_cfg = load_config(settings)

    topic_cfg = next((t for t in pipeline_cfg.topics if t.topic_name == args.topic),None)
    if not topic_cfg:
        log.error("topic_not_found", topic=args.topic)
        sys.exit(1)

    tidb_pool = get_pool(
        host=settings.tidb_host,
        port=settings.tidb_port,
        user=settings.tidb_user,
        password=settings.tidb_password,
        database=settings.tidb_database,
        pool_size=settings.tidb_pool_size,
    )

    def pg_conn_factory():
        return psycopg2.connect(
            host=settings.pg_host, port=settings.pg_port,
            user=settings.pg_user, password=settings.pg_password,
            dbname=settings.pg_database,
        )

    consumer = Consumer(
        topic_config=topic_cfg,
        pipeline_config=pipeline_cfg,
        tidb_pool=tidb_pool,
        pg_conn_factory=pg_conn_factory,
    )
    consumer.run()