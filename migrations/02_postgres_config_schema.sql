-- PIPELINE CONFIGURATION — Postgres Schema
-- These tables drive all behaviour of the Python service.
-- The service reads config on startup (and can reload via SIGHUP).

CREATE SCHEMA IF NOT EXISTS pipeline_config;

-- Kafka topic configuration
CREATE TABLE pipeline_config.kafka_topics (
    topic_name          VARCHAR(120)    NOT NULL,
    consumer_group      VARCHAR(120)    NOT NULL,
    target_table        VARCHAR(80)     NOT NULL    COMMENT 'TiDB table to upsert into',
    schema_version      SMALLINT        NOT NULL DEFAULT 1,
    batch_size          INT             NOT NULL DEFAULT 500,
    max_poll_interval_ms INT            NOT NULL DEFAULT 30000,
    session_timeout_ms  INT             NOT NULL DEFAULT 10000,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (topic_name)
);

-- Feature group configuration
-- Each feature group is one cache key pattern + ttl
CREATE TABLE pipeline_config.feature_groups (
    group_id            VARCHAR(60)     NOT NULL,
    display_name        VARCHAR(100)    NOT NULL,
    cache_key_template  VARCHAR(200)    NOT NULL,   -- e.g. 'client:{client_id}:loan_apps'
    ttl_seconds         INT             NOT NULL DEFAULT 3600,
    sql_file            VARCHAR(200)    NOT NULL,   -- path relative to /sql/aggregations/
    recompute_on_miss   BOOLEAN         NOT NULL DEFAULT TRUE,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    priority            SMALLINT        NOT NULL DEFAULT 10,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id)
);

-- Field-level transformation rules for adding computed columns 
CREATE TABLE pipeline_config.field_transforms (
    transform_id        SERIAL          PRIMARY KEY,
    topic_name          VARCHAR(120)    NOT NULL REFERENCES pipeline_config.kafka_topics(topic_name),
    source_field        VARCHAR(80)     NOT NULL,   -- field name in Kafka JSON
    target_field        VARCHAR(80)     NOT NULL,   -- column in TiDB
    transform_type      VARCHAR(30)     NOT NULL,   -- COPY, CAST_DECIMAL, PARSE_DATETIME, HASH, UPPER, LOWER, MAP_VALUE
    transform_params    JSONB           DEFAULT '{}',
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Dead letter queue tracking 
CREATE TABLE pipeline_config.dlq_events (
    dlq_id              BIGSERIAL       PRIMARY KEY,
    topic_name          VARCHAR(120)    NOT NULL,
    kafka_partition     INT             NOT NULL,
    kafka_offset        BIGINT          NOT NULL,
    raw_payload         TEXT            NOT NULL,
    error_type          VARCHAR(80)     NOT NULL,
    error_message       TEXT            NOT NULL,
    retry_count         SMALLINT        NOT NULL DEFAULT 0,
    resolved            BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    resolved_at         TIMESTAMPTZ     DEFAULT NULL
);

-- Pipeline run metrics (lightweight observability)
CREATE TABLE pipeline_config.pipeline_runs (
    run_id              BIGSERIAL       PRIMARY KEY,
    topic_name          VARCHAR(120)    NOT NULL,
    batch_number        BIGINT          NOT NULL,
    messages_consumed   INT             NOT NULL DEFAULT 0,
    messages_inserted   INT             NOT NULL DEFAULT 0,
    messages_rejected   INT             NOT NULL DEFAULT 0,
    cache_keys_written  INT             NOT NULL DEFAULT 0,
    duration_ms         INT             DEFAULT NULL,
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ     DEFAULT NULL
);

-- Seed data - topic registrations
INSERT INTO pipeline_config.kafka_topics (topic_name, consumer_group, target_table, batch_size) VALUES
    ('loan-applications',   'risk-pipeline-apps',   'loan_applications',     500),
    ('loan-payments',       'risk-pipeline-pay',    'loan_payment_events',   500),
    ('loan-operations',     'risk-pipeline-ops',    'loan_operation_events', 500),
    ('client-money',        'risk-pipeline-money',  'client_money_events',   500)
ON CONFLICT (topic_name) DO NOTHING;

-- Seed data- feature groups
INSERT INTO pipeline_config.feature_groups (group_id, display_name, cache_key_template, ttl_seconds, sql_file, priority) VALUES
    ('loan_app_features',     'Loan application history',   'client:{client_id}:loan_apps',    1800, 'loan_app_features.sql',     10),
    ('payment_features',      'Payment behaviour',          'client:{client_id}:payments',     3600, 'payment_features.sql',      10),
    ('operation_features',    'Loan operation signals',     'client:{client_id}:operations',   3600, 'operation_features.sql',    20),
    ('money_features',        'Client money profile',       'client:{client_id}:money',        7200, 'money_features.sql',        20)
ON CONFLICT (group_id) DO NOTHING;

