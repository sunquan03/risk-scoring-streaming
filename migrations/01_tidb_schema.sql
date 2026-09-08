-- RISK SCORING PIPELINE — TiDB Schema

-- Domain 1: Loan Applications
-- Source: Kafka topic loan-applications
-- Historical data
CREATE TABLE IF NOT EXISTS loan_applications (
    application_id      VARCHAR(36)     NOT NULL,
    client_id           VARCHAR(20)     NOT NULL COMMENT 'IIN — Kazakhstan identity number',
    product_code        VARCHAR(20)     NOT NULL COMMENT 'e.g. CONSUMER_LOAN, MORTGAGE, MICRO',
    requested_amount    DECIMAL(15,2)   NOT NULL,
    approved_amount     DECIMAL(15,2)   DEFAULT NULL,
    term_months         SMALLINT        NOT NULL,
    annual_rate         DECIMAL(6,4)    NOT NULL,
    purpose             VARCHAR(100)    DEFAULT NULL,
    channel             VARCHAR(30)     NOT NULL COMMENT 'MOBILE_APP, WEB, BRANCH, KASPI_PAY',
    status              VARCHAR(20)     NOT NULL COMMENT 'PENDING, APPROVED, REJECTED, CANCELLED',
    rejection_reason    VARCHAR(100)    DEFAULT NULL,
    score_at_decision   DECIMAL(5,2)    DEFAULT NULL COMMENT 'credit score when decision made',
    city                VARCHAR(60)     DEFAULT NULL,
    applied_at          DATETIME(3)     NOT NULL,
    decided_at          DATETIME(3)     DEFAULT NULL,
    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    kafka_offset        BIGINT          DEFAULT NULL COMMENT 'source Kafka offset for dedup',
    PRIMARY KEY (application_id),
    INDEX idx_client_applied   (client_id, applied_at DESC),
    INDEX idx_status_decided   (status, decided_at),
    INDEX idx_product_channel  (product_code, channel)
) COMMENT='Loan application events - historical + streaming';


-- Domain 2: Loan Payments and Fee Events
-- Source: Kafka topic loan-payments
-- Historical: migrated from Postgres
CREATE TABLE IF NOT EXISTS loan_payment_events (
    event_id            VARCHAR(36)     NOT NULL,
    client_id           VARCHAR(20)     NOT NULL,
    loan_id             VARCHAR(36)     NOT NULL,
    event_type          VARCHAR(30)     NOT NULL COMMENT 'PAYMENT, PARTIAL_PAYMENT, MISSED, LATE_FEE, PENALTY, RESTRUCTURE',
    scheduled_amount    DECIMAL(15,2)   DEFAULT NULL,
    actual_amount       DECIMAL(15,2)   DEFAULT NULL,
    principal_part      DECIMAL(15,2)   DEFAULT NULL,
    interest_part       DECIMAL(15,2)   DEFAULT NULL,
    penalty_amount      DECIMAL(15,2)   DEFAULT 0,
    days_overdue        SMALLINT        DEFAULT 0,
    payment_channel     VARCHAR(30)     DEFAULT NULL COMMENT 'KASPI_PAY, BANK_TRANSFER, CASH, AUTO_DEBIT',
    due_date            DATE            NOT NULL,
    event_at            DATETIME(3)     NOT NULL,
    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    kafka_offset        BIGINT          DEFAULT NULL,
    PRIMARY KEY (event_id),
    INDEX idx_client_event    (client_id, event_at DESC),
    INDEX idx_loan_type       (loan_id, event_type),
    INDEX idx_overdue         (client_id, days_overdue, event_at DESC),
    INDEX idx_due_date        (due_date, event_type)
) COMMENT='Loan payment events -schedule hits, misses, fees';


-- Domain 3: Loan Operations & Transaction Events
-- Source: Kafka topic loan-operations
-- Signals: disbursement, top-up, early close, device ops
CREATE TABLE IF NOT EXISTS loan_operation_events (
    event_id            VARCHAR(36)     NOT NULL,
    client_id           VARCHAR(20)     NOT NULL,
    loan_id             VARCHAR(36)     NOT NULL,
    operation_type      VARCHAR(40)     NOT NULL COMMENT 'DISBURSEMENT, TOPUP, EARLY_CLOSE, REFINANCE, WRITE_OFF, TRANSFER',
    amount              DECIMAL(15,2)   DEFAULT NULL,
    device_id           VARCHAR(64)     DEFAULT NULL COMMENT 'hashed device fingerprint',
    device_type         VARCHAR(20)     DEFAULT NULL COMMENT 'IOS, ANDROID, WEB, POS',
    ip_country          CHAR(2)         DEFAULT NULL,
    is_suspicious       TINYINT(1)      DEFAULT 0,
    suspicious_reason   VARCHAR(100)    DEFAULT NULL,
    operation_at        DATETIME(3)     NOT NULL,
    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    kafka_offset        BIGINT          DEFAULT NULL,
    PRIMARY KEY (event_id),
    INDEX idx_client_op       (client_id, operation_at DESC),
    INDEX idx_loan_op         (loan_id, operation_type),
    INDEX idx_device          (device_id, operation_at DESC),
    INDEX idx_suspicious      (client_id, is_suspicious, operation_at DESC)
) COMMENT='Loan lifecycle and device transaction events';


-- Domain 4: Client Money Events (Balance and Income)
-- Source: Kafka topic client-money
-- Signals: month-end balance, salary credits, transfers
CREATE TABLE IF NOT EXISTS client_money_events (
    event_id            VARCHAR(36)     NOT NULL,
    client_id           VARCHAR(20)     NOT NULL,
    event_type          VARCHAR(30)     NOT NULL COMMENT 'MONTH_END_BALANCE, SALARY_CREDIT, TRANSFER_IN, TRANSFER_OUT, LARGE_DEBIT, LARGE_CREDIT',
    account_type        VARCHAR(20)     DEFAULT NULL COMMENT 'CURRENT, SAVINGS, DEPOSIT',
    balance_before      DECIMAL(15,2)   DEFAULT NULL,
    balance_after       DECIMAL(15,2)   DEFAULT NULL,
    amount              DECIMAL(15,2)   NOT NULL,
    currency            CHAR(3)         NOT NULL DEFAULT 'KZT',
    counterparty_id     VARCHAR(36)     DEFAULT NULL COMMENT 'hashed beneficiary or sender',
    source_system       VARCHAR(30)     DEFAULT NULL COMMENT 'KASPI_BANK, HALYK, FREEDOM, etc.',
    event_at            DATETIME(3)     NOT NULL,
    period_month        DATE            DEFAULT NULL COMMENT 'for MONTH_END_BALANCE: first day of month',
    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    kafka_offset        BIGINT          DEFAULT NULL,
    PRIMARY KEY (event_id),
    INDEX idx_client_money    (client_id, event_at DESC),
    INDEX idx_client_type     (client_id, event_type, event_at DESC),
    INDEX idx_period          (client_id, period_month DESC)
) COMMENT='Client financial -  position and money movement events';


-- KV Cache Table as a redis replacement
-- The Go API reads from here first (point lookup by cache_key).
-- Python service writes here after aggregation.
-- Go falls back to live aggregation SQL if row missing
CREATE TABLE IF NOT EXISTS kv_cache (
    cache_key           VARCHAR(200)    NOT NULL COMMENT 'pattern client:{id}:{feature_group}',
    value_json          JSON            NOT NULL,
    computed_at         DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    expires_at          DATETIME(3)     DEFAULT NULL COMMENT 'NULL means never expires; for Go API checks',
    version             INT             NOT NULL DEFAULT 1 COMMENT 'incremented on each recompute',
    PRIMARY KEY (cache_key)
) COMMENT='Pre-aggregated feature cache - replaces Redis hash store';


-- Reference / Enrichment Tables
-- These are NOT from Kafka. Loaded once, updated periodically.
CREATE TABLE IF NOT EXISTS clients (
    client_id           VARCHAR(20)     NOT NULL COMMENT 'IIN',
    full_name           VARCHAR(200)    DEFAULT NULL,
    birth_date          DATE            DEFAULT NULL,
    gender              CHAR(1)         DEFAULT NULL COMMENT 'M, F',
    city                VARCHAR(60)     DEFAULT NULL,
    region              VARCHAR(60)     DEFAULT NULL,
    registration_date   DATE            NOT NULL,
    segment             VARCHAR(20)     DEFAULT NULL COMMENT 'PREMIUM, STANDARD, SUBPRIME, NEW',
    is_active           TINYINT(1)      NOT NULL DEFAULT 1,
    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (client_id)
) COMMENT='Client master data enrichment reference';


CREATE TABLE IF NOT EXISTS loan_accounts (
    loan_id             VARCHAR(36)     NOT NULL,
    client_id           VARCHAR(20)     NOT NULL,
    product_code        VARCHAR(20)     NOT NULL,
    principal_amount    DECIMAL(15,2)   NOT NULL,
    outstanding_balance DECIMAL(15,2)   NOT NULL,
    monthly_payment     DECIMAL(15,2)   NOT NULL,
    term_months         SMALLINT        NOT NULL,
    annual_rate         DECIMAL(6,4)    NOT NULL,
    open_date           DATE            NOT NULL,
    close_date          DATE            DEFAULT NULL,
    status              VARCHAR(20)     NOT NULL COMMENT 'ACTIVE, CLOSED, OVERDUE, WRITTEN_OFF',
    dpd                 SMALLINT        NOT NULL DEFAULT 0 COMMENT 'days past due — current',
    max_dpd_ever        SMALLINT        NOT NULL DEFAULT 0 COMMENT 'worst DPD in loan history',
    restructure_count   TINYINT         NOT NULL DEFAULT 0,
    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (loan_id),
    INDEX idx_client_status   (client_id, status),
    INDEX idx_client_dpd      (client_id, dpd)
) COMMENT='Loan account master current state of each loan';


-- Aggregation query registry (loaded from file, linked here)
CREATE TABLE IF NOT EXISTS agg_query_registry (
    query_id            VARCHAR(60)     NOT NULL,
    feature_group       VARCHAR(40)     NOT NULL COMMENT 'loan_apps, payments, operations, money',
    cache_key_template  VARCHAR(200)    NOT NULL COMMENT 'e.g. client:{client_id}:loan_apps',
    ttl_seconds         INT             NOT NULL DEFAULT 3600,
    is_active           TINYINT(1)      NOT NULL DEFAULT 1,
    description         TEXT            DEFAULT NULL,
    created_at          DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (query_id)
) COMMENT='Registry of aggregation queries that link to SQL files';


-- Adding TiFlash (OLAP) replicas alongside TiKV for aggregation queries performance improvement
ALTER TABLE loan_applications    SET TIFLASH REPLICA 1;
ALTER TABLE loan_payment_events  SET TIFLASH REPLICA 1;
ALTER TABLE loan_operation_events SET TIFLASH REPLICA 1;
ALTER TABLE client_money_events  SET TIFLASH REPLICA 1;
ALTER TABLE loan_accounts        SET TIFLASH REPLICA 1;
ALTER TABLE clients              SET TIFLASH REPLICA 1;
