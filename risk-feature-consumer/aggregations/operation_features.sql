-- Aggregation: Loan Operation & Device Features
-- Parameter: :client_id
SELECT JSON_OBJECT(
    'client_id',            :client_id,
    'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
    -- Operation counts
    'total_operations',         COUNT(*),
    'disbursement_count',       SUM(CASE WHEN operation_type = 'DISBURSEMENT' THEN 1 ELSE 0 END),
    'topup_count',              SUM(CASE WHEN operation_type = 'TOPUP' THEN 1 ELSE 0 END),
    'early_close_count',        SUM(CASE WHEN operation_type = 'EARLY_CLOSE' THEN 1 ELSE 0 END),
    'refinance_count',          SUM(CASE WHEN operation_type = 'REFINANCE' THEN 1 ELSE 0 END),
    'write_off_count',          SUM(CASE WHEN operation_type = 'WRITE_OFF' THEN 1 ELSE 0 END),
    -- Amount metrics
    'total_disbursed',          SUM(CASE WHEN operation_type = 'DISBURSEMENT' THEN COALESCE(amount, 0) ELSE 0 END),
    'avg_disbursement',         ROUND(AVG(CASE WHEN operation_type = 'DISBURSEMENT' THEN amount END), 2),
    'max_disbursement',         MAX(CASE WHEN operation_type = 'DISBURSEMENT' THEN amount END),
    -- Device/fraud signals
    'distinct_devices',         COUNT(DISTINCT device_id),
    'distinct_device_types',    COUNT(DISTINCT device_type),
    'suspicious_op_count',      SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END),
    'suspicious_last_30d',      SUM(CASE WHEN is_suspicious = 1 AND operation_at >= NOW() - INTERVAL 30 DAY THEN 1 ELSE 0 END),
    'distinct_countries',       COUNT(DISTINCT ip_country),
    'foreign_op_count',         SUM(CASE WHEN ip_country != 'KZ' AND ip_country IS NOT NULL THEN 1 ELSE 0 END),
    -- Temporal
    'first_operation_at',       MIN(operation_at),
    'last_operation_at',        MAX(operation_at),
    'ops_last_30d',             SUM(CASE WHEN operation_at >= NOW() - INTERVAL 30 DAY THEN 1 ELSE 0 END)
) AS features
FROM loan_operation_events
WHERE client_id = :client_id;
