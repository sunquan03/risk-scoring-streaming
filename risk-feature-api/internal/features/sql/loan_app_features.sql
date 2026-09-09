-- Aggregation: Loan Application Features
-- Called by Python service after upsert, and by Go API on cache miss
-- Parameter: :client_id (string)
-- Returns: single JSON object

SELECT JSON_OBJECT(
    'client_id',            :client_id,
    'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
    -- Volume metrics
    'total_applications',       COUNT(*),
    'apps_last_30d',            SUM(CASE WHEN applied_at >= NOW() - INTERVAL 30 DAY THEN 1 ELSE 0 END),
    'apps_last_90d',            SUM(CASE WHEN applied_at >= NOW() - INTERVAL 90 DAY THEN 1 ELSE 0 END),
    'apps_last_12m',            SUM(CASE WHEN applied_at >= NOW() - INTERVAL 12 MONTH THEN 1 ELSE 0 END),
    -- Amount metrics
    'max_requested_amount',     MAX(requested_amount),
    'min_requested_amount',     MIN(requested_amount),
    'avg_requested_amount',     ROUND(AVG(requested_amount), 2),
    'total_approved_amount',    SUM(COALESCE(approved_amount, 0)),
    'max_approved_amount',      MAX(approved_amount),
    -- Decision metrics
    'approved_count',           SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END),
    'rejected_count',           SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END),
    'approval_rate',            ROUND(SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4),
    -- Recent behaviour
    'last_application_at',      MAX(applied_at),
    'last_status',              (SELECT status FROM loan_applications WHERE client_id = :client_id ORDER BY applied_at DESC LIMIT 1),
    'last_rejection_reason',    (SELECT rejection_reason FROM loan_applications WHERE client_id = :client_id AND status = 'REJECTED' ORDER BY applied_at DESC LIMIT 1),
    -- Channel diversity
    'distinct_channels',        COUNT(DISTINCT channel),
    'mobile_app_count',         SUM(CASE WHEN channel = 'MOBILE_APP' THEN 1 ELSE 0 END),
    -- Score at decision (risk signal)
    'avg_score_at_decision',    ROUND(AVG(score_at_decision), 2),
    'min_score_at_decision',    MIN(score_at_decision),
    -- Product diversity
    'distinct_products',        COUNT(DISTINCT product_code),
    'has_mortgage',             MAX(CASE WHEN product_code = 'MORTGAGE' THEN 1 ELSE 0 END),
    'has_micro_loan',           MAX(CASE WHEN product_code = 'MICRO' THEN 1 ELSE 0 END)

) AS features
FROM loan_applications
WHERE client_id = :client_id;
