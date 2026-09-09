-- Aggregation: Payment Behaviour Features
-- Parameter: :client_id
SELECT JSON_OBJECT(
    'client_id',            :client_id,
    'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
    -- Payment counts
    'total_payment_events',     COUNT(*),
    'on_time_payments',         SUM(CASE WHEN event_type = 'PAYMENT' AND days_overdue = 0 THEN 1 ELSE 0 END),
    'late_payments',            SUM(CASE WHEN event_type = 'PAYMENT' AND days_overdue > 0 THEN 1 ELSE 0 END),
    'missed_payments',          SUM(CASE WHEN event_type = 'MISSED' THEN 1 ELSE 0 END),
    'missed_last_90d',          SUM(CASE WHEN event_type = 'MISSED' AND event_at >= NOW() - INTERVAL 90 DAY THEN 1 ELSE 0 END),
    -- Amount metrics
    'total_paid_amount',        SUM(COALESCE(actual_amount, 0)),
    'total_penalties',          SUM(COALESCE(penalty_amount, 0)),
    'avg_payment_amount',       ROUND(AVG(actual_amount), 2),
    'max_single_payment',       MAX(actual_amount),
    -- Overdue metrics
    'max_days_overdue',         MAX(days_overdue),
    'avg_days_overdue',         ROUND(AVG(CASE WHEN days_overdue > 0 THEN days_overdue END), 1),
    'ever_30dpd',               MAX(CASE WHEN days_overdue >= 30 THEN 1 ELSE 0 END),
    'ever_60dpd',               MAX(CASE WHEN days_overdue >= 60 THEN 1 ELSE 0 END),
    'ever_90dpd',               MAX(CASE WHEN days_overdue >= 90 THEN 1 ELSE 0 END),
    -- Principal vs interest breakdown
    'total_principal_paid',     SUM(COALESCE(principal_part, 0)),
    'total_interest_paid',      SUM(COALESCE(interest_part, 0)),
    -- Temporal
    'last_payment_at',          MAX(CASE WHEN event_type IN ('PAYMENT','PARTIAL_PAYMENT') THEN event_at END),
    'days_since_last_payment',  DATEDIFF(NOW(), MAX(CASE WHEN event_type IN ('PAYMENT','PARTIAL_PAYMENT') THEN event_at END)),
    -- Fee events
    'late_fee_count',           SUM(CASE WHEN event_type = 'LATE_FEE' THEN 1 ELSE 0 END),
    'penalty_count',            SUM(CASE WHEN event_type = 'PENALTY' THEN 1 ELSE 0 END),
    'restructure_count',        SUM(CASE WHEN event_type = 'RESTRUCTURE' THEN 1 ELSE 0 END)
) AS features
FROM loan_payment_events
WHERE client_id = :client_id;
