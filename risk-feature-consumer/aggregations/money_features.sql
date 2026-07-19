-- Aggregation: Client Money Profile Features
-- Parameter: :client_id
SELECT JSON_OBJECT(
    'client_id',            :client_id,
    'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
    -- Balance metrics
    'latest_month_end_balance', (
        SELECT balance_after FROM client_money_events
        WHERE client_id = :client_id AND event_type = 'MONTH_END_BALANCE'
        ORDER BY period_month DESC LIMIT 1
    ),
    'avg_monthly_balance',      ROUND(AVG(CASE WHEN event_type = 'MONTH_END_BALANCE' THEN balance_after END), 2),
    'min_monthly_balance',      MIN(CASE WHEN event_type = 'MONTH_END_BALANCE' THEN balance_after END),
    'max_monthly_balance',      MAX(CASE WHEN event_type = 'MONTH_END_BALANCE' THEN balance_after END),
    'balance_months_tracked',   COUNT(CASE WHEN event_type = 'MONTH_END_BALANCE' THEN 1 END),
    -- Income metrics
    'total_salary_credits',     SUM(CASE WHEN event_type = 'SALARY_CREDIT' THEN amount ELSE 0 END),
    'salary_credit_count',      COUNT(CASE WHEN event_type = 'SALARY_CREDIT' THEN 1 END),
    'avg_salary_credit',        ROUND(AVG(CASE WHEN event_type = 'SALARY_CREDIT' THEN amount END), 2),
    'max_salary_credit',        MAX(CASE WHEN event_type = 'SALARY_CREDIT' THEN amount END),
    'last_salary_at',           MAX(CASE WHEN event_type = 'SALARY_CREDIT' THEN event_at END),
    -- Transfer activity
    'total_transfer_in',        SUM(CASE WHEN event_type = 'TRANSFER_IN' THEN amount ELSE 0 END),
    'total_transfer_out',       SUM(CASE WHEN event_type = 'TRANSFER_OUT' THEN amount ELSE 0 END),
    'large_debit_count',        COUNT(CASE WHEN event_type = 'LARGE_DEBIT' THEN 1 END),
    'large_credit_count',       COUNT(CASE WHEN event_type = 'LARGE_CREDIT' THEN 1 END),
    'max_single_debit',         MAX(CASE WHEN event_type IN ('TRANSFER_OUT','LARGE_DEBIT') THEN amount END),
    'max_single_credit',        MAX(CASE WHEN event_type IN ('TRANSFER_IN','SALARY_CREDIT','LARGE_CREDIT') THEN amount END),
    -- Velocity (last 30 day)
    'net_flow_last_30d',        SUM(CASE
                                    WHEN event_at >= NOW() - INTERVAL 30 DAY AND event_type IN ('TRANSFER_IN','SALARY_CREDIT','LARGE_CREDIT') THEN amount
                                    WHEN event_at >= NOW() - INTERVAL 30 DAY AND event_type IN ('TRANSFER_OUT','LARGE_DEBIT') THEN -amount
                                    ELSE 0 END),
    'events_last_30d',          SUM(CASE WHEN event_at >= NOW() - INTERVAL 30 DAY THEN 1 ELSE 0 END),
    -- Source diversity
    'distinct_source_systems',  COUNT(DISTINCT source_system),
    'distinct_accounts',        COUNT(DISTINCT account_type)
) AS features
FROM client_money_events
WHERE client_id = :client_id;
