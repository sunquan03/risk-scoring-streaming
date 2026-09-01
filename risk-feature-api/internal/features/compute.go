package features

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

func Compute(ctx context.Context, db *sql.DB, groupID string, clientID string) (map[string]any, error) {
	query, ok := aggregationSQL[groupID]
	if !ok {
		return nil, fmt.Errorf("unknown group %s", groupID)
	}
	cntPlaceholders := 0
	for _, ch := range query {
		if ch == '?' {
			cntPlaceholders++
		}
	}

	params := make([]any, cntPlaceholders)
	for i := range params {
		params[i] = clientID
	}

	var raw json.RawMessage
	err := db.QueryRowContext(ctx, query, params...).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("GroupID [%s]: error: %w", groupID, err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("GroupID [%s]: error: %w", groupID, err)
	}
	return result, nil

}

var aggregationSQL = map[string]string{

	"loan_app_features": `
		SELECT CAST(JSON_OBJECT(
			'client_id',            ?,
			'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
			'total_applications',   COUNT(*),
			'apps_last_30d',        SUM(CASE WHEN applied_at >= NOW() - INTERVAL 30 DAY THEN 1 ELSE 0 END),
			'apps_last_90d',        SUM(CASE WHEN applied_at >= NOW() - INTERVAL 90 DAY THEN 1 ELSE 0 END),
			'approved_count',       SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END),
			'rejected_count',       SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END),
			'approval_rate',        ROUND(SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4),
			'max_requested_amount', MAX(requested_amount),
			'avg_requested_amount', ROUND(AVG(requested_amount), 2),
			'last_application_at',  MAX(applied_at),
			'distinct_channels',    COUNT(DISTINCT channel)
		) AS CHAR) AS features
		from loan_applications
		where client_id = ?`,

	"payment_features": `
		select CAST(JSON_OBJECT(
			'client_id',            ?,
			'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
			'total_payment_events', COUNT(*),
			'on_time_payments',     SUM(CASE WHEN event_type = 'PAYMENT' AND days_overdue = 0 THEN 1 ELSE 0 END),
			'missed_payments',      SUM(CASE WHEN event_type = 'MISSED' THEN 1 ELSE 0 END),
			'missed_last_90d',      SUM(CASE WHEN event_type = 'MISSED' AND event_at >= NOW() - INTERVAL 90 DAY THEN 1 ELSE 0 END),
			'max_days_overdue',     MAX(days_overdue),
			'ever_90dpd',           MAX(CASE WHEN days_overdue >= 90 THEN 1 ELSE 0 END),
			'total_paid_amount',    SUM(COALESCE(actual_amount, 0)),
			'total_penalties',      SUM(COALESCE(penalty_amount, 0)),
			'last_payment_at',      MAX(CASE WHEN event_type IN ('PAYMENT','PARTIAL_PAYMENT') THEN event_at END)
		) AS CHAR) AS features
		from loan_payment_events
		where client_id = ?`,

	"operation_features": `
		select CAST(JSON_OBJECT(
			'client_id',            ?,
			'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
			'total_operations',     COUNT(*),
			'disbursement_count',   SUM(CASE WHEN operation_type = 'DISBURSEMENT' THEN 1 ELSE 0 END),
			'topup_count',          SUM(CASE WHEN operation_type = 'TOPUP' THEN 1 ELSE 0 END),
			'suspicious_op_count',  SUM(CASE WHEN is_suspicious = 1 THEN 1 ELSE 0 END),
			'distinct_devices',     COUNT(DISTINCT device_id),
			'foreign_op_count',     SUM(CASE WHEN ip_country != 'KZ' AND ip_country IS NOT NULL THEN 1 ELSE 0 END),
			'total_disbursed',      SUM(CASE WHEN operation_type = 'DISBURSEMENT' THEN COALESCE(amount, 0) ELSE 0 END),
			'last_operation_at',    MAX(operation_at)
		) AS CHAR) AS features
		from loan_operation_events
		where client_id = ?`,

	"money_features": `
		select CAST(JSON_OBJECT(
			'client_id',            ?,
			'computed_at',          DATE_FORMAT(NOW(3), '%Y-%m-%dT%H:%i:%s.%fZ'),
			'avg_monthly_balance',  ROUND(AVG(CASE WHEN event_type = 'MONTH_END_BALANCE' THEN balance_after END), 2),
			'min_monthly_balance',  MIN(CASE WHEN event_type = 'MONTH_END_BALANCE' THEN balance_after END),
			'max_monthly_balance',  MAX(CASE WHEN event_type = 'MONTH_END_BALANCE' THEN balance_after END),
			'total_salary_credits', SUM(CASE WHEN event_type = 'SALARY_CREDIT' THEN amount ELSE 0 END),
			'avg_salary_credit',    ROUND(AVG(CASE WHEN event_type = 'SALARY_CREDIT' THEN amount END), 2),
			'large_debit_count',    COUNT(CASE WHEN event_type = 'LARGE_DEBIT' THEN 1 END),
			'net_flow_last_30d',    SUM(CASE
				WHEN event_at >= NOW() - INTERVAL 30 DAY AND event_type IN ('TRANSFER_IN','SALARY_CREDIT','LARGE_CREDIT') THEN amount
				WHEN event_at >= NOW() - INTERVAL 30 DAY AND event_type IN ('TRANSFER_OUT','LARGE_DEBIT') THEN -amount
				ELSE 0 END),
			'last_salary_at',       MAX(CASE WHEN event_type = 'SALARY_CREDIT' THEN event_at END)
		) AS CHAR) AS features
		from client_money_events
		where client_id = ?`,
}
