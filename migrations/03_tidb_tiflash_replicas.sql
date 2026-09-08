-- Adding TiFlash (OLAP) replicas alongside TiKV for aggregation queries performance improvement
-- (only if tidb cluster supports TiFlash replicas)
ALTER TABLE loan_applications    SET TIFLASH REPLICA 1;
ALTER TABLE loan_payment_events  SET TIFLASH REPLICA 1;
ALTER TABLE loan_operation_events SET TIFLASH REPLICA 1;
ALTER TABLE client_money_events  SET TIFLASH REPLICA 1;
ALTER TABLE loan_accounts        SET TIFLASH REPLICA 1;
ALTER TABLE clients              SET TIFLASH REPLICA 1;
