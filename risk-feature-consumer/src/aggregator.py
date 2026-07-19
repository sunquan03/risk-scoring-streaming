from typing import Any
import json
import db
from config import PipelineConfig
from datetime import datetime, timedelta, timezone

def compute_and_cache(
        pool: db.TiDB,
        config: PipelineConfig,
        client_ids: set[str]
) -> dict[str, int]:
    stats = {fg.group_id: 0 for fg in config.feature_groups}
    aggr_queries = config.sql_queries
    if not aggr_queries:
        return

    for client_id in client_ids:
        for fg in config.feature_groups:
            query = aggr_queries.get(fg.group_id)
            if not query:
                continue
            cache_key = fg.cache_key_template.replace("{client_id}", client_id)
            res = _run_aggregation(pool, query, client_id)
            features = res["features"]
            if isinstance(features, str):
                features = json.loads(features)
            db.write_kv_cache(pool, cache_key, features, fg.ttl_seconds)
            stats[fg.group_id] += 1
    return stats

def _run_aggregation(
        pool: db.TiDB,
        sql: str,
        client_id: str
):
    with pool.connection() as conn:
        cursor = conn.cursor()
        sql = sql.replace(":client_id", '%s')
        cnt = sql.count("%s")
        sql_params = (client_id,) * cnt
        cursor.execute(sql, params=sql_params)
        row = cursor.fetchone()
        return row

