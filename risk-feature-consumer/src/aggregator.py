import json
import db
import structlog
from config import PipelineConfig

log = structlog.get_logger()


def compute_and_cache(
        pool: db.TiDB,
        config: PipelineConfig,
        client_ids: set[str]
) -> dict[str, int]:
    stats = {fg.group_id: 0 for fg in config.feature_groups}
    aggr_queries = config.sql_queries
    if not aggr_queries:
        return stats

    for client_id in client_ids:
        for fg in config.feature_groups:
            query = aggr_queries.get(fg.group_id)
            if not query:
                continue
            cache_key = fg.cache_key_template.replace("{client_id}", client_id)
            row = _run_aggregation(pool, query, client_id)
            if not row or row.get("features") is None:
                log.warning("aggregation_empty", group_id=fg.group_id, client_id=client_id)
                continue
            features = row["features"]
            if isinstance(features, (str, bytes, bytearray)):
                features = json.loads(features)
            db.write_kv_cache(pool, cache_key, features, fg.ttl_seconds)
            stats[fg.group_id] += 1
    return stats


def _strip_line_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


def _run_aggregation(
        pool: db.TiDB,
        sql: str,
        client_id: str
):
    stripped = _strip_line_comments(sql)
    n_params = stripped.count(":client_id")
    prepared = stripped.replace("%", "%%").replace(":client_id", "%s")

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(prepared, (client_id,) * n_params)
            return cur.fetchone()
