# risk-scoring-streaming

A real-time credit risk feature pipeline. Kafka events go in, precomputed risk
features come out over HTTP.

```
Kafka topics ──▶ risk-feature-consumer (Python) ──▶ TiDB (events + kv_cache)
                                                          │
                                        risk-feature-api (Go) ──▶ HTTP /v1
```

## Services

| Service | What it does |
|---|---|
| [risk-feature-consumer/](risk-feature-consumer/) | Consumes four Kafka topics, writes events to TiDB, computes aggregate features into the `kv_cache` table. Reads its topic and feature-group config from Postgres. |
| [risk-feature-api/](risk-feature-api/) | Serves features from `kv_cache`, computing them live on a cache miss. Listens on `:8080`. |

Topics: `loan-applications`, `loan-payments`, `loan-operations`, `client-money` —
one consumer process per topic.

## API

| Method | Path |
|---|---|
| `GET` | `/v1/health` |
| `GET` | `/v1/risk-profile/:client_id` |
| `GET` | `/v1/risk-profile/:client_id/:group_id` |
| `DELETE` | `/v1/risk-profile/:client_id/cache` |

## Infrastructure

Kafka, TiDB, and Postgres are managed services outside the cluster. Both services
read all configuration from environment variables.
