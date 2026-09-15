#!/usr/bin/env bash
#
# Apply schemas and seed data to TiDB and PostgreSQL.
#
# Idempotent: safe to run repeatedly. Schemas use CREATE TABLE IF NOT EXISTS;
# seed data is loaded only when the target table is empty.
#
#   cp .env.example .env    # fill in your endpoints
#   ./scripts/migrate.sh
#
# Or with variables already exported:
#   ./scripts/migrate.sh
#
# Requires: mysql client, psql client.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MIG_DIR="${MIG_DIR:-$ROOT_DIR/migrations}"
RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; DIM=$'\033[2m'; OFF=$'\033[0m'

info()  { echo "${DIM}→${OFF} $*"; }
ok()    { echo "${GREEN}✓${OFF} $*"; }
warn()  { echo "${YELLOW}!${OFF} $*"; }
die()   { echo "${RED}✗${OFF} $*" >&2; exit 1; }


# ── load .env if present ──────────────────────────────────────
if [[ -f "$ROOT_DIR/.env" ]]; then
    info "loading $ROOT_DIR/.env"
    set -a
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.env"
    set +a
fi


# ── preflight ─────────────────────────────────────────────────
command -v mysql >/dev/null || die "mysql client not found (apt install mysql-client / brew install mysql-client)"
command -v psql  >/dev/null || die "psql client not found (apt install postgresql-client / brew install libpq)"

required=(TIDB_HOST TIDB_PORT TIDB_USER TIDB_PASSWORD TIDB_DATABASE
          PG_HOST PG_PORT PG_USER PG_PASSWORD PG_DATABASE)
missing=()
for var in "${required[@]}"; do
    [[ -n "${!var:-}" ]] || missing+=("$var")
done
if (( ${#missing[@]} )); then
    echo "${RED}✗${OFF} missing required variables:" >&2
    printf '    %s\n' "${missing[@]}" >&2
    echo >&2
    echo "  cp .env.example .env  and fill it in, or export them." >&2
    exit 1
fi

for f in 01_tidb_schema.sql 02_postgres_config_schema.sql \
         clients_insert.sql loan_accounts_insert.sql; do
    [[ -f "$MIG_DIR/$f" ]] || die "missing $MIG_DIR/$f — run this from the repo root"
done

# ── mysql client flag differs between MySQL and MariaDB builds ─
if mysql --help 2>&1 | grep -q -- '--ssl-mode'; then
    TLS_FLAG=(--ssl-mode=REQUIRED)
else
    TLS_FLAG=(--ssl)
    warn "MariaDB client detected — using --ssl instead of --ssl-mode"
fi

my() {
    mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
        "${TLS_FLAG[@]}" --connect-timeout=10 "$@"
}

PG_URI="host=$PG_HOST port=$PG_PORT user=$PG_USER dbname=$PG_DATABASE sslmode=require connect_timeout=10"
PG_ADMIN_URI="host=$PG_HOST port=$PG_PORT user=$PG_USER dbname=defaultdb sslmode=require connect_timeout=10"
export PGPASSWORD="$PG_PASSWORD"


# ── TiDB ──────────────────────────────────────────────────────
echo
echo "TiDB  ${DIM}$TIDB_HOST:$TIDB_PORT${OFF}"

my -e "SELECT 1" >/dev/null 2>&1 || die "cannot connect to TiDB — check host, credentials and IP allowlist"
ok "connected"

my -e "CREATE DATABASE IF NOT EXISTS \`$TIDB_DATABASE\`"
my "$TIDB_DATABASE" < "$MIG_DIR/01_tidb_schema.sql"
TABLES=$(my -N -B "$TIDB_DATABASE" -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$TIDB_DATABASE'")
ok "schema applied — $TABLES tables"

# Seed inserts are plain INSERTs, so guard on an empty table rather than
# relying on conflict handling that the generated files do not have.
CLIENTS=$(my -N -B "$TIDB_DATABASE" -e "SELECT COUNT(*) FROM clients")
if [[ "$CLIENTS" == "0" ]]; then
    info "loading seed data"
    my "$TIDB_DATABASE" < "$MIG_DIR/clients_insert.sql"
    my "$TIDB_DATABASE" < "$MIG_DIR/loan_accounts_insert.sql"
    CLIENTS=$(my -N -B "$TIDB_DATABASE" -e "SELECT COUNT(*) FROM clients")
    LOANS=$(my -N -B "$TIDB_DATABASE" -e "SELECT COUNT(*) FROM loan_accounts")
    ok "seeded — $CLIENTS clients, $LOANS loans"
else
    LOANS=$(my -N -B "$TIDB_DATABASE" -e "SELECT COUNT(*) FROM loan_accounts")
    ok "seed data already present — $CLIENTS clients, $LOANS loans"
fi


# ── PostgreSQL ────────────────────────────────────────────────
# ── PostgreSQL ────────────────────────────────────────────────
echo
echo "PostgreSQL  ${DIM}$PG_HOST:$PG_PORT  db=$PG_DATABASE${OFF}"

# Providers differ on which database exists by default: Aiven gives you
# 'defaultdb', Supabase 'postgres'. Default to the target database itself,
# which is correct whenever it already exists.
PG_ADMIN_DB="${PG_ADMIN_DB:-$PG_DATABASE}"
PG_ADMIN_URI="host=$PG_HOST port=$PG_PORT user=$PG_USER dbname=$PG_ADMIN_DB sslmode=require connect_timeout=10"
PG_URI="host=$PG_HOST port=$PG_PORT user=$PG_USER dbname=$PG_DATABASE sslmode=require connect_timeout=10"

if ! PG_ERR=$(psql "$PG_ADMIN_URI" -c "SELECT 1" 2>&1 >/dev/null); then
    echo "${RED}✗${OFF} cannot connect to PostgreSQL" >&2
    echo "    $PG_ERR" >&2
    exit 1
fi
ok "connected"

# Only create the target database when connecting through a different one.
# Managed Postgres often forbids CREATE DATABASE entirely.
if [[ "$PG_ADMIN_DB" != "$PG_DATABASE" ]]; then
    if ! psql "$PG_ADMIN_URI" -tAc \
            "SELECT 1 FROM pg_database WHERE datname='$PG_DATABASE'" | grep -q 1; then
        info "creating database $PG_DATABASE"
        psql "$PG_ADMIN_URI" -c "CREATE DATABASE \"$PG_DATABASE\""
    fi
fi

psql "$PG_URI" -v ON_ERROR_STOP=1 -q -f "$MIG_DIR/02_postgres_config_schema.sql"

TOPICS=$(psql "$PG_URI" -tAc "SELECT COUNT(*) FROM pipeline_config.kafka_topics")
GROUP_COUNT=$(psql "$PG_URI" -tAc "SELECT COUNT(*) FROM pipeline_config.feature_groups")
ok "schema applied — $TOPICS topics, $GROUP_COUNT feature groups"


# ── summary ───────────────────────────────────────────────────
echo
if [[ "$CLIENTS" == "200" && "$TOPICS" == "4" && "$GROUP_COUNT" == "4" ]]; then
    ok "migrations complete"
else
    warn "migrations ran, but counts are unexpected:"
    warn "  clients=$CLIENTS (expected 200)  topics=$TOPICS (4)  groups=$GROUP_COUNT (4)"
fi
echo
