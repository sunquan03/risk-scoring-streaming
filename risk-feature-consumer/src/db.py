from socket import create_connection

import pymysql
import pymysql.cursors
import threading
from contextlib import contextmanager
from typing import Optional


class TiDB:
    def __init__(self, host: str, port: int, user: str, password: str, database: str, pool_size: int = 10):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool_size = pool_size

        self._conn_kwargs = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": False,
            "connect_timeout": 5,

        }

        self._pool: list[pymysql.connections.Connection] = []
        self._lock = threading.Lock()

        # pre-filling pool
        for _ in range(self.pool_size):
            self._pool.append(self.create_conn())

    def create_conn(self) -> pymysql.connections.Connection:
        return pymysql.connect(**self._conn_kwargs)

    def acquire(self):
        with self._lock:
            if self._pool:
                conn = self._pool.pop()
            else:
                return self.create_conn()

        try:
            conn.ping(reconnect=True)
            return conn
        except Exception:
            return self.create_conn()


    def release(self, conn: pymysql.connections.Connection):
        with self._lock:
            self._pool.append(conn)

    @contextmanager
    def connection(self) :
        conn = self.acquire()
        try:
            yield conn
        finally:
            self.release(conn)


_pool_instance: Optional[TiDB] = None
_pool_lock = threading.Lock()


def get_pool(host: str, port: int, user: str, password: str, database: str, pool_size: int = 10) -> Optional[TiDB]:
    global _pool_instance
    with _pool_lock:
        if _pool_instance is None:
            _pool_instance = TiDB(host, port, user, password, database, pool_size)
    return _pool_instance



def _pk_for_table(table: str) -> str:
    pk_map = {
        "loan_applications":     "application_id",
        "loan_payment_events":   "event_id",
        "loan_operation_events": "event_id",
        "client_money_events":   "event_id",
    }
    return pk_map.get(table, "id")



def upsert_batch(pool: TiDB, table: str, rows: list[dict]) -> int:
    pk = _pk_for_table(table)

    all_columns = rows[0].keys()
    upd_columns = [col for col in all_columns if col != pk]

    columns = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(upd_columns))

    update_clause = ", ".join(f"{col} = VALUES({col})" for col in upd_columns)

    row_vals = []
    for row in rows:
        row_vals.append(tuple(row[col]) for col in all_columns)

    vals_placeholders = ", ".join([f"{placeholders}"] * len(row_vals))
    query = f"""
        INSERT INTO `{table}` ({columns}) VALUES ({vals_placeholders})
        ON DUPLICATE KEY UPDATE {update_clause}
    """

    print(f"QUERY: {query}")

    params = [val for rv in row_vals for val in rv]

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            affected_rows = cur.rowcount
            conn.commit()

    return affected_rows
