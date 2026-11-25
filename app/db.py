"""One small connection pool for the app.

min_size=0 and a short max_idle let idle connections close, so a managed
database that suspends when idle can do so. check= tests each connection
before use, because an instance that was paused or restored from a snapshot
may hold connections the server has already closed.
"""

import os
import threading

from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None
_lock = threading.Lock()


def pool() -> ConnectionPool:
    global _pool
    with _lock:
        if _pool is None:
            _pool = _open()
    return _pool


def _open() -> ConnectionPool:
    p = ConnectionPool(
        os.environ["DATABASE_URL"],
        min_size=0,
        max_size=int(os.environ.get("DATABASE_POOL_SIZE", "5")),
        max_idle=30,
        check=ConnectionPool.check_connection,
        open=False,
    )
    p.open()
    return p


def close() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
