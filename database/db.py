from contextlib import contextmanager
import logging
import os

import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from database.pgdsn import get_pg_dsn

load_dotenv()  # take environment variables from .env.

log = logging.getLogger("db")


def get_conn():
    """Return a raw psycopg2 connection. Caller must close.

    Adds minimal logging when LOG_LEVEL includes INFO. Uses cached DSN builder.
    """
    dsn = get_pg_dsn()
    if log.isEnabledFor(logging.INFO):
        log.info("db: connecting host=%s sslmode=%s", _extract_host(dsn), _extract_sslmode(dsn))
    try:
        c = psycopg2.connect(dsn)
    except OperationalError as e:
        msg = str(e)
        # Auto-downgrade sslmode for typical local dev case
        if "does not support SSL" in msg and "localhost" in dsn:
            if "sslmode=require" in dsn:
                no_ssl_dsn = dsn.replace("sslmode=require", "sslmode=disable")
                if log.isEnabledFor(logging.WARNING):
                    log.warning("db: retrying without SSL (auto fallback): host=localhost")
                c = psycopg2.connect(no_ssl_dsn)
            else:
                raise
        else:
            raise
    if log.isEnabledFor(logging.INFO):
        log.info("db: connected pid=%s", getattr(c, "get_backend_pid", lambda: None)())
    return c


def _extract_host(dsn: str) -> str:
    try:
        # naive parse
        after_at = dsn.split("@", 1)[1]
        host_port = after_at.split("/", 1)[0]
        return host_port.split(":", 1)[0]
    except Exception:
        return "?"


def _extract_sslmode(dsn: str) -> str:
    if "sslmode=" in dsn:
        return dsn.split("sslmode=", 1)[1].split("&", 1)[0]
    return "?"


@contextmanager
def conn():
    with get_conn() as c:
        yield c


def fetch_all(sql, params=None):
    with conn() as c, c.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()


def fetch_one(sql, params=None):
    with conn() as c, c.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql, params or {})
        return cur.fetchone()


def execute(sql, params=None):
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params or {})
        c.commit()
