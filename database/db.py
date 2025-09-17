from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from database.pgdsn import get_pg_dsn

load_dotenv()  # take environment variables from .env.


@contextmanager
def conn():
    with psycopg2.connect(get_pg_dsn()) as c:
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
