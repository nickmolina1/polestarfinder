import os, psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
from psycopg2.extras import DictCursor

load_dotenv()


DSN = os.getenv("PG_DSN")


@contextmanager
def conn():
    if not DSN:
        raise RuntimeError("PG_DSN not set")
    with psycopg2.connect(DSN) as c:
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
