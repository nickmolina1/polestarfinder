import os, psycopg2
from contextlib import contextmanager

DSN = os.getenv("PG_DSN")


@contextmanager
def conn():
    if not DSN:
        raise RuntimeError("PG_DSN not set")
    with psycopg2.connect(DSN) as c:
        yield c


def fetch_all(sql, params=None):
    with conn() as c, c.cursor(row_factory=psycopg2.rows.dict_row) as cur:
        cur.execute(sql, params or {})
        return cur.fetchall()


def fetch_one(sql, params=None):
    with conn() as c, c.cursor(row_factory=psycopg2.rows.dict_row) as cur:
        cur.execute(sql, params or {})
        return cur.fetchone()


def execute(sql, params=None):
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params or {})
        c.commit()
