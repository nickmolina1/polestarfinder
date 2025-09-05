import os, glob, re, sys
import psycopg2 as psycopg
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()
DSN = os.getenv("PG_DSN")

if not DSN:
    raise RuntimeError("PG_DSN not set! Did you create .env?")


def read_sql_statements(path: str):
    """
    Naive splitter: remove /* */ and -- comments, then split on semicolons.
    Handles blank lines; skips empty statements.
    """
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    # strip /* ... */ comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    # strip -- comments
    sql = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    # split on semicolons
    parts = [s.strip() for s in sql.split(";")]
    return [p for p in parts if p]


def ensure_migrations_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS _migrations (
              id TEXT PRIMARY KEY,
              applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """
        )


def get_applied(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM _migrations ORDER BY id;")
        return {r[0] for r in cur.fetchall()}


def apply_migration(conn, mig_id, path):
    stmts = read_sql_statements(path)
    if not stmts:
        print(f"[SKIP] {mig_id}: no statements")
        return
    print(f"[APPLY] {mig_id}: {len(stmts)} statements")
    with conn.cursor() as cur:
        for i, stmt in enumerate(stmts, 1):
            try:
                cur.execute(stmt)
            except Exception as e:
                print(
                    f"\n[ERROR] {mig_id} stmt#{i}\n{stmt}\n--> {e}\n", file=sys.stderr
                )
                raise
        cur.execute(
            "INSERT INTO _migrations (id) VALUES (%s) ON CONFLICT DO NOTHING;",
            (mig_id,),
        )


def main():
    if not DSN:
        print("PG_DSN not set", file=sys.stderr)
        sys.exit(1)

    files = sorted(glob.glob("database/sql/migrations/*.sql"))
    if not files:
        print("No migration files found in sql/migrations")
        sys.exit(1)

    with psycopg.connect(DSN) as conn:
        ensure_migrations_table(conn)
        applied = get_applied(conn)
        to_apply = [f for f in files if os.path.basename(f) not in applied]
        if not to_apply:
            print("No new migrations.")
            return
        for path in to_apply:
            mig_id = os.path.basename(path)
            apply_migration(conn, mig_id, path)
        conn.commit()
        print("Migrations complete.")


if __name__ == "__main__":
    main()
