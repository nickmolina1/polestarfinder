# database/pgdsn.py
import os, json
import boto3

_REGION = os.getenv("AWS_REGION", "us-east-1")
_SECRET_ARN = os.getenv("PG_SECRET_ARN")

_cached_dsn = None

def get_pg_dsn() -> str:
    """
    Resolve the Postgres DSN from Secrets Manager if PG_SECRET_ARN is set,
    else fall back to plain PG_DSN. Adds a short connect timeout.
    """
    global _cached_dsn
    if _cached_dsn:
        return _cached_dsn

    # 1) If a full DSN is explicitly provided, use it (handy for local/dev)
    dsn = os.getenv("PG_DSN")
    if dsn:
        if "connect_timeout" not in dsn:
            dsn += ("&" if "?" in dsn else "?") + "connect_timeout=5"
        _cached_dsn = dsn
        return _cached_dsn

    # 2) Otherwise, fetch from Secrets Manager
    if not _SECRET_ARN:
        raise RuntimeError("Neither PG_DSN nor PG_SECRET_ARN is set")

    sm = boto3.client("secretsmanager", region_name=_REGION)
    resp = sm.get_secret_value(SecretId=_SECRET_ARN)
    secret_str = resp.get("SecretString", "")

    # Your secret can be a JSON object or just the password string.
    try:
        data = json.loads(secret_str)
        user = data["username"]
        pwd  = data["password"]
        host = data["host"]
        port = data.get("port", 5432)
        db   = data.get("dbname") or data.get("database") or os.getenv("PG_DBNAME", "postgres")
    except (json.JSONDecodeError, KeyError):
        # Secret is just the password; get the rest from env
        pwd  = secret_str
        user = os.environ["PG_USER"]
        host = os.environ["PG_HOST"]
        port = os.getenv("PG_PORT", "5432")
        db   = os.environ["PG_DBNAME"]

    _cached_dsn = f"postgresql://{user}:{pwd}@{host}:{port}/{db}?connect_timeout=5&sslmode=require"
    return _cached_dsn
