# database/pgdsn.py
import json
import os
from urllib.parse import quote_plus

import boto3

_REGION = os.getenv("AWS_REGION", "us-east-1")
_SECRET_ARN = os.getenv("PG_SECRET_ARN")

_cached_dsn = None


def get_pg_dsn() -> str:
    """
    Build DSN from a Secrets Manager secret that contains:
    { "username": "...", "password": "..." }
    and env vars PG_HOST, PG_PORT, PG_DBNAME.
    Falls back to PG_DSN if set (for local dev).
    """
    global _cached_dsn
    if _cached_dsn:
        return _cached_dsn

    # Local/dev override
    dsn_env = os.getenv("PG_DSN")
    if dsn_env:
        if "sslmode=" not in dsn_env:
            dsn_env += ("&" if "?" in dsn_env else "?") + "sslmode=require"
        if "connect_timeout=" not in dsn_env:
            dsn_env += ("&" if "?" in dsn_env else "?") + "connect_timeout=5"
        _cached_dsn = dsn_env
        return _cached_dsn

    if not _SECRET_ARN:
        raise RuntimeError("Set PG_SECRET_ARN or PG_DSN")

    sm = boto3.client("secretsmanager", region_name=_REGION)
    secret = sm.get_secret_value(SecretId=_SECRET_ARN)["SecretString"]
    data = json.loads(secret)  # must contain username, password

    user = data["username"]
    pwd = data["password"]
    host = os.environ["PG_HOST"]
    port = os.getenv("PG_PORT", "5432")
    db = os.environ["PG_DBNAME"]

    # URL-encode just in case the password has special characters
    pwd_q = quote_plus(pwd)

    _cached_dsn = (
        f"postgresql://{user}:{pwd_q}@{host}:{port}/{db}" f"?sslmode=require&connect_timeout=5"
    )
    return _cached_dsn
