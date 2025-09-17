# database/pgdsn.py
import json
import os
import socket
import logging
from urllib.parse import quote_plus

import boto3

_REGION = os.getenv("AWS_REGION", "us-east-1")
_SECRET_ARN = os.getenv("PG_SECRET_ARN")

_cached_dsn = None
_debug = os.getenv("PG_DSN_DEBUG")
_logger = logging.getLogger("pgdsn")


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

    # Local/dev override (full DSN provided)
    dsn_env = os.getenv("PG_DSN")
    if dsn_env:
        dsn_env = _augment_dsn(dsn_env)
        _cached_dsn = dsn_env
        if _debug:
            _log_debug_details(_cached_dsn, source="PG_DSN env")
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

    _cached_dsn = build_base_dsn(user, pwd_q, host, port, db)
    if _debug:
        _log_debug_details(_cached_dsn, source="secrets")
    return _cached_dsn


def build_base_dsn(user: str, pwd_q: str, host: str, port: str, db: str) -> str:
    sslmode = os.getenv("PG_SSLMODE", "require")  # set to 'disable' for local docker
    base = f"postgresql://{user}:{pwd_q}@{host}:{port}/{db}?sslmode={sslmode}&connect_timeout=5"
    return _augment_dsn(base)


def _augment_dsn(dsn: str) -> str:
    # Ensure connect_timeout present
    if "connect_timeout=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "connect_timeout=5"
    # If sslmode absent, add (default require)
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + f"sslmode={os.getenv('PG_SSLMODE','require')}"
    return dsn


def _log_debug_details(dsn: str, source: str):
    try:
        host = dsn.split("@", 1)[1].split("/", 1)[0].split(":", 1)[0]
    except Exception:
        host = "?"
    resolved = []
    try:
        for fam, _, _, _, sockaddr in socket.getaddrinfo(host, None):  # type: ignore
            ip = sockaddr[0]
            if ip not in resolved:
                resolved.append(ip)
    except Exception as e:  # pragma: no cover
        resolved = [f"resolve_error:{e}"]
    masked = _mask_credentials(dsn)
    _logger.info("pgdsn debug: source=%s host=%s ips=%s dsn=%s", source, host, resolved, masked)


def _mask_credentials(dsn: str) -> str:
    try:
        prefix, rest = dsn.split("//", 1)
        creds, remainder = rest.split("@", 1)
        if ":" in creds:
            user, _ = creds.split(":", 1)
            return f"{prefix}//{user}:***@{remainder}"
    except Exception:
        return dsn
    return dsn
