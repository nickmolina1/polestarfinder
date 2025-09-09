# jobs/scrape_to_s3.py
from __future__ import annotations

import os, json, datetime as dt, logging
import boto3
import scraper.scraper as scraper  # your scraper.fetch_raw()

log = logging.getLogger("scrape_to_s3")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

REGION      = os.getenv("AWS_REGION", "us-east-1")
RAW_BUCKET  = os.getenv("RAW_BUCKET")              # e.g., staging.polestarfinder.com
RAW_KEY     = os.getenv("RAW_KEY", "raw/latest.json")  # where we store latest snapshot

s3 = boto3.client("s3", region_name=REGION)

def _timestamped_key() -> str:
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    return f"raw/{ts}/inventory.json"

def handler(event=None, context=None):
    # 1) Scrape (internet OK, this lambda is NOT in a VPC)
    data = scraper.fetch_raw()  # uses MODELS, MARKET, PAGE_LIMIT envs
    log.info("scraped vehicles=%d", len(data))

    body = json.dumps({"vehicles": data}, separators=(",", ":")).encode("utf-8")

    # 2) Write timestamped snapshot
    tkey = _timestamped_key()
    s3.put_object(Bucket=RAW_BUCKET, Key=tkey, Body=body, ContentType="application/json")
    log.info("wrote s3://%s/%s", RAW_BUCKET, tkey)

    # 3) Overwrite 'latest' pointer (stable key the loader will read)
    s3.put_object(Bucket=RAW_BUCKET, Key=RAW_KEY, Body=body, ContentType="application/json")
    log.info("updated s3://%s/%s", RAW_BUCKET, RAW_KEY)

    return {"ok": True, "vehicles": len(data), "snapshot_key": tkey, "latest_key": RAW_KEY}
