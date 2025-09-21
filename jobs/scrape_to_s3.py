# jobs/scrape_to_s3.py
from __future__ import annotations

import datetime as dt
import json
import logging
import os

import boto3

import scraper.scraper as scraper  # your scraper.fetch_raw()
from scraper.filters import filters as FILTERS  # type: ignore

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
if not root_logger.handlers:
    logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("scrape_to_s3")
log.setLevel(LOG_LEVEL)

REGION = os.getenv("AWS_REGION", "us-east-1")
RAW_BUCKET = os.getenv("RAW_BUCKET")  # e.g., staging.polestarfinder.com
RAW_KEY = os.getenv("RAW_KEY", "raw/latest.json")  # where we store latest snapshot

s3 = boto3.client("s3", region_name=REGION)


def _timestamped_key() -> str:
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    return f"raw/{ts}/inventory.json"


def handler(event=None, context=None):
    log.info(
        "startup: LOG_LEVEL=%s, SKIP_DEEP_SCAN=%r, event_has_skip=%s",
        os.getenv("LOG_LEVEL"),
        os.getenv("SKIP_DEEP_SCAN"),
        isinstance(event, dict) and ("skip_deep_scan" in event),
    )
    # 1) Scrape (internet OK, this lambda is NOT in a VPC)
    data = scraper.fetch_raw()  # uses MODELS, MARKET, PAGE_LIMIT envs
    log.info("scraped vehicles=%d", len(data))

    # 1b) Optional deep feature scans (outside VPC): enrich wheels/motor/packages
    skip_scan = False
    reason = ""
    try:
        if isinstance(event, dict) and ("skip_deep_scan" in event):
            skip_scan = bool(event.get("skip_deep_scan"))
            reason = "event override"
        elif os.getenv("SKIP_DEEP_SCAN", "").lower() in {"1", "true", "yes"}:
            skip_scan = True
            reason = "env SKIP_DEEP_SCAN"
    except Exception:
        pass

    if not skip_scan and data:
        log.info("feature-scan: starting (outside VPC)")
        # Build reverse lists from FILTERS
        wheel_defs = []
        package_defs = []
        motor_defs = []
        for label, mapping in FILTERS.items():
            for category, code in mapping.items():
                c_low = category.lower()
                if c_low == "wheels":
                    wheel_defs.append((label, code))
                elif c_low == "package":
                    package_defs.append((label, code))
                elif c_low == "motor":
                    motor_defs.append((label, code))

        # Accumulators: id -> label/flags
        wheels_by_id: dict[str, str] = {}
        motors_by_id: dict[str, str] = {}
        pkg_perf: set[str] = set()
        pkg_pilot: set[str] = set()
        pkg_plus: set[str] = set()

        # Package label -> target set
        package_target = {
            "Performance": pkg_perf,
            "Pilot": pkg_pilot,
            "Plus": pkg_plus,
        }

        model = scraper.DEFAULT_MODELS[0] if scraper.DEFAULT_MODELS else "PS2"
        market = scraper.DEFAULT_MARKET

        # Wheels
        total_wheel_ids = 0
        for human, code in wheel_defs:
            try:
                ids = scraper.fetch_ids_for_filter("Wheels", code, model, market)
            except Exception as e:
                log.warning("feature-scan: wheels code=%s failed: %s", code, e)
                ids = set()
            for vid in ids:
                wheels_by_id[vid] = human
            total_wheel_ids += len(ids)
        log.info("feature-scan: wheels matched ids=%d unique=%d", total_wheel_ids, len(wheels_by_id))

        # Motors
        total_motor_ids = 0
        for human, code in motor_defs:
            try:
                ids = scraper.fetch_ids_for_filter("Motor", code, model, market)
            except Exception as e:
                log.warning("feature-scan: motor code=%s failed: %s", code, e)
                ids = set()
            for vid in ids:
                motors_by_id[vid] = human
            total_motor_ids += len(ids)
        log.info("feature-scan: motors matched ids=%d unique=%d", total_motor_ids, len(motors_by_id))

        # Packages
        total_pkg_ids = 0
        for human, code in package_defs:
            target = package_target.get(human)
            if target is None:
                continue
            try:
                ids = scraper.fetch_ids_for_filter("Package", code, model, market)
            except Exception as e:
                log.warning("feature-scan: package %s code=%s failed: %s", human, code, e)
                ids = set()
            target.update(ids)
            total_pkg_ids += len(ids)
        log.info(
            "feature-scan: packages matched ids=%d perf=%d pilot=%d plus=%d",
            total_pkg_ids,
            len(pkg_perf),
            len(pkg_pilot),
            len(pkg_plus),
        )

        # Apply enrichment onto vehicles
        wheels_set = motors_set = perf_set = pilot_set = plus_set = 0
        for v in data:
            vid = str(v.get("id"))
            if vid in wheels_by_id:
                v["wheels"] = wheels_by_id[vid]
                wheels_set += 1
            if vid in motors_by_id:
                v["motor"] = motors_by_id[vid]
                motors_set += 1
            if vid in pkg_perf:
                v["performance"] = True
                perf_set += 1
            if vid in pkg_pilot:
                v["pilot"] = True
                pilot_set += 1
            if vid in pkg_plus:
                v["plus"] = True
                plus_set += 1

        log.info(
            "feature-scan: applied wheels=%d motors=%d performance=%d pilot=%d plus=%d over total=%d",
            wheels_set,
            motors_set,
            perf_set,
            pilot_set,
            plus_set,
            len(data),
        )
    else:
        log.info("feature-scan: skipped (%s)", reason or "not requested")

    body = json.dumps({"vehicles": data}, separators=(",", ":")).encode("utf-8")

    # 2) Write timestamped snapshot
    tkey = _timestamped_key()
    s3.put_object(Bucket=RAW_BUCKET, Key=tkey, Body=body, ContentType="application/json")
    log.info("wrote s3://%s/%s", RAW_BUCKET, tkey)

    # 3) Overwrite 'latest' pointer (stable key the loader will read)
    s3.put_object(Bucket=RAW_BUCKET, Key=RAW_KEY, Body=body, ContentType="application/json")
    log.info("updated s3://%s/%s", RAW_BUCKET, RAW_KEY)

    return {"ok": True, "vehicles": len(data), "snapshot_key": tkey, "latest_key": RAW_KEY}
