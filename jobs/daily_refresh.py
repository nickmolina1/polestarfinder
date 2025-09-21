# jobs/daily_refresh.py
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from psycopg2.extras import Json

import scraper.scraper as scraper  # your library-style scraper.py
from scraper.filters import filters as FILTERS  # type: ignore
from database.db import execute, fetch_all, fetch_one, execute_values

# ----------------- Config -----------------

RAW_BUCKET = os.getenv("RAW_BUCKET")
RAW_KEY = os.getenv("RAW_KEY")  # e.g., raw/latest.json
REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("PUBLIC_BUCKET", "local")  # "local" => write to disk
KEY_PREFIX = os.getenv("PUBLIC_KEY_PREFIX", "")  # e.g., "staging/" or ""
EXPORT_KEY = (KEY_PREFIX + "data/vehicles.json") if KEY_PREFIX else "data/vehicles.json"

s3 = boto3.client("s3", region_name=REGION)
log = logging.getLogger("daily_refresh")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ----------------- SQL -----------------
UPSERT_VEHICLE = """
INSERT INTO vehicles (
  id, vin, model, year, partner_location, state, mileage,
  first_time_registration, retail_price, dealer_price,
  exterior, interior, wheels, motor, edition,
  performance, pilot, plus, available, stock_images,
  first_seen_at, last_seen_at
) VALUES (
  %(id)s, %(vin)s, %(model)s, %(year)s, %(partner_location)s, %(state)s, %(mileage)s,
  %(first_time_registration)s, %(retail_price)s, %(dealer_price)s,
  %(exterior)s, %(interior)s, %(wheels)s, %(motor)s, %(edition)s,
  %(performance)s, %(pilot)s, %(plus)s, TRUE, %(stock_images)s,
  now(), now()
)
ON CONFLICT (id) DO UPDATE SET
  vin = EXCLUDED.vin,
  model = EXCLUDED.model,
  year = EXCLUDED.year,
  partner_location = EXCLUDED.partner_location,
  state = EXCLUDED.state,
  mileage = EXCLUDED.mileage,
  first_time_registration = EXCLUDED.first_time_registration,
  retail_price = EXCLUDED.retail_price,
  dealer_price = EXCLUDED.dealer_price,
  exterior = EXCLUDED.exterior,
  interior = EXCLUDED.interior,
    wheels = COALESCE(EXCLUDED.wheels, vehicles.wheels),
    motor = COALESCE(EXCLUDED.motor, vehicles.motor),
  edition = EXCLUDED.edition,
    performance = CASE WHEN EXCLUDED.performance THEN TRUE ELSE vehicles.performance END,
    pilot = CASE WHEN EXCLUDED.pilot THEN TRUE ELSE vehicles.pilot END,
    plus = CASE WHEN EXCLUDED.plus THEN TRUE ELSE vehicles.plus END,
  stock_images = EXCLUDED.stock_images,
  available = TRUE,

  last_seen_at = now(),
  -- PRESERVE the original first_seen_at from the existing row
  first_seen_at = vehicles.first_seen_at;
"""

GET_OLD_PRICE = "SELECT retail_price FROM vehicles WHERE id=%(id)s;"

INSERT_HISTORY = """
INSERT INTO price_history (id, vehicle_id, price, observed_at)
VALUES (%(id)s, %(vehicle_id)s, %(price)s, %(observed_at)s);
"""

MARK_UNAVAILABLE = """
UPDATE vehicles
SET available = FALSE
WHERE last_seen_at < now() - interval '12 hours'
  AND available = TRUE;

"""

SELECT_EXPORT = """
SELECT v.id,
             v.model,
             v.year,
             v.partner_location,
             v.retail_price,
             v.dealer_price,
             v.mileage,
             v.first_time_registration,
             v.vin,
             v.stock_images,
             v.exterior,
             v.interior,
             v.wheels,
             v.motor,
             v.edition,
             v.performance,
             v.pilot,
             v.plus,
             v.state,
             v.available,
             v.first_seen_at,
             v.last_seen_at,
             prev.previous_price,
             CASE
                 WHEN prev.previous_price IS NOT NULL THEN v.retail_price - prev.previous_price
                 ELSE NULL
             END AS price_delta
FROM vehicles v
LEFT JOIN LATERAL (
        SELECT ph.price AS previous_price
        FROM price_history ph
        WHERE ph.vehicle_id = v.id
        ORDER BY ph.observed_at DESC
        OFFSET 1 LIMIT 1
) prev ON TRUE
WHERE v.available = TRUE
ORDER BY v.year DESC, v.retail_price NULLS LAST;
"""


# ----------------- Helpers -----------------
def _load_raw_from_s3():
    if not RAW_BUCKET or not RAW_KEY:
        return None
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=RAW_KEY)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    vehicles = data["vehicles"] if isinstance(data, dict) and "vehicles" in data else data
    return vehicles


def _normalize_for_db(v: dict) -> dict:
    """
    Ensure the dict from scraper has all DB keys (even if None).
    Your scraper already normalizes most fields; fill the remainder.
    """
    # Fill optional text/boolean fields that may not come from the scraper yet
    base = {
        "exterior": v.get("exterior"),
        "interior": v.get("interior"),
        "wheels": v.get("wheels"),
        "motor": v.get("motor"),
        "edition": v.get("edition"),
        "performance": bool(v.get("performance", False)),
        "pilot": bool(v.get("pilot", False)),
        "plus": bool(v.get("plus", False)),
    }
    merged = {**base, **v}
    # Ensure stock_images is a list/JSONB-compatible
    imgs = merged.get("stock_images") or []
    if isinstance(imgs, str):
        imgs = [p.strip() for p in imgs.split(",") if p.strip()]
    merged["stock_images"] = Json(imgs)  # <-- wrap with Json
    return merged


def _export_json(rows: list[dict]) -> None:
    body = json.dumps({"vehicles": rows}, indent=2, default=str)
    if BUCKET == "local":
        # Write to local file so your SPA can read it during dev
        out_path = os.path.join("public", "data", "vehicles.json")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(body)
        log.info("Exported %d vehicles to %s", len(rows), out_path)
    else:
        s3.put_object(
            Bucket=BUCKET,
            Key=EXPORT_KEY,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        log.info("Exported %d vehicles to s3://%s/%s", len(rows), BUCKET, EXPORT_KEY)


# ----------------- Entry point -----------------
def handler(event=None, context=None):
    # 1) Extract
    print("LOADER: handler entered")  # shows even without logging config
    import logging
    import os

    logging.getLogger().setLevel(logging.INFO)
    print(f"LOADER: env OK, region={os.getenv('AWS_REGION')}, bucket={os.getenv('PUBLIC_BUCKET')}")

    log.info("start: daily_refresh (loader)")
    log.info("loader: get raw from s3 ...")
    raw = _load_raw_from_s3()
    log.info("loader: got raw, count=%s", None if raw is None else len(raw))
    s3_loaded = raw is not None
    if raw is None:
        log.info("RAW_* not set, scraping directly (local/dev mode)")
        raw = scraper.fetch_raw()
    log.info("fetched=%d", len(raw))

    # 2) Transform + Load (batched upsert + history)
    log.info("loader: start db upsert (batched) ...")
    now_utc = datetime.now(timezone.utc)
    # Normalize all vehicles first
    normalized: list[dict] = [_normalize_for_db(item) for item in raw]
    all_ids = [v["id"] for v in normalized]

    # Load existing prices for all ids to classify new vs existing and detect changes
    existing_price_map: dict[str, float | None] = {}
    if all_ids:
        try:
            rows = fetch_all(
                "SELECT id, retail_price FROM vehicles WHERE id IN %(ids)s", {"ids": tuple(all_ids)}
            )
            existing_price_map = {r["id"]: r["retail_price"] for r in rows}
        except Exception as e:  # pragma: no cover
            log.warning("preload existing prices failed: %s", e)

    inserted_ids: list[str] = [vid for vid in all_ids if vid not in existing_price_map]
    updated_count = len(all_ids) - len(inserted_ids)

    # Batch UPSERT using execute_values
    UPSERT_VEHICLE_BULK = (
        "INSERT INTO vehicles (\n"
        "  id, vin, model, year, partner_location, state, mileage,\n"
        "  first_time_registration, retail_price, dealer_price,\n"
        "  exterior, interior, wheels, motor, edition,\n"
        "  performance, pilot, plus, available, stock_images,\n"
        "  first_seen_at, last_seen_at\n"
        ") VALUES %s\n"
        "ON CONFLICT (id) DO UPDATE SET\n"
        "  vin = EXCLUDED.vin,\n"
        "  model = EXCLUDED.model,\n"
        "  year = EXCLUDED.year,\n"
        "  partner_location = EXCLUDED.partner_location,\n"
        "  state = EXCLUDED.state,\n"
        "  mileage = EXCLUDED.mileage,\n"
        "  first_time_registration = EXCLUDED.first_time_registration,\n"
        "  retail_price = EXCLUDED.retail_price,\n"
        "  dealer_price = EXCLUDED.dealer_price,\n"
        "  exterior = EXCLUDED.exterior,\n"
        "  interior = EXCLUDED.interior,\n"
        "  wheels = COALESCE(EXCLUDED.wheels, vehicles.wheels),\n"
        "  motor = COALESCE(EXCLUDED.motor, vehicles.motor),\n"
        "  edition = EXCLUDED.edition,\n"
        "  performance = CASE WHEN EXCLUDED.performance THEN TRUE ELSE vehicles.performance END,\n"
        "  pilot = CASE WHEN EXCLUDED.pilot THEN TRUE ELSE vehicles.pilot END,\n"
        "  plus = CASE WHEN EXCLUDED.plus THEN TRUE ELSE vehicles.plus END,\n"
        "  stock_images = EXCLUDED.stock_images,\n"
        "  available = TRUE,\n"
        "  last_seen_at = now(),\n"
        "  first_seen_at = vehicles.first_seen_at;\n"
    )
    VALUES_TEMPLATE = (
        "(%(id)s, %(vin)s, %(model)s, %(year)s, %(partner_location)s, %(state)s, %(mileage)s,\n"
        " %(first_time_registration)s, %(retail_price)s, %(dealer_price)s,\n"
        " %(exterior)s, %(interior)s, %(wheels)s, %(motor)s, %(edition)s,\n"
        " %(performance)s, %(pilot)s, %(plus)s, TRUE, %(stock_images)s, now(), now())"
    )

    # Chunk to keep statements reasonable in size
    def _chunks(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    for batch in _chunks(normalized, 500):
        execute_values(UPSERT_VEHICLE_BULK, batch, template=VALUES_TEMPLATE, page_size=500)
    log.info(
        "loader: db upsert done (rows=%d new=%d existing=%d)",
        len(all_ids),
        len(inserted_ids),
        updated_count,
    )

    # Build price history rows in bulk
    price_change_details: list[dict] = []
    history_rows: list[dict] = []
    for v in normalized:
        vid = v["id"]
        new_price = v.get("retail_price")
        old_price = existing_price_map.get(vid)
        if vid in inserted_ids:
            if new_price is not None:
                history_rows.append(
                    {
                        "id": f"{vid}-{uuid.uuid4()}",
                        "vehicle_id": vid,
                        "price": new_price,
                        "observed_at": now_utc,
                    }
                )
        else:
            if new_price is not None and new_price != old_price:
                history_rows.append(
                    {
                        "id": f"{vid}-{uuid.uuid4()}",
                        "vehicle_id": vid,
                        "price": new_price,
                        "observed_at": now_utc,
                    }
                )
                try:
                    delta = new_price - old_price if old_price is not None else None
                except Exception:
                    delta = None
                price_change_details.append(
                    {
                        "id": vid,
                        "old_price": old_price,
                        "new_price": new_price,
                        "delta": delta,
                    }
                )

    if history_rows:
        execute_values(
            "INSERT INTO price_history (id, vehicle_id, price, observed_at) VALUES %s",
            history_rows,
            template="(%(id)s, %(vehicle_id)s, %(price)s, %(observed_at)s)",
            page_size=1000,
        )

    inserted = len(inserted_ids)
    updated = updated_count
    price_changes = len(price_change_details)
    price_change_ids = [d["id"] for d in price_change_details]
    # 3) Secondary deep feature scans (wheels, packages, motors) unless skipped
    # Decide deep scan policy
    # Default: if data came from S3 (staging/prod pipeline), skip deep scans to avoid outbound calls from VPC.
    # Allow explicit override via event.skip_deep_scan or env SKIP_DEEP_SCAN.
    skip_scan = False
    reason = ""
    try:
        # explicit event override takes precedence
        if isinstance(event, dict) and ("skip_deep_scan" in event):
            skip_scan = bool(event.get("skip_deep_scan"))
            reason = "event override"
        else:
            env_flag = os.getenv("SKIP_DEEP_SCAN", "").lower() in {"1", "true", "yes"}
            if env_flag:
                skip_scan = True
                reason = "env SKIP_DEEP_SCAN"
            elif s3_loaded:
                skip_scan = True
                reason = "raw loaded from S3"
    except Exception:  # pragma: no cover
        pass

    deep_scan_performed = False
    if not skip_scan:

        log.info("loader: starting feature deep scans")

        # Build reverse maps: category -> list of (human_label, code)
        wheels = []
        packages = []
        motors = []
        for label, mapping in FILTERS.items():
            for category, code in mapping.items():
                c_low = category.lower()
                if c_low == "wheels":
                    wheels.append((label, code))
                elif c_low == "package":
                    packages.append((label, code))
                elif c_low == "motor":
                    motors.append((label, code))

        # Helper to batch update list of ids
        def _update_with_ids(sql_template: str, label: str, ids: list[str]):
            if not ids:
                return 0
            # Use ANY(array) pattern; simpler to build VALUES list then update
            # For portability, run one UPDATE per chunk
            chunk_size = 200
            total_updated = 0
            for i in range(0, len(ids), chunk_size):
                subset = ids[i : i + chunk_size]
                execute(
                    sql_template,
                    {"ids": tuple(subset), "label": label},
                )
                total_updated += len(subset)
            return total_updated

        # Map package label -> column
        package_column = {
            "Performance": "performance",
            "Pilot": "pilot",
            "Plus": "plus",
        }

        # Wheels: find vehicles for each wheel code
        wheel_updates = 0
        for human, code in wheels:
            ids = scraper.fetch_ids_for_filter(
                "Wheels", code, scraper.DEFAULT_MODELS[0], scraper.DEFAULT_MARKET
            )
            if ids:
                wheel_updates += _update_with_ids(
                    "UPDATE vehicles SET wheels=%(label)s WHERE id IN %(ids)s AND (wheels IS NULL OR wheels != %(label)s);",
                    human,
                    list(ids),
                )
        log.info("feature-scan: wheels updated approx rows=%d", wheel_updates)

        # Motors: similar pattern
        motor_updates = 0
        for human, code in motors:
            ids = scraper.fetch_ids_for_filter(
                "Motor", code, scraper.DEFAULT_MODELS[0], scraper.DEFAULT_MARKET
            )
            if ids:
                motor_updates += _update_with_ids(
                    "UPDATE vehicles SET motor=%(label)s WHERE id IN %(ids)s AND (motor IS NULL OR motor != %(label)s);",
                    human,
                    list(ids),
                )
        log.info("feature-scan: motors updated approx rows=%d", motor_updates)

        # Packages: set boolean flags
        pkg_updates = 0
        for human, code in packages:
            column = package_column.get(human)
            if not column:
                continue
            ids = scraper.fetch_ids_for_filter(
                "Package", code, scraper.DEFAULT_MODELS[0], scraper.DEFAULT_MARKET
            )
            if ids:
                pkg_updates += _update_with_ids(
                    f"UPDATE vehicles SET {column}=TRUE WHERE id IN %(ids)s AND {column}=FALSE;",
                    human,
                    list(ids),
                )
        log.info("feature-scan: packages updated approx rows=%d", pkg_updates)
        # Coverage snapshot (approximate): counts after updates
        try:
            coverage_rows = fetch_all(
                """
                SELECT
                  count(*) FILTER (WHERE wheels IS NOT NULL) AS wheels_set,
                  count(*) FILTER (WHERE motor IS NOT NULL) AS motor_set,
                  count(*) FILTER (WHERE performance) AS performance_set,
                  count(*) FILTER (WHERE pilot) AS pilot_set,
                  count(*) FILTER (WHERE plus) AS plus_set,
                  count(*) AS total
                FROM vehicles
                """
            )
            if coverage_rows:
                cv = coverage_rows[0]
                log.info(
                    "feature-scan: coverage wheels=%s/%s motor=%s/%s performance=%s pilot=%s plus=%s",
                    cv["wheels_set"],
                    cv["total"],
                    cv["motor_set"],
                    cv["total"],
                    cv["performance_set"],
                    cv["pilot_set"],
                    cv["plus_set"],
                )
        except Exception as ce:  # pragma: no cover
            log.warning("feature-scan: coverage query failed: %s", ce)
        except Exception as e:
            log.warning("feature deep scan failed: %s", e)
        else:
            deep_scan_performed = True
    else:
        log.info("loader: deep feature scan skipped (%s)", reason or "policy")

    # 4) Mark vehicles not seen today as unavailable
    execute(MARK_UNAVAILABLE)

    # 5) Export snapshot for the SPA
    log.info("loader: export json ...")
    rows = fetch_all(SELECT_EXPORT)
    _export_json(rows)
    log.info("loader: export json done")

    summary = {
        "fetched": len(raw),
        "inserted": inserted,
        "updated": updated,
        "price_changes": price_changes,
        "exported": len(rows),
        "inserted_ids": inserted_ids,
        "price_change_ids": price_change_ids,
        "price_change_details": price_change_details,
        "deep_scan_skipped": not deep_scan_performed,
    }
    if inserted_ids:
        log.info(
            "Inserted IDs (%d):\n%s", len(inserted_ids), "\n".join(str(i) for i in inserted_ids)
        )
    if price_change_details:
        # Log a concise table-like output
        lines = [
            f"{d['id']}: {d['old_price']} -> {d['new_price']} (delta={d['delta']})"
            for d in price_change_details
        ]
        log.info(
            "Price Changes (%d):\n%s",
            len(price_change_details),
            "\n".join(lines),
        )
    log.info("Summary: %s", summary)
    return summary


if __name__ == "__main__":
    print(handler())
