# jobs/daily_refresh.py
from __future__ import annotations

import os
import json
import uuid
import logging
from datetime import datetime, timezone

import boto3
from psycopg2.extras import Json

from database.db import fetch_all, fetch_one, execute
import scraper.scraper as scraper  # your library-style scraper.py

# ----------------- Config -----------------
REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("PUBLIC_BUCKET", "local")        # "local" => write to disk
KEY_PREFIX = os.getenv("PUBLIC_KEY_PREFIX", "")     # e.g., "staging/" or ""
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
  wheels = EXCLUDED.wheels,
  motor = EXCLUDED.motor,
  edition = EXCLUDED.edition,
  performance = EXCLUDED.performance,
  pilot = EXCLUDED.pilot,
  plus = EXCLUDED.plus,
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
SELECT id, model, year, partner_location, retail_price, dealer_price, mileage,
       first_time_registration, vin, stock_images,
       exterior, interior, wheels, motor, edition,
       performance, pilot, plus, state, available,
       first_seen_at, last_seen_at
FROM vehicles
WHERE available = TRUE
ORDER BY year DESC, retail_price NULLS LAST;
"""

# ----------------- Helpers -----------------
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
    merged["stock_images"] = Json(imgs)   # <-- wrap with Json
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
    raw = scraper.fetch_raw()  # uses env: MODELS, MARKET, PAGE_LIMIT
    log.info("Fetched %d raw vehicles", len(raw))

    # 2) Transform + Load (upsert + history)
    inserted = updated = price_changes = 0
    for item in raw:
        v = _normalize_for_db(item)

        old = fetch_one(GET_OLD_PRICE, {"id": v["id"]})
        old_price = old["retail_price"] if old else None

        execute(UPSERT_VEHICLE, v)
        if old is None:
            inserted += 1
        else:
            updated += 1

        new_price = v.get("retail_price")
        if new_price is not None and new_price != old_price:
            execute(INSERT_HISTORY, {
                "id": f'{v["id"]}-{uuid.uuid4()}',
                "vehicle_id": v["id"],
                "price": new_price,
                "observed_at": datetime.now(timezone.utc),
            })
            price_changes += 1

    # 3) Mark vehicles not seen today as unavailable
    execute(MARK_UNAVAILABLE)

    # 4) Export snapshot for the SPA
    rows = fetch_all(SELECT_EXPORT)
    _export_json(rows)

    summary = {
        "fetched": len(raw),
        "inserted": inserted,
        "updated": updated,
        "price_changes": price_changes,
        "exported": len(rows),
    }
    log.info("Summary: %s", summary)
    return summary

if __name__ == "__main__":
    print(handler())
