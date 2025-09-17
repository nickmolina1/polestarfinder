import json
import os
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

from database.db import execute, fetch_one

load_dotenv()

dsn = os.getenv("PG_DSN")
if not dsn:
    raise RuntimeError("PG_DSN not set! Did you create .env?")

VEH_JSON = "public/data/vehicles.json"

UPSERT_VEHICLE = """
INSERT INTO vehicles (id, vin, model, year, partner_location, state, mileage,
                      first_time_registration, retail_price, dealer_price,
                      exterior, interior, wheels, motor, edition,
                      performance, pilot, plus, available, stock_images, date_added, last_scan)
VALUES (%(id)s, %(vin)s, %(model)s, %(year)s, %(partner_location)s, %(state)s, %(mileage)s,
        %(first_time_registration)s, %(retail_price)s, %(dealer_price)s,
        %(exterior)s, %(interior)s, %(wheels)s, %(motor)s, %(edition)s,
        %(performance)s, %(pilot)s, %(plus)s, TRUE, %(stock_images)s, now(), now())
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
  available = TRUE,
  stock_images = EXCLUDED.stock_images,
  last_scan = now();
"""

GET_PRICE = "SELECT retail_price FROM vehicles WHERE id=%(id)s;"
INSERT_HISTORY = """
INSERT INTO price_history (id, vehicle_id, price, observed_at)
VALUES (%(id)s, %(vehicle_id)s, %(price)s, %(observed_at)s);
"""


def normalize_vehicle(v):
    # adapt keys from your JSON shape
    images = v.get("stock_images") or v.get("images") or []
    if isinstance(images, str):
        images = [img.strip() for img in images.split(",") if img.strip()]
    return {
        "id": str(v["id"]),
        "vin": v.get("vin"),
        "model": v.get("model") or "",
        "year": int(v.get("year") or 0),
        "partner_location": v.get("partner_location"),
        "state": v.get("state"),
        "mileage": int(v["mileage"]) if v.get("mileage") is not None else None,
        "first_time_registration": v.get("first_time_registration"),  # 'YYYY-MM-DD' or None
        "retail_price": v.get("retail_price"),
        "dealer_price": v.get("dealer_price"),
        "exterior": v.get("exterior"),
        "interior": v.get("interior"),
        "wheels": v.get("wheels"),
        "motor": v.get("motor"),
        "edition": v.get("edition"),
        "performance": bool(v.get("performance", False)),
        "pilot": bool(v.get("pilot", False)),
        "plus": bool(v.get("plus", False)),
        "stock_images": images,  # jsonb
    }


if __name__ == "__main__":
    with open(VEH_JSON, "r", encoding="utf-8") as f:
        payload = json.load(f)
    items = payload["vehicles"] if isinstance(payload, dict) and "vehicles" in payload else payload

    for raw in items:
        nv = normalize_vehicle(raw)
        # upsert vehicle
        execute(UPSERT_VEHICLE, nv)

        # history on price change
        row = fetch_one(GET_PRICE, {"id": nv["id"]})
        old_price = row["retail_price"] if row else None
        new_price = nv["retail_price"]
        if new_price is not None and new_price != old_price:
            execute(
                INSERT_HISTORY,
                {
                    "id": f'{nv["id"]}-{uuid.uuid4()}',
                    "vehicle_id": nv["id"],
                    "price": new_price,
                    "observed_at": datetime.now(timezone.utc),
                },
            )

    print("Seed complete.")
