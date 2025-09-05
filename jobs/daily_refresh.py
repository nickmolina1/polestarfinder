import os, json, uuid, boto3
from datetime import datetime, timezone
from db import fetch_all, fetch_one, execute

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
BUCKET = os.getenv("PUBLIC_BUCKET", "polestarfinder.com")

# SQL
UPSERT = """  -- same as in seed script (you can import from a shared module)
...
"""
GET_PRICE = "SELECT retail_price FROM vehicles WHERE id=%(id)s;"
INSERT_HISTORY = """ ... """
SELECT_EXPORT = """
SELECT id, model, year, partner_location, retail_price, dealer_price, mileage,
       first_time_registration, vin, stock_images, exterior, interior, wheels, motor, edition,
       performance, pilot, plus, state, available
FROM vehicles
WHERE available = TRUE
ORDER BY year DESC, retail_price NULLS LAST;
"""


def fetch_external():
    # Call the public API here and return list[dict] in your unified shape
    return []


def handler(event=None, context=None):
    vehicles = fetch_external()

    # upsert + history
    for v in vehicles:
        # normalize to your DB shape first
        execute(UPSERT, v)
        row = fetch_one(GET_PRICE, {"id": v["id"]})
        old = row["retail_price"] if row else None
        if v.get("retail_price") is not None and v.get("retail_price") != old:
            execute(
                INSERT_HISTORY,
                {
                    "id": f'{v["id"]}-{uuid.uuid4()}',
                    "vehicle_id": v["id"],
                    "price": v["retail_price"],
                    "observed_at": datetime.now(timezone.utc),
                },
            )

    # export snapshot for SPA
    rows = fetch_all(SELECT_EXPORT)
    body = json.dumps({"vehicles": rows}, indent=2, default=str)
    s3.put_object(
        Bucket=BUCKET,
        Key="data/vehicles.json",
        Body=body,
        ContentType="application/json",
        ACL="public-read",  # or leave off if served via CloudFront origin access control
    )
    return {"ok": True, "count": len(vehicles)}


if __name__ == "__main__":
    print(handler())
