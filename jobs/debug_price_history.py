"""Diagnostic queries for price history distribution.

Run with:
  python -m jobs.debug_price_history
Optionally pass VEHICLE_ID=<id> to dump that vehicle's full history.
"""

from __future__ import annotations
import os
from database.db import fetch_all

DIST_QUERY = """
SELECT cnt, COUNT(*) AS vehicle_count
FROM (
  SELECT vehicle_id, COUNT(*) AS cnt
  FROM price_history
  GROUP BY vehicle_id
) t
GROUP BY cnt
ORDER BY cnt;
"""

MULTI_QUERY = """
SELECT vehicle_id, COUNT(*) AS rows
FROM price_history
GROUP BY vehicle_id
HAVING COUNT(*) >= 2
ORDER BY rows DESC, vehicle_id
LIMIT 25;
"""

HISTORY_QUERY = """
SELECT vehicle_id, price, observed_at
FROM price_history
WHERE vehicle_id = %(vehicle_id)s
ORDER BY observed_at;
"""


def main():
    print("-- Distribution of price_history row counts per vehicle --")
    dist = fetch_all(DIST_QUERY)
    for d in dist:
        print(f"rows_per_vehicle={d['cnt']}: vehicles={d['vehicle_count']}")

    print("\n-- Vehicles with >=2 rows (sample up to 25) --")
    mult = fetch_all(MULTI_QUERY)
    if not mult:
        print("(none)")
    else:
        for m in mult:
            print(f"vehicle_id={m['vehicle_id']} rows={m['rows']}")

    vid = os.getenv("VEHICLE_ID")
    if vid:
        print(f"\n-- Full history for vehicle {vid} --")
        hist = fetch_all(HISTORY_QUERY, {"vehicle_id": vid})
        if not hist:
            print("(no rows)")
        else:
            for h in hist:
                print(f"{h['observed_at']}: price={h['price']}")


if __name__ == "__main__":  # pragma: no cover
    main()
