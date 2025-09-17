from __future__ import annotations

import datetime as dt
import gzip
import json
import os
from typing import Dict, List, Optional

import brotli
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config (env-tunable) ----------
API_URL = "https://pc-api.polestar.com/eu-north-1/partner-rm-tool/public/"

DEFAULT_MODELS = [m.strip() for m in os.getenv("MODELS", "PS2").split(",") if m.strip()]
DEFAULT_MARKET = os.getenv("MARKET", "us")
DEFAULT_LIMIT = int(os.getenv("PAGE_LIMIT", "200"))

HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Content-Type": "application/json",
    "Origin": "https://www.polestar.com",
    "Referer": "https://www.polestar.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
}


# ---------- HTTP session with retries ----------
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


# ---------- GraphQL payload ----------
def _payload(
    model: str,
    market: str,
    offset: int,
    limit: int,
    equal_filters: Optional[List[Dict]] = None,
    exclude_filters: Optional[List[Dict]] = None,
) -> dict:
    if equal_filters is None:
        equal_filters = []
    if exclude_filters is None:
        # Exclude "New" cycle state by default (used/CPO focus)
        exclude_filters = [{"filterType": "CycleState", "value": "New"}]

    return {
        "operationName": "SearchVehicleAds",
        "variables": {
            "carModel": model,
            "market": market,
            "region": None,
            "offset": offset,
            "limit": limit,
            "sortOrder": "Ascending",
            "sortProperty": "Price",
            "equalFilters": equal_filters,
            "excludeFilters": exclude_filters,
        },
        "query": """
        query SearchVehicleAds($carModel: CarModel!, $market: String!, $region: String, $offset: Int!,
                               $limit: Int!, $sortOrder: SortOrder2!, $sortProperty: SortProperty!,
                               $equalFilters: [EqualFilter!], $excludeFilters: [ExcludeFilter!]) {
          searchVehicleAds(
            carModel: $carModel
            market: $market
            region: $region
            offset: $offset
            limit: $limit
            sortOrder: $sortOrder
            sortProperty: $sortProperty
            equalFilters: $equalFilters
            excludeFilters: $excludeFilters
          ) {
            metadata { limit offset resultCount totalCount }
            vehicleAds {
              id
              firstTimeRegistration
              price { retail dealer currency }
              partnerLocation { city name }
              mileageInfo { distance metric }  # metric may be KM
              vehicleDetails {
                vin
                modelDetails { displayName modelYear }
                stockImages
                cycleState
              }
            }
          }
        }
        """,
    }


# ---------- Response decoding (brotli/gzip safe) ----------
def _decode_json(resp: requests.Response) -> dict:
    enc = resp.headers.get("Content-Encoding", "")
    if enc == "br":
        try:
            text = brotli.decompress(resp.content).decode("utf-8")
        except Exception:
            text = resp.text
    elif enc == "gzip":
        try:
            text = gzip.decompress(resp.content).decode("utf-8")
        except Exception:
            text = resp.text
    else:
        text = resp.text
    return json.loads(text)


# ---------- Helpers ----------
def _km_to_miles(value, metric) -> Optional[int]:
    if value is None:
        return None
    try:
        dist = float(value)
    except Exception:
        return None
    m = (metric or "").lower()
    if "km" in m:
        dist *= 0.621371
    return int(round(dist))


def _normalize_vehicle(ad: dict, model_family: str) -> dict:
    vd = ad.get("vehicleDetails") or {}
    md = vd.get("modelDetails") or {}
    pl = ad.get("partnerLocation") or {}
    mi = ad.get("mileageInfo") or {}
    price = ad.get("price") or {}

    return {
        "id": str(ad.get("id")),
        "vin": vd.get("vin"),
        "model": md.get("displayName") or model_family,
        "year": md.get("modelYear"),
        "partner_location": pl.get("name") or pl.get("city"),
        "state": vd.get("cycleState"),
        "mileage": _km_to_miles(mi.get("distance"), mi.get("metric")),
        "first_time_registration": ad.get("firstTimeRegistration"),
        "retail_price": price.get("retail"),
        "dealer_price": price.get("dealer"),
        "currency": price.get("currency"),
        "stock_images": vd.get("stockImages") or [],
        "model_family": model_family,
        "scrape_date": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _fetch_page(sess: requests.Session, model: str, market: str, offset: int, limit: int) -> dict:
    resp = sess.post(API_URL, json=_payload(model, market, offset, limit), timeout=20)
    resp.raise_for_status()
    return _decode_json(resp)


# ---------- (Optional) deep details hook ----------
def fetch_details(vehicle_id: str) -> dict:
    """
    Placeholder for per-vehicle deep scan if you decide to enrich each car.
    Return {} for now; keep this function to wire in later without changing callers.
    """
    return {}


# ---------- Public API ----------
def fetch_raw(
    models: Optional[List[str]] = None,
    market: Optional[str] = None,
    page_limit: Optional[int] = None,
    include_details: bool = False,
) -> List[Dict]:
    """
    Fetch ALL vehicles for the given model list with pagination.
    Returns a list of normalized dicts, one per vehicle.
    Set include_details=True later if you wire up fetch_details().
    """
    models = models or DEFAULT_MODELS
    market = market or DEFAULT_MARKET
    limit = page_limit or DEFAULT_LIMIT

    sess = _session()
    out: List[Dict] = []

    for model in models:
        offset = 0
        total = None
        while True:
            block = _fetch_page(sess, model, market, offset, limit)
            data = (block.get("data") or {}).get("searchVehicleAds") or {}
            meta = data.get("metadata") or {}
            ads = data.get("vehicleAds") or []

            for ad in ads:
                v = _normalize_vehicle(ad, model_family=model)
                if include_details:
                    try:
                        details = fetch_details(v["id"])
                        if details:
                            v.update(details)
                    except Exception as e:
                        # Non-fatal; continue with summary
                        print(f"[deep-scan] {v['id']} failed: {e}")
                out.append(v)

            result_count = int(meta.get("resultCount") or len(ads))
            total = int(
                meta.get("totalCount") or (offset + result_count) if total is None else total
            )

            if result_count <= 0 or offset + result_count >= total:
                break
            offset += result_count  # or += limit

    return out


# ---------- CLI test ----------
if __name__ == "__main__":
    cars = fetch_raw()  # uses env defaults
    print(f"Fetched {len(cars)} vehicles")
    if cars:
        from pprint import pprint

        pprint(cars[0])
