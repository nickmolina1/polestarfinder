import json
import datetime
import requests
import brotli
import gzip
import time
from copy import deepcopy

from filters import filters  # Your filters dictionary for deep scan
from db import Vehicle, SessionLocal

# Define the API endpoint and headers (common to both models)
url = "https://pc-api.polestar.com/eu-north-1/partner-rm-tool/public/"
headers = {
    "authority": "pc-api.polestar.com",
    "method": "POST",
    "path": "/eu-north-1/partner-rm-tool/public/",
    "scheme": "https",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.polestar.com",
    "Referer": "https://www.polestar.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
}


def get_payload(
    model: str, limit: int = 200, equalFilters: list = None, excludeFilters: list = None
) -> dict:
    """
    Returns a payload for searching vehicles for a given model.

    :param model: The model string to use in the payload (e.g., "PS1" or "PS2").
    :param limit: The number of results to return.
    :param equalFilters: Optional list of filters to include.
    :return: A dictionary representing the payload.
    """
    if equalFilters is None:
        equalFilters = []  # default: no extra filters

    payload = {
        "operationName": "SearchVehicleAds",
        "variables": {
            "carModel": f"{model}",
            "market": "us",
            "offset": 0,
            "limit": limit,
            "sortOrder": "Ascending",
            "sortProperty": "Price",
            "equalFilters": equalFilters,
            "excludeFilters": excludeFilters,
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
                metadata {
                    limit
                    offset
                    resultCount
                    totalCount
                }
                vehicleAds {
                    id
                    firstTimeRegistration
                    price {
                        retail
                        dealer
                        currency
                    }
                    partnerLocation {
                        city
                        name
                    }
                    mileageInfo {
                        distance
                        metric
                    }
                    vehicleDetails {
                        vin
                        modelDetails {
                            displayName
                            modelYear
                        }
                        stockImages
                        cycleState
                    }
                }
            }
        }
        """,
    }
    return payload


def fetch_scan(payload: dict) -> list:
    """
    Executes a scan using the given payload and returns a list of vehicle data dictionaries.
    """
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        print(f"Scan Response Status Code: {response.status_code}")
        # Handle compression if necessary
        content_encoding = response.headers.get("Content-Encoding", "")
        if content_encoding == "br":
            try:
                decompressed_text = brotli.decompress(response.content).decode("utf-8")
            except brotli.error:
                decompressed_text = response.content.decode("utf-8", errors="ignore")
        elif content_encoding == "gzip":
            try:
                decompressed_text = gzip.decompress(response.content).decode("utf-8")
            except Exception:
                decompressed_text = response.content.decode("utf-8", errors="ignore")
        else:
            decompressed_text = response.text

        data = json.loads(decompressed_text)
        vehicle_ads = (
            data.get("data", {}).get("searchVehicleAds", {}).get("vehicleAds", [])
        )

        vehicles_data = []
        for vehicle_ad in vehicle_ads:
            vehicle_details = vehicle_ad.get("vehicleDetails", {})
            model_details = vehicle_details.get("modelDetails", {})
            partner_location = vehicle_ad.get("partnerLocation", {})

            vehicles_data.append(
                {
                    "vehicle_id": vehicle_ad.get("id"),
                    "model": model_details.get("displayName"),
                    "year": model_details.get("modelYear"),
                    "partner_location": partner_location.get("name"),
                    "retail_price": vehicle_ad.get("price", {}).get("retail"),
                    "mileage": vehicle_ad.get("mileageInfo", {}).get("distance"),
                    "vin": vehicle_details.get("vin"),
                    "state": vehicle_details.get("cycleState"),
                    "scrape_date": datetime.datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
            )
        return vehicles_data
    except Exception as e:
        print(f"Error during scan: {e}")
        return []


def update_inventory(vehicles_data: list) -> int:
    """
    Inserts or updates vehicles in the database.
    Returns the number of new vehicles inserted.
    This function is used for both PS1 and PS2 vehicles.
    """
    new_count = 0
    session = SessionLocal()
    try:
        for car in vehicles_data:
            existing_vehicle = (
                session.query(Vehicle).filter_by(id=car["vehicle_id"]).first()
            )
            if existing_vehicle:
                # Update existing vehicle details
                existing_vehicle.model = car["model"]
                existing_vehicle.year = car["year"]
                existing_vehicle.partner_location = car.get("partner_location")
                existing_vehicle.retail_price = car.get("retail_price")
                existing_vehicle.mileage = car.get("mileage")
                existing_vehicle.vin = car.get("vin")
                existing_vehicle.state = car.get("state")
                existing_vehicle.last_scan = datetime.datetime.now()
                print(f"Updated vehicle {existing_vehicle.id}")
            else:
                # Insert new vehicle
                new_vehicle = Vehicle(
                    id=car["vehicle_id"],
                    model=car["model"],
                    year=car["year"],
                    partner_location=car.get("partner_location"),
                    retail_price=car.get("retail_price"),
                    mileage=car.get("mileage"),
                    vin=car.get("vin"),
                    state=car.get("state"),
                    date_added=datetime.datetime.now(),
                    last_scan=datetime.datetime.now(),
                )
                session.add(new_vehicle)
                new_count += 1
                print(f"Added new vehicle {new_vehicle.id}")
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error updating inventory: {e}")
    finally:
        session.close()
    return new_count


def deep_scan_loop(carModel: str):
    """
    Executes deep scans for each filter defined in the filters dictionary.
    Uses the global 'filters' imported from filters.py.
    """
    for feature_name, filter_mapping in filters.items():
        time.sleep(1)  # Delay to avoid rate limits
        deep_data = fetch_scan(
            deepcopy(get_payload(carModel, equalFilters=[filter_mapping]))
        )

        # Determine which feature column to update
        raw_feature_key = list(filter_mapping.keys())[0]
        if raw_feature_key.lower() == "cyclestate":
            feature_column = "state"
        elif feature_name.lower() in ["performance", "pilot", "plus"]:
            feature_column = feature_name.lower()
        else:
            feature_column = raw_feature_key.lower()

        update_feature_scan(feature_column, feature_name, deep_data)


def update_feature_scan(feature_key: str, feature_name: str, deep_vehicles_data: list):
    """
    Updates vehicles in the database for a specific feature.

    :param feature_key: The feature column to update (e.g., "exterior", "interior").
    :param feature_name: The value to set (e.g., "Snow", "Space", etc.).
    :param deep_vehicles_data: List of vehicles (dicts) returned from the deep scan.
    """
    session = SessionLocal()
    try:
        for car in deep_vehicles_data:
            vehicle = session.query(Vehicle).filter_by(id=car["vehicle_id"]).first()
            if vehicle:
                if feature_key.lower() in [
                    "exterior",
                    "interior",
                    "wheels",
                    "motor",
                    "edition",
                    "state",
                ]:
                    setattr(vehicle, feature_key.lower(), feature_name)
                elif feature_key.lower() in ["performance", "pilot", "plus"]:
                    setattr(vehicle, feature_key.lower(), True)
                # Update price, mileage, etc.
                vehicle.retail_price = car.get("retail_price")
                vehicle.dealer_price = car.get("dealer_price")
                vehicle.mileage = car.get("mileage")
                vehicle.last_scan = datetime.datetime.now()
                print(
                    f"Updated {feature_key} for vehicle {vehicle.id} -> {feature_name}"
                )
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error updating {feature_key}: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    # --- Full Scan for PS2 ---
    ps2_payload = get_payload("PS2")
    ps2_data = fetch_scan(deepcopy(ps2_payload))
    print(f"Fetched {len(ps2_data)} PS2 vehicles.")
    new_count_ps2 = update_inventory(ps2_data)
    print(f"New PS2 vehicles inserted: {new_count_ps2}")

    # --- Deep Scan for PS2 Only If New Vehicles Were Added ---
    if new_count_ps2 > 0:
        print("Performing deep scans for PS2...")
        deep_scan_loop("PS2")
    else:
        print("No new PS2 vehicles; skipping deep scans.")

    # --- Full Scan for PS1 ---
    ps1_payload = get_payload("PS1")
    ps1_data = fetch_scan(deepcopy(ps1_payload))
    print(f"Fetched {len(ps1_data)} PS1 vehicles.")
    new_count_ps1 = update_inventory(ps1_data)
    print(f"New PS1 vehicles inserted: {new_count_ps1}")

    # # --- Deep Scan for PS1 Only If New Vehicles Were Added ---
    # if new_count_ps1 > 0:
    #     # Optionally, if you have a separate set of filters for PS1, use them.
    #     # For now, we re-use the same deep scan loop.
    #     print("Performing deep scans for PS1...")
    #     # You can define a separate deep_scan_loop_ps1 if necessary.
    #     deep_scan_loop()
    # else:
    #     print("No new PS1 vehicles; skipping deep scans.")
