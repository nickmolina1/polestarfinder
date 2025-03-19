import json
import datetime
import requests
import brotli
import gzip
import time
from copy import deepcopy

from db import Vehicle, SessionLocal


# Define the API endpoint
url = "https://pc-api.polestar.com/eu-north-1/partner-rm-tool/public/"

# Headers for request
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

filters = {
    # "Snow": {"Exterior": "70700"},
    # "Space": {"Exterior": "71700"},
    # "Thunder": {"Exterior": "72800"},
    # "Void": {"Exterior": "01900"},
    # "Magnesium": {"Exterior": "72900"},
    # "Midnight": {"Exterior": "72300"},
    # "Moon": {"Exterior": "72700"},
    # "Jupiter": {"Exterior": "73600"},
    # "Performance": {"Package": "1010"},
    # "Pilot": {"Package": "1040"},
    # "Plus": {"Package": "1050"},
    # "Long range Dual motor - AWD": {"Motor": "ED"},
    # "Long range Dual motor - All Wheel Drive (AWD)": {"Motor": "FD"},
    # "Long range Dual motor with Performance pack - AWD": {"Motor": "ET"},
    # "Long range Single motor ": {"Motor": "EG"},
    # "Long range Single motor - Rear Wheel Drive (RWD) ": {"Motor": "FE"},
    # "Slate WeaveTech with Black Ash deco": {"Interior": "RFA000"},
    # "Charcoal WeaveTech with Black Ash deco": {"Interior": "RF8000"},
    # "Charcoal Embossed Textile with 3D Etched deco": {"Interior": "R60000"},
    # "Zinc Embossed Textile with 3D Etched deco": {"Interior": "R6B000"},
    # "Ventilated Nappa leather Barley with reconstructed wood deco": {
    #     "Interior": "RCC000"
    # },
    # "Ventilated Nappa leather Zinc with Light Ash deco": {"Interior": "RCZ300"},
    # "Charcoal MicroSuede textile with Black ash deco": {"Interior": "BST230"},
    # "BST Edition 230": {"Edition": "BST Edition 230"},
    # "BST Edition 270": {"Edition": "BST Edition 270"},
    # '19" 5-V Spoke Black Diamond Cut Alloy Wheels - Summer Tire': {"Wheels": "R14B"},
    # "19'' 5-Double Spoke Black Diamond Cut Alloy Wheel": {"Wheels": "R184"},
    # '20" 4-V Spoke Black Diamond Cut Alloy Wheels': {"Wheels": "001147"},
    # '20" 4-Y Spoke Black Polished Forged Alloy Wheels - Summer Tire': {
    #     "Wheels": "XPFWHE"
    # },
    # '20" Pro - All-Season Tires': {"Wheels": "001257"},
    # "21'' Gloss Black Diamond Cut Alloy Wheel BST edition": {"Wheels": "XPEWHE"},
    "Certified PreOwned": {"CycleState": "CertifiedPreOwned"},
    "PreOwned": {"CycleState": "PreOwned"},
    "New": {"CycleState": "New"},
}


# (Assuming your base payload and headers are defined above)
base_payload = {
    "operationName": "SearchVehicleAds",
    "variables": {
        "carModel": "PS2",
        "market": "us",
        "offset": 0,
        "limit": 200,
        "sortOrder": "Ascending",
        "sortProperty": "Price",
        "equalFilters": [],  # Will be updated per deep scan
        "excludeFilters": [{"filterType": "CycleState", "value": "New"}],
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
                }
            }
        }
    }
    """,
}


def fetch_deep_scan(filter_mapping):
    """
    Executes a deep scan for a given filter.

    :param filter_mapping: A dictionary representing the filter, e.g. {"Exterior": "70700"}
    :return: A list of dictionaries with vehicle details from this deep scan.
    """
    # Create a new payload based on the base payload, updating the equalFilters
    payload = deepcopy(base_payload)
    # The filter_mapping is expected to be something like {"Exterior": "70700"}
    key = list(filter_mapping.keys())[0]
    value = list(filter_mapping.values())[0]
    payload["variables"]["equalFilters"] = [{"filterType": key, "value": value}]

    # Make the request
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        print(
            f"Deep Scan for {key}={value} Response Status Code: {response.status_code}"
        )

        # Handle compression
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

        # Create a list of dictionaries similar to the first scan
        deep_vehicles_data = []
        for vehicle_ad in vehicle_ads:
            vehicle_id = vehicle_ad["id"]
            vehicle_details = vehicle_ad["vehicleDetails"]
            model = vehicle_details["modelDetails"]["displayName"]
            year = vehicle_details["modelDetails"]["modelYear"]
            partner_location = vehicle_ad["partnerLocation"]["name"]
            retail_price = vehicle_ad["price"]["retail"]
            dealer_price = vehicle_ad["price"]["dealer"]
            mileage = vehicle_ad["mileageInfo"]["distance"]
            scrape_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            first_time_registration = vehicle_ad["firstTimeRegistration"]
            vin = vehicle_details["vin"]
            stock_images = vehicle_details["stockImages"]

            deep_vehicles_data.append(
                {
                    "vehicle_id": vehicle_id,
                    "model": model,
                    "year": year,
                    "partner_location": partner_location,
                    "retail_price": retail_price,
                    "dealer_price": dealer_price,
                    "mileage": mileage,
                    "scrape_date": scrape_date,
                    "first_time_registration": first_time_registration,
                    "vin": vin,
                    "stock_images": stock_images,
                    # We won't update features like state here unless they come from a specific filter for state.
                }
            )
        return deep_vehicles_data

    except Exception as e:
        print(f"Error during deep scan for {key}={value}: {e}")
        return []


def update_feature_scan(feature_key, feature_name, deep_vehicles_data):
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
                # For text features, update the column with the feature name
                if feature_key.lower() in [
                    "exterior",
                    "interior",
                    "wheels",
                    "motor",
                    "edition",
                    "state",
                ]:
                    setattr(vehicle, feature_key.lower(), feature_name)
                # For boolean features (if implemented later), set them to True
                elif feature_key.lower() in ["performance", "pilot", "plus"]:
                    setattr(vehicle, feature_key.lower(), True)
                # Update mileage, price, etc. to ensure they reflect the latest scan
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


# Main deep scan loop: now generic for all features
if __name__ == "__main__":
    # Assume full inventory is already updated.
    # Now perform deep scans for each filter in the filters dictionary.
    for feature_name, filter_mapping in filters.items():
        # Wait 1 second between scans to avoid rate limits.
        time.sleep(1)

        # Fetch data for this filter.
        deep_data = fetch_deep_scan(filter_mapping)

        # Determine which feature column to update.
        # Extract the key from the filter mapping (e.g., "Exterior", "Package", "Motor", "Interior", "CycleState")
        raw_feature_key = list(filter_mapping.keys())[0]

        # Remap if necessary:
        # If the raw key is "CycleState", update the "state" column.
        if raw_feature_key.lower() == "cyclestate":
            feature_column = "state"
        # For performance, pilot, and plus filters, use the feature name itself (converted to lowercase)
        elif feature_name.lower() in ["performance", "pilot", "plus"]:
            feature_column = feature_name.lower()
        else:
            # Otherwise, assume the DB column name matches the raw key lowercased.
            feature_column = raw_feature_key.lower()

        # Call update_feature_scan with the computed feature column and feature name.
        update_feature_scan(feature_column, feature_name, deep_data)
