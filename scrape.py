import json
import datetime
import requests
import brotli
import gzip
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
    "Snow": {"Exterior": "70700"},
    "Space": {"Exterior": "71700"},
    "Thunder": {"Exterior": "72800"},
    "Void": {"Exterior": "01900"},
    "Magnesium": {"Exterior": "72900"},
    "Midnight": {"Exterior": "72300"},
    "Moon": {"Exterior": "72700"},
    "Jupiter": {"Exterior": "73600"},
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
    # "Certified PreOwned": {"CycleState": "CertifiedPreOwned"},
    # "Used": {"CycleState": "PreOwned"},
    # "New": {"CycleState": "New"},
}


payload = {
    "operationName": "SearchVehicleAds",
    "variables": {
        "carModel": "PS2",
        "market": "us",
        "offset": 0,
        "limit": 200,
        "sortOrder": "Ascending",
        "sortProperty": "Price",
        "equalFilters": [],
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

try:
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    print(f"Response Status Code: {response.status_code}")

    content_encoding = response.headers.get("Content-Encoding", "")
    if content_encoding == "br":
        try:
            decompressed_text = brotli.decompress(response.content).decode("utf-8")
        except brotli.error as e:
            decompressed_text = response.content.decode("utf-8", errors="ignore")
    elif content_encoding == "gzip":
        try:
            decompressed_text = gzip.decompress(response.content).decode("utf-8")
        except Exception as e:
            decompressed_text = response.content.decode("utf-8", errors="ignore")
    else:
        decompressed_text = response.text

    data = json.loads(decompressed_text)

    vehicle_ads = data.get("data", {}).get("searchVehicleAds", {}).get("vehicleAds", [])

    # create a list of dictionaries with vehicle details
    vehicles_data = []

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

        # ✅ Set all unknown features to None (NULL in SQL)
        vehicles_data.append(
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
                "state": None,  # Initially NULL until deep scan updates it
                "exterior": None,
                "interior": None,
                "wheels": None,
                "motor": None,
                "edition": None,
                "performance": False,  # Booleans default to False
                "pilot": False,
                "plus": False,
            }
        )

except Exception as e:
    print(f"Error fetching data: {e}")


def update_full_inventory(vehicles_data):
    """
    Inserts or updates vehicles in the database based on full inventory scan.
    """
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
                existing_vehicle.dealer_price = car.get("dealer_price")
                existing_vehicle.mileage = car.get("mileage")
                existing_vehicle.first_time_registration = car.get(
                    "first_time_registration"
                )
                existing_vehicle.vin = car.get("vin")
                existing_vehicle.stock_images = (
                    ",".join(car.get("stock_images", []))
                    if car.get("stock_images")
                    else None
                )
                existing_vehicle.last_scan = (
                    datetime.datetime.now()
                )  # Update last scan timestamp

                print(
                    f"Updated vehicle {existing_vehicle.id} (last scanned: {existing_vehicle.last_scan})"
                )
            else:
                # Insert new vehicle
                new_vehicle = Vehicle(
                    id=car["vehicle_id"],
                    model=car["model"],
                    year=car["year"],
                    partner_location=car.get("partner_location"),
                    retail_price=car.get("retail_price"),
                    dealer_price=car.get("dealer_price"),
                    mileage=car.get("mileage"),
                    first_time_registration=car.get("first_time_registration"),
                    vin=car.get("vin"),
                    stock_images=(
                        ",".join(car.get("stock_images", []))
                        if car.get("stock_images")
                        else None
                    ),
                    date_added=datetime.datetime.now(),  # First time added
                    last_scan=datetime.datetime.now(),  # First scan timestamp
                )
                session.add(new_vehicle)
                print(
                    f"Added new vehicle {new_vehicle.id} (added: {new_vehicle.date_added})"
                )

        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error updating inventory: {e}")
    finally:
        session.close()


# def update_feature_scan(feature_type, feature_value, vehicles_data):
#     """
#     Updates vehicles with specific features identified in a second scan.

#     :param feature_type: The attribute to update (e.g., 'exterior', 'interior', 'wheels', etc.).
#     :param feature_value: The value to set (e.g., 'Space' for exterior).
#     :param vehicles_data: The list of vehicles returned from the feature scan.
#     """
#     session = SessionLocal()
#     try:
#         for car in vehicles_data:
#             vehicle = session.query(Vehicle).filter_by(id=car["vehicle_id"]).first()

#             if vehicle:
#                 if feature_type in [
#                     "exterior",
#                     "interior",
#                     "wheels",
#                     "motor",
#                     "edition",
#                 ]:
#                     setattr(vehicle, feature_type, feature_value)  # Set text field

#                 elif feature_type in ["performance", "pilot", "plus"]:
#                     setattr(vehicle, feature_type, True)  # Set boolean flag

#                 print(
#                     f"Updated {feature_type} for vehicle {vehicle.id} -> {feature_value}"
#                 )

#         session.commit()
#     except Exception as e:
#         session.rollback()
#         print(f"Error updating {feature_type}: {e}")
#     finally:
#         session.close()


if __name__ == "__main__":

    # Update full inventory
    update_full_inventory(vehicles_data)

    # # Update feature scans
    # for feature, value in filters.items():
    #     update_feature_scan(feature, value, vehicles_data)
