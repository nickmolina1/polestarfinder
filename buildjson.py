import json


def extract_feature_images(json_data):
    organized = {}
    # Navigate into the JSON structure to get the list of features.
    features = json_data.get("data", {}).get("datoContent", {}).get("carFeatures", [])

    for feature in features:
        feature_title = feature.get("featureTitle")
        if not feature_title:
            continue

        # Initialize a dictionary for this feature
        feature_dict = {
            "featureCode": feature.get("featureCode"),
            "featureType": feature.get("featureType"),
            "thumbnail": None,
            "exteriorImages": [],
            "oldExteriorImages": [],
            "exteriorVideos": [],
            "galleryImages": [],
        }

        # Thumbnail image (if available)
        thumbnail = feature.get("thumbnail")
        if thumbnail and thumbnail.get("url"):
            feature_dict["thumbnail"] = thumbnail["url"]

        # modelSpecificMediaContent may contain exterior images, old exterior images, and videos.
        media_contents = feature.get("modelSpecificMediaContent", [])
        for media in media_contents:
            exterior = media.get("exteriorImage")
            if exterior and exterior.get("url"):
                feature_dict["exteriorImages"].append(exterior["url"])
            old_exterior = media.get("oldExteriorImage")
            if old_exterior and old_exterior.get("url"):
                feature_dict["oldExteriorImages"].append(old_exterior["url"])
            video = media.get("exteriorVideo")
            if video and video.get("url"):
                feature_dict["exteriorVideos"].append(video["url"])

        # galleryImage array
        gallery = feature.get("galleryImage", [])
        for img in gallery:
            if img.get("url"):
                feature_dict["galleryImages"].append(img["url"])

        organized[feature_title] = feature_dict

    return organized


# Specifying UTF-8 encoding
with open("stuff.json.txt", encoding="utf-8") as f:
    data = json.load(f)

feature_images = extract_feature_images(data)

# Print the result as a pretty-formatted JSON string
print(json.dumps(feature_images, indent=2))
