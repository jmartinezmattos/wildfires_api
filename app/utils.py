import os
from datetime import date, datetime, timedelta
from google.cloud import storage

SA_PATH = os.getenv("SA_PATH")

if SA_PATH and SA_PATH.endswith(".json"):
    client = storage.Client.from_service_account_json(SA_PATH)
else:
    client = storage.Client()


def generate_signed_url(gcs_path: str, expiration_minutes: int = 60) -> str | None:
    if not gcs_path or not gcs_path.startswith("gs://"):
        return None

    bucket_name, blob_name = gcs_path[5:].split("/", 1)

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    return blob.generate_signed_url(
        expiration=timedelta(minutes=expiration_minutes),
        method="GET",
    )

def convert_to_geojson(
    rows,
    lat_field="latitude",
    lon_field="longitude",
    id_field="id",
):
    features = []

    for r in rows:
        try:
            lat = float(r[lat_field])
            lon = float(r[lon_field])
        except (KeyError, TypeError, ValueError):
            continue

        features.append(
            {
                "type": "Feature",
                "id": r.get(id_field),
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat],
                },
                "properties": {
                    k: v
                    for k, v in r.items()
                    if k not in [lat_field, lon_field, id_field]
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }
