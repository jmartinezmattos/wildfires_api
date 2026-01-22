import os
from datetime import timedelta
from google.cloud import storage
from fastapi import HTTPException
from cachetools import TTLCache
import json

SA_PATH = os.getenv("SA_PATH")

storage_client = storage.Client()

if SA_PATH:
    try:
        client = storage.Client.from_service_account_json(SA_PATH)
    except Exception as e:
        raise Exception(f"Failed to create storage client: {e}")
else:
    raise EnvironmentError("Service Account path (SA_PATH) is not set in environment variables.")

SIGNED_URL_CACHE_TTL_SECONDS = int(os.getenv("SIGNED_URL_CACHE_TTL_SECONDS", 300)) # default 5 minutes
FIREFIGHTERS_CACHE_TTL_SECONDS = int(os.getenv("FIREFIGHTERS_CACHE_TTL_SECONDS", 3600)) # default 1 hour

signed_url_cache = TTLCache(maxsize=512, ttl=SIGNED_URL_CACHE_TTL_SECONDS)
firefighters_cache = TTLCache(maxsize=1, ttl=FIREFIGHTERS_CACHE_TTL_SECONDS)

def get_cached_firefighters_geojson() -> dict:
    if "data" in firefighters_cache:
        return firefighters_cache["data"]

    BUCKET_NAME = os.getenv("URUGUAY_DATA_BUCKET")
    OBJECT_NAME = os.getenv("FIREFIGHTERS_FILE")

    data = download_blob_as_text(BUCKET_NAME, OBJECT_NAME)

    geojson_data = json.loads(data)
    firefighters_cache["data"] = geojson_data  # Save to cache

    return geojson_data

def get_cached_signed_url(gcs_path: str) -> str | None:
    if not gcs_path:
        return None

    if gcs_path in signed_url_cache:
        print("Cache hit for signed URL")
        return signed_url_cache[gcs_path]
    
    print("Cache miss for signed URL")

    signed_url = generate_signed_url(gcs_path, expiration_minutes=SIGNED_URL_CACHE_TTL_SECONDS // 60)
    signed_url_cache[gcs_path] = signed_url

    return signed_url

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

def download_blob_as_text(BUCKET_NAME: str, OBJECT_NAME: str):

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(OBJECT_NAME)

        if not blob.exists():
            raise HTTPException(status_code=404, detail="Archivo no encontrado en el bucket")
        
        return blob.download_as_text()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading file {OBJECT_NAME} from bucket {BUCKET_NAME}: {str(e)}")
