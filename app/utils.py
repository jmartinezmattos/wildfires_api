import os
from datetime import timedelta
from typing import List, Dict, Any

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

    for feature in geojson_data.get("features", []):
        props = feature.get("properties", {})

        feature["properties"] = {
            "DEPARTAMENTO": props.get("DEPARTAMEN"),
            "NOMBRE": props.get("NOMBRE"),
        }

    firefighters_cache["data"] = geojson_data  # Save to cache
    return geojson_data

def get_cached_signed_url(gcs_path: str) -> str | None:
    if not gcs_path:
        return None

    signed_url = signed_url_cache.get(gcs_path)
    if signed_url is not None:
        return signed_url

    signed_url = generate_signed_url(gcs_path)
    if signed_url is not None:
        signed_url_cache[gcs_path] = signed_url

    return signed_url

def generate_signed_url(gcs_path: str) -> str | None:
    if not gcs_path or not gcs_path.startswith("gs://"):
        return None

    bucket_name, blob_name = gcs_path[5:].split("/", 1)

    blob = client.bucket(bucket_name).blob(blob_name)

    return blob.generate_signed_url(
        expiration=timedelta(seconds=SIGNED_URL_CACHE_TTL_SECONDS),
        method="GET",
        version="v4",
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


def fires_to_geojson(fires: list[dict]) -> dict:
    local_signed_url_cache = {}
    features = []

    for fire in fires:
        fire = fire.copy()

        gcs_path = fire.get("gcs_image_path")
        if gcs_path:
            if gcs_path not in local_signed_url_cache:
                local_signed_url_cache[gcs_path] = get_cached_signed_url(gcs_path)
            fire["signed_url"] = local_signed_url_cache[gcs_path]
        else:
            fire["signed_url"] = None

        features.append(fire_to_feature(fire))

    return {
        "type": "FeatureCollection",
        "features": features,
    }

def add_signed_url_if_image(fire: dict) -> dict:
    fire = fire.copy()

    gcs_path = fire.get("gcs_image_path")
    fire["signed_url"] = (
        get_cached_signed_url(gcs_path) if gcs_path else None
    )

    return fire


def fire_to_feature(fire: dict) -> dict:
    return {
        "type": "Feature",
        "id": fire["id"],
        "geometry": {
            "type": "Point",
            "coordinates": [fire["longitude"], fire["latitude"]],
        },
        "properties": {
            k: v
            for k, v in fire.items()
            if k not in "gcs_image_path"
        },
    }