from fastapi import APIRouter
import os
from app.db import db_client
from app.utils import generate_signed_url, download_blob_as_text
from google.cloud import storage
import json
from cachetools import TTLCache

storage_client = storage.Client()

router = APIRouter()

MYSQL_FIRMS_TABLE = os.getenv("MYSQL_FIRMS_TABLE")

@router.get("/ping")
def ping():
    return {"status": "ok", "message": "FastAPI server is running"}

#http://localhost:8000/fires?start_date=2025-12-20&end_date=2025-12-31

@router.get("/fires")
async def get_fires(start_date, end_date):
    db_results = await db_client.fetch_fires(start_date, end_date)
    for point in db_results:
        if point.get("gcs_image_path"):
            point["signed_url"] = generate_signed_url(point["gcs_image_path"])
        else:
            point["signed_url"] = None
    return db_results

# Cache de 1 elemento con expiración de 300 segundos (5 minutos)
firefighters_cache = TTLCache(maxsize=1, ttl=300)

@router.get("/firefighters")
async def get_firefighters():
    if "data" in firefighters_cache:
        return firefighters_cache["data"]

    BUCKET_NAME = os.getenv("URUGUAY_DATA_BUCKET")
    OBJECT_NAME = os.getenv("FIREFIGHTERS_FILE")

    data = download_blob_as_text(BUCKET_NAME, OBJECT_NAME)

    geojson_data = json.loads(data)
    firefighters_cache["data"] = geojson_data  # Save to cache

    return geojson_data