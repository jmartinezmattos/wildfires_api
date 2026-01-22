from fastapi import APIRouter, HTTPException
import os
from app.db import db_client
from app.utils import generate_signed_url, download_blob_as_text
from google.cloud import storage
import json
from cachetools import TTLCache
from datetime import date
from app.schemas import MetricName, MetricResponse

storage_client = storage.Client()

router = APIRouter()

MYSQL_FIRMS_TABLE = os.getenv("MYSQL_FIRMS_TABLE")
SIGNED_URL_CACHE_TTL_SECONDS = int(os.getenv("SIGNED_URL_CACHE_TTL_SECONDS", 300))

firefighters_cache = TTLCache(maxsize=1, ttl=3600) # Cache for 1 hour
signed_url_cache = TTLCache(maxsize=512, ttl=SIGNED_URL_CACHE_TTL_SECONDS)

@router.get("/ping")
def ping():
    return {"status": "ok", "message": "FastAPI server is running"}

#http://localhost:8000/fires?start_date=2025-12-20&end_date=2025-12-31

@router.get("/fires")
async def get_fires(start_date: date, end_date: date):
    db_results = await db_client.fetch_fires(start_date, end_date)
    for point in db_results:
        if point.get("gcs_image_path"):
            point["signed_url"] = get_cached_signed_url(point["gcs_image_path"])
        else:
            point["signed_url"] = None
    return db_results

# Cache de 1 elemento con expiración de 300 segundos (5 minutos)


@router.get("/metrics/{metric_name}/last", response_model=MetricResponse)
async def get_last_metric(metric_name: MetricName):
    
    metric_db_name = metric_name.value.upper()

    db_result = await db_client.fetch_last_metric(metric_db_name)
    
    if db_result:
        signed_url = get_cached_signed_url(db_result.get("gcs_path"))

        return {"url": signed_url, 
                "acq_datetime": db_result["acq_datetime"]}
    else:
        return {"message": "No metric data found"}
    
@router.get("/metrics/{metric_name}/{acq_date}", response_model=MetricResponse)
async def get_metric_by_date(metric_name: MetricName, acq_date: date):

    metric_db_name = metric_name.value.upper()

    db_result = await db_client.fetch_metric_by_date(
        metric_name=metric_db_name,
        acq_date=acq_date
    )

    if not db_result:
        raise HTTPException(
            status_code=404,
            detail=f"No {metric_name.value} data found for given date"
        )

    signed_url = get_cached_signed_url(db_result.get("gcs_path"))

    return {
        "url": signed_url,
        "acq_datetime": db_result["acq_datetime"]
    }

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


def get_cached_signed_url(gcs_path: str) -> str | None:
    if not gcs_path:
        return None

    if gcs_path in signed_url_cache:
        print("Cache hit for signed URL")
        return signed_url_cache[gcs_path]
    
    print("Cache miss for signed URL")

    signed_url = generate_signed_url(gcs_path)
    signed_url_cache[gcs_path] = signed_url

    return signed_url
