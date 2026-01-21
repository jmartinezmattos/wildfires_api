from fastapi import APIRouter, HTTPException
import os
from app.db import db_client
from app.utils import generate_signed_url, download_blob_as_text
from google.cloud import storage
import json
from cachetools import TTLCache
from datetime import date

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

@router.get("/ndvi")
async def get_ndvi_url():
    NDVI_GCS_PATH = "gs://wildfires_data_um/ndvi/NDVI_Uruguay_20251206.tif"

    signed_url = generate_signed_url(NDVI_GCS_PATH)

    return {"ndvi_signed_url": signed_url}

@router.get("/ndvi/last")
async def get_last_ndvi():
    metric_name = "NDVI"
    db_result = await db_client.fetch_last_metric(metric_name)
    print(db_result)
    if db_result:
        signed_url = generate_signed_url(db_result.get("gcs_path"))
        return {"ndvi_signed_url": signed_url, "acq_datetime": db_result["acq_datetime"]}
    else:
        return {"message": "No NDVI data found"}
    
@router.get("/ndvi/last")
async def get_last_ndvi():
    metric_name = "NDVI"
    db_result = await db_client.fetch_last_metric(metric_name)
    print(db_result)
    if db_result:
        signed_url = generate_signed_url(db_result.get("gcs_path"))
        return {"ndvi_signed_url": signed_url, "acq_datetime": db_result["acq_datetime"]}
    else:
        return {"message": "No NDVI data found"}


@router.get("/ndvi/{acq_date}")
async def get_ndvi_by_date(acq_date: date):
    metric_name = "NDVI"

    db_result = await db_client.fetch_metric_by_date(
        metric_name=metric_name,
        acq_date=acq_date
    )

    if not db_result:
        raise HTTPException(status_code=404, detail="No NDVI data found for given date")

    signed_url = generate_signed_url(db_result.get("gcs_path"))

    return {
        "ndvi_signed_url": signed_url,
        "acq_datetime": db_result["acq_datetime"]
    }


@router.get("/lst")
async def get_lst_url():
    LST_GCS_PATH = "gs://wildfires_data_um/lst/MODIS_LST_Uruguay_20251207.tif"

    signed_url = generate_signed_url(LST_GCS_PATH)

    return {"lst_signed_url": signed_url}

@router.get("/fwi")
async def get_fwi_url():
    FWI_GCS_PATH = "gs://wildfires_data_um/fwi/FWI_Uruguay_20251201.tif"

    signed_url = generate_signed_url(FWI_GCS_PATH)

    return {"fwi_signed_url": signed_url}

@router.get("/rgb")
async def get_rgb_url():
    RGB_GCS_PATH = "gs://wildfires_data_um/wildfire_rgb_aqua_-34.5338_-56.2831.tif"

    signed_url = generate_signed_url(RGB_GCS_PATH)

    return {"rgb_signed_url": signed_url}

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