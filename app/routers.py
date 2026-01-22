from fastapi import APIRouter, HTTPException
import os
from app.db import db_client
from app.utils import get_cached_signed_url, get_cached_firefighters_geojson
from google.cloud import storage
from datetime import date
from app.schemas import MetricName, MetricResponse

storage_client = storage.Client()

router = APIRouter()

MYSQL_FIRMS_TABLE = os.getenv("MYSQL_FIRMS_TABLE")

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
    
    data = get_cached_firefighters_geojson()

    return data

