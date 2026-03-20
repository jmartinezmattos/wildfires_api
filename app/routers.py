from fastapi import APIRouter, HTTPException
import os
import json
from app.db import db_client
from app.utils import get_cached_signed_url, get_cached_firefighters_geojson, fires_to_geojson, process_unchecked_fires, alerts_to_geojson
from google.cloud import storage
from datetime import date
from app.schemas import MetricName, MetricResponse, FireRevision, FireRevisionList
from typing import Literal
from fastapi import Request
storage_client = storage.Client()

router = APIRouter()

MYSQL_FIRMS_TABLE = os.getenv("MYSQL_FIRMS_TABLE")

@router.get("/ping")
def ping():
    return {"status": "ok", "message": "FastAPI server is running"}

#http://localhost:8000/fires?start_date=2025-12-20&end_date=2025-12-31

@router.get("/fires")
async def get_fires(start_date: date, end_date: date, source: Literal["FIRMS", "BATCH", "ALL"] | None = None, confirmed_only: bool = False):
    db_results = await db_client.fetch_fires(start_date, end_date, source, confirmed_only)
    return fires_to_geojson(db_results)

@router.get("/firms_alerts")
async def get_fires(start_date: date, end_date: date):
    db_results = await db_client.fetch_firms_alerts(start_date, end_date)
    return alerts_to_geojson(db_results)

@router.get("/fires_unchecked")
async def get_fires_unchecked(
    limit: int = 100,
    start_date: date | None = None,
    end_date: date | None = None,
    source: Literal["FIRMS", "BATCH"] | None = None,
):
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=422,
            detail="start_date must be less than or equal to end_date",
        )

    fires = await db_client.fetch_unchecked_fires(
        limit=limit,
        start_date=start_date,
        end_date=end_date,
        source=source,
    )
    return process_unchecked_fires(fires)

@router.post("/fires_unchecked")
async def save_fire_revision(payload: FireRevisionList):
    await db_client.process_revision(payload.revisions)
    return {"message": "Fire revision saved successfully"}



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

@router.get("/wms_datetimes")
async def get_wms_datetimes():
    res = await db_client.fetch_wms_datetimes()
    print(f"Datetimes for LST: {len(res['lst'])}")
    print(f"Datetimes for NDVI: {len(res['ndvi'])}")
    print(f"Datetimes for TRUE_COLOR: {len(res['true_color'])}")
    return res