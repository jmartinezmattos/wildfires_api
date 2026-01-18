from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
import os
from app.db import db_client
from app.utils import generate_signed_url

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
