from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
import os
from app.db import db_client

router = APIRouter()

TABLE_NAME = os.getenv("MYSQL_FIRMS_TABLE", "firms")

@router.get("/ping")
def ping():
    return {"status": "ok", "message": "FastAPI server is running"}


@router.get("/uru_training_data")
def get_uru_training_data():
    results = db_client.fetch_table_to_geojson("training_uruguay_fire")
    return {
        "count": len(results),
        "data": results,
    }


@router.get("/firms")
def get_firms(
    start_date: datetime = Query(..., description="Fecha inicial ISO"),
    end_date: datetime = Query(..., description="Fecha final ISO"),
    fire: bool = Query(True, description="Filtrar solo 'Fire'"),
):
    try:
        results = db_client.fetch_between_dates(
            table=TABLE_NAME,
            start_date=start_date,
            end_date=end_date,
            fire=fire,
        )
        return {
            "count": len(results),
            "data": results,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
