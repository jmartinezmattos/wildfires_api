from enum import Enum
from datetime import datetime
from pydantic import BaseModel


# =========================
# Enums (API contract)
# =========================

class MetricName(str, Enum):
    """
    Metric names as exposed in the public API (URL-friendly).
    Internally they are mapped to uppercase values used in the DB.
    """
    ndvi = "ndvi"
    fwi = "fwi"
    lst = "lst"
    rgb = "rgb"


# =========================
# Response schemas
# =========================

class MetricResponse(BaseModel):
    url: str
    acq_datetime: datetime