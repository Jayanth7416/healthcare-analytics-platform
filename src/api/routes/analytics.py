"""Analytics endpoints"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from datetime import datetime, timedelta
from enum import Enum
import structlog

from src.services.analytics_service import AnalyticsService
from src.models.analytics import (
    RealtimeMetrics,
    PatientAnalytics,
    ProviderAnalytics,
    TimeSeriesData,
    AggregationType
)
from src.utils.auth import verify_api_key

router = APIRouter()
logger = structlog.get_logger()

analytics_service = AnalyticsService()


class TimeRange(str, Enum):
    HOUR = "1h"
    DAY = "24h"
    WEEK = "7d"
    MONTH = "30d"


@router.get("/realtime", response_model=RealtimeMetrics)
async def get_realtime_metrics(
    api_key: str = Depends(verify_api_key)
):
    """
    Get real-time platform metrics

    Returns:
    - Events processed in last minute
    - Active providers
    - Current throughput
    - Error rate
    """
    metrics = await analytics_service.get_realtime_metrics()
    return metrics


@router.get("/patients/{patient_id}", response_model=PatientAnalytics)
async def get_patient_analytics(
    patient_id: str,
    time_range: TimeRange = Query(default=TimeRange.MONTH),
    api_key: str = Depends(verify_api_key)
):
    """
    Get analytics for a specific patient (de-identified)

    - Visit frequency
    - Event types distribution
    - Care timeline
    """
    analytics = await analytics_service.get_patient_analytics(
        patient_id=patient_id,
        time_range=time_range.value
    )

    if not analytics:
        raise HTTPException(status_code=404, detail="Patient data not found")

    return analytics


@router.get("/providers/{provider_id}", response_model=ProviderAnalytics)
async def get_provider_analytics(
    provider_id: str,
    time_range: TimeRange = Query(default=TimeRange.MONTH),
    api_key: str = Depends(verify_api_key)
):
    """
    Get analytics for a healthcare provider

    - Patient volume
    - Event distribution
    - Peak hours analysis
    - Comparative benchmarks
    """
    analytics = await analytics_service.get_provider_analytics(
        provider_id=provider_id,
        time_range=time_range.value
    )

    if not analytics:
        raise HTTPException(status_code=404, detail="Provider not found")

    return analytics


@router.get("/timeseries")
async def get_timeseries_data(
    metric: str = Query(..., description="Metric to retrieve"),
    time_range: TimeRange = Query(default=TimeRange.DAY),
    aggregation: AggregationType = Query(default=AggregationType.HOUR),
    provider_id: Optional[str] = None,
    api_key: str = Depends(verify_api_key)
):
    """
    Get time series data for dashboards

    Supported metrics:
    - event_count
    - unique_patients
    - error_rate
    - latency_p99
    """
    valid_metrics = ["event_count", "unique_patients", "error_rate", "latency_p99"]
    if metric not in valid_metrics:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric. Must be one of: {valid_metrics}"
        )

    data = await analytics_service.get_timeseries(
        metric=metric,
        time_range=time_range.value,
        aggregation=aggregation,
        provider_id=provider_id
    )

    return data


@router.get("/summary")
async def get_analytics_summary(
    time_range: TimeRange = Query(default=TimeRange.DAY),
    api_key: str = Depends(verify_api_key)
):
    """
    Get summary analytics for the platform
    """
    summary = await analytics_service.get_summary(time_range.value)
    return {
        "time_range": time_range.value,
        "generated_at": datetime.utcnow().isoformat(),
        **summary
    }
