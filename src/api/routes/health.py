"""Health check endpoints"""

from fastapi import APIRouter, Depends
from datetime import datetime
import structlog

from src.services.database import get_db_health
from src.services.kinesis_consumer import get_kinesis_health

router = APIRouter()
logger = structlog.get_logger()


@router.get("/health")
async def health_check():
    """
    Health check endpoint for load balancers and monitoring
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "healthcare-analytics-platform"
    }


@router.get("/health/detailed")
async def detailed_health_check():
    """
    Detailed health check with dependency status
    """
    db_status = await get_db_health()
    kinesis_status = await get_kinesis_health()

    overall_status = "healthy" if all([
        db_status["healthy"],
        kinesis_status["healthy"]
    ]) else "degraded"

    return {
        "status": overall_status,
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "database": db_status,
            "kinesis": kinesis_status
        }
    }
