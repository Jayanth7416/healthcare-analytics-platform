"""Analytics Data Models"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any
from datetime import datetime
from enum import Enum


class AggregationType(str, Enum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"


class RealtimeMetrics(BaseModel):
    """Real-time platform metrics"""
    events_per_minute: int
    events_per_hour: int
    active_providers: int
    active_patients: int
    current_throughput: float  # events/second
    error_rate: float  # percentage
    avg_latency_ms: float
    p99_latency_ms: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "events_per_minute": 16500,
                "events_per_hour": 1000000,
                "active_providers": 50,
                "active_patients": 8500,
                "current_throughput": 275.5,
                "error_rate": 0.01,
                "avg_latency_ms": 45.2,
                "p99_latency_ms": 98.5
            }
        }


class EventDistribution(BaseModel):
    """Distribution of events by type"""
    patient_visit: int = 0
    lab_result: int = 0
    prescription: int = 0
    vitals: int = 0
    diagnosis: int = 0
    procedure: int = 0


class PatientAnalytics(BaseModel):
    """Analytics for a specific patient (de-identified)"""
    patient_id_hash: str  # Hashed for privacy
    total_events: int
    event_distribution: EventDistribution
    first_event: datetime
    last_event: datetime
    providers_count: int
    facilities_count: int
    time_range_days: int


class ProviderAnalytics(BaseModel):
    """Analytics for a healthcare provider"""
    provider_id: str
    provider_name: Optional[str] = None
    total_events: int
    unique_patients: int
    event_distribution: EventDistribution
    avg_events_per_day: float
    peak_hour: int  # 0-23
    peak_day: str  # Monday-Sunday
    error_rate: float
    avg_processing_time_ms: float
    time_range: str


class TimeSeriesDataPoint(BaseModel):
    """Single data point in time series"""
    timestamp: datetime
    value: float


class TimeSeriesData(BaseModel):
    """Time series analytics data"""
    metric: str
    aggregation: AggregationType
    data_points: List[TimeSeriesDataPoint]
    total: float
    average: float
    min_value: float
    max_value: float


class AnalyticsSummary(BaseModel):
    """Platform analytics summary"""
    total_events: int
    total_providers: int
    total_patients: int
    events_by_type: Dict[str, int]
    events_by_source: Dict[str, int]
    top_providers: List[Dict[str, Any]]
    error_summary: Dict[str, int]
    performance_metrics: Dict[str, float]
