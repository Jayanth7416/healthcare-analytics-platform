"""Healthcare Event Models"""

from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
import uuid


class EventType(str, Enum):
    PATIENT_VISIT = "patient_visit"
    LAB_RESULT = "lab_result"
    PRESCRIPTION = "prescription"
    VITALS = "vitals"
    DIAGNOSIS = "diagnosis"
    PROCEDURE = "procedure"
    DISCHARGE = "discharge"
    ADMISSION = "admission"


class EventSource(str, Enum):
    EHR_EPIC = "ehr_epic"
    EHR_CERNER = "ehr_cerner"
    LAB_SYSTEM = "lab_system"
    IOT_DEVICE = "iot_device"
    MANUAL_ENTRY = "manual_entry"


class EventMetadata(BaseModel):
    source: EventSource
    version: str = "1.0"
    correlation_id: Optional[str] = None
    retry_count: int = 0


class HealthcareEvent(BaseModel):
    """
    Healthcare event schema for ingestion

    All PHI (Protected Health Information) fields are encrypted
    before storage and transmission.
    """
    event_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    provider_id: str = Field(..., min_length=1, max_length=50)
    patient_id: str = Field(..., min_length=1, max_length=100)  # Encrypted
    facility_id: Optional[str] = None
    department: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: EventMetadata

    @validator('patient_id')
    def validate_patient_id(cls, v):
        """Ensure patient_id is not empty"""
        if not v or not v.strip():
            raise ValueError("patient_id cannot be empty")
        return v

    @validator('provider_id')
    def validate_provider_id(cls, v):
        """Ensure provider_id is not empty"""
        if not v or not v.strip():
            raise ValueError("provider_id cannot be empty")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "patient_visit",
                "provider_id": "PROV-001",
                "patient_id": "encrypted_patient_id",
                "facility_id": "FAC-NYC-001",
                "department": "cardiology",
                "payload": {
                    "visit_type": "routine_checkup",
                    "duration_minutes": 30
                },
                "metadata": {
                    "source": "ehr_epic",
                    "version": "1.0"
                }
            }
        }


class EventResponse(BaseModel):
    """Response model for event ingestion"""
    event_id: str
    status: str  # accepted, rejected, processing
    timestamp: datetime
    error: Optional[str] = None


class BatchEventRequest(BaseModel):
    """Batch event ingestion request"""
    events: List[HealthcareEvent] = Field(..., max_length=500)


class ProcessedEvent(HealthcareEvent):
    """Event after processing with additional fields"""
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    encryption_key_id: Optional[str] = None
    checksum: Optional[str] = None
    partition_key: Optional[str] = None
