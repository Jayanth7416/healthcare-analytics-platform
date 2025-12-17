"""Tests for Event Processing"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from src.models.event import HealthcareEvent, EventType, EventSource, EventMetadata
from src.services.event_processor import EventProcessor


@pytest.fixture
def sample_event():
    """Create a sample healthcare event for testing"""
    return HealthcareEvent(
        event_type=EventType.PATIENT_VISIT,
        provider_id="PROV-001",
        patient_id="PAT-12345",
        facility_id="FAC-NYC-001",
        department="cardiology",
        payload={
            "visit_type": "routine_checkup",
            "duration_minutes": 30,
            "diagnosis_code": "I10"
        },
        metadata=EventMetadata(
            source=EventSource.EHR_EPIC,
            version="1.0"
        )
    )


@pytest.fixture
def event_processor():
    """Create event processor instance"""
    with patch('src.services.event_processor.EncryptionService'):
        with patch('src.services.event_processor.CacheService'):
            return EventProcessor()


class TestHealthcareEvent:
    """Test HealthcareEvent model"""

    def test_event_creation(self, sample_event):
        """Test creating a valid healthcare event"""
        assert sample_event.event_type == EventType.PATIENT_VISIT
        assert sample_event.provider_id == "PROV-001"
        assert sample_event.patient_id == "PAT-12345"
        assert sample_event.event_id is not None

    def test_event_id_auto_generation(self):
        """Test that event_id is auto-generated if not provided"""
        event = HealthcareEvent(
            event_type=EventType.LAB_RESULT,
            provider_id="PROV-001",
            patient_id="PAT-12345",
            metadata=EventMetadata(source=EventSource.LAB_SYSTEM)
        )
        assert event.event_id is not None
        assert len(event.event_id) == 36  # UUID format

    def test_event_timestamp_default(self):
        """Test that timestamp defaults to current time"""
        event = HealthcareEvent(
            event_type=EventType.VITALS,
            provider_id="PROV-001",
            patient_id="PAT-12345",
            metadata=EventMetadata(source=EventSource.IOT_DEVICE)
        )
        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)

    def test_invalid_empty_provider_id(self):
        """Test validation fails for empty provider_id"""
        with pytest.raises(ValueError):
            HealthcareEvent(
                event_type=EventType.PRESCRIPTION,
                provider_id="",
                patient_id="PAT-12345",
                metadata=EventMetadata(source=EventSource.EHR_CERNER)
            )

    def test_invalid_empty_patient_id(self):
        """Test validation fails for empty patient_id"""
        with pytest.raises(ValueError):
            HealthcareEvent(
                event_type=EventType.DIAGNOSIS,
                provider_id="PROV-001",
                patient_id="",
                metadata=EventMetadata(source=EventSource.MANUAL_ENTRY)
            )


class TestEventProcessor:
    """Test EventProcessor service"""

    @pytest.mark.asyncio
    async def test_process_event(self, event_processor, sample_event):
        """Test processing a healthcare event"""
        event_processor.encryption_service.encrypt_phi = AsyncMock(return_value="encrypted_value")
        event_processor.encryption_service.current_key_id = "test-key-id"
        event_processor.cache.set = AsyncMock(return_value=True)

        processed = await event_processor.process(sample_event)

        assert processed.event_id == sample_event.event_id
        assert processed.processed_at is not None
        assert processed.checksum is not None
        assert processed.partition_key is not None

    @pytest.mark.asyncio
    async def test_partition_key_generation(self, event_processor, sample_event):
        """Test partition key is generated correctly"""
        event_processor.encryption_service.encrypt_phi = AsyncMock(return_value="encrypted")
        event_processor.encryption_service.current_key_id = "test-key"
        event_processor.cache.set = AsyncMock(return_value=True)

        processed = await event_processor.process(sample_event)

        expected_partition_key = f"{sample_event.provider_id}:{sample_event.event_type.value}"
        assert processed.partition_key == expected_partition_key

    @pytest.mark.asyncio
    async def test_checksum_consistency(self, event_processor, sample_event):
        """Test checksum is consistent for same event"""
        event_processor.encryption_service.encrypt_phi = AsyncMock(return_value="encrypted")
        event_processor.encryption_service.current_key_id = "test-key"
        event_processor.cache.set = AsyncMock(return_value=True)

        checksum1 = event_processor._generate_checksum(sample_event)
        checksum2 = event_processor._generate_checksum(sample_event)

        assert checksum1 == checksum2


class TestEventTypes:
    """Test different event types"""

    @pytest.mark.parametrize("event_type", [
        EventType.PATIENT_VISIT,
        EventType.LAB_RESULT,
        EventType.PRESCRIPTION,
        EventType.VITALS,
        EventType.DIAGNOSIS,
        EventType.PROCEDURE,
        EventType.DISCHARGE,
        EventType.ADMISSION,
    ])
    def test_all_event_types(self, event_type):
        """Test all event types can be created"""
        event = HealthcareEvent(
            event_type=event_type,
            provider_id="PROV-001",
            patient_id="PAT-12345",
            metadata=EventMetadata(source=EventSource.EHR_EPIC)
        )
        assert event.event_type == event_type
