"""Event Processing Service"""

import hashlib
import json
from datetime import datetime
from typing import Optional, Dict, Any
import structlog

from src.models.event import HealthcareEvent, ProcessedEvent
from src.services.encryption import EncryptionService
from src.services.cache import CacheService

logger = structlog.get_logger()


class EventProcessor:
    """
    Processes incoming healthcare events

    Responsibilities:
    - Validate event data
    - Encrypt PHI fields
    - Generate checksums
    - Determine partition keys for Kinesis
    """

    def __init__(self):
        self.encryption_service = EncryptionService()
        self.cache = CacheService()

    async def process(self, event: HealthcareEvent) -> ProcessedEvent:
        """
        Process a healthcare event for ingestion

        Args:
            event: Raw healthcare event

        Returns:
            ProcessedEvent with encrypted PHI and metadata
        """
        logger.info(
            "processing_event",
            event_id=event.event_id,
            event_type=event.event_type
        )

        # Encrypt PHI fields
        encrypted_patient_id = await self.encryption_service.encrypt_phi(
            event.patient_id
        )

        # Encrypt sensitive payload fields if present
        encrypted_payload = await self._encrypt_payload(event.payload)

        # Generate checksum for data integrity
        checksum = self._generate_checksum(event)

        # Determine partition key for even distribution
        partition_key = self._generate_partition_key(event)

        # Create processed event
        processed_event = ProcessedEvent(
            event_id=event.event_id,
            event_type=event.event_type,
            timestamp=event.timestamp,
            provider_id=event.provider_id,
            patient_id=encrypted_patient_id,
            facility_id=event.facility_id,
            department=event.department,
            payload=encrypted_payload,
            metadata=event.metadata,
            processed_at=datetime.utcnow(),
            encryption_key_id=self.encryption_service.current_key_id,
            checksum=checksum,
            partition_key=partition_key
        )

        # Cache event status
        await self.cache.set(
            f"event:{event.event_id}:status",
            {"status": "processed", "timestamp": datetime.utcnow().isoformat()},
            ttl=3600
        )

        logger.info(
            "event_processed",
            event_id=event.event_id,
            partition_key=partition_key
        )

        return processed_event

    async def _encrypt_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive fields in payload"""
        sensitive_fields = [
            "ssn", "mrn", "dob", "address", "phone",
            "email", "insurance_id", "diagnosis_code"
        ]

        encrypted_payload = payload.copy()

        for field in sensitive_fields:
            if field in encrypted_payload:
                encrypted_payload[field] = await self.encryption_service.encrypt_phi(
                    str(encrypted_payload[field])
                )

        return encrypted_payload

    def _generate_checksum(self, event: HealthcareEvent) -> str:
        """Generate SHA-256 checksum for data integrity"""
        event_data = event.model_dump_json()
        return hashlib.sha256(event_data.encode()).hexdigest()

    def _generate_partition_key(self, event: HealthcareEvent) -> str:
        """
        Generate partition key for Kinesis

        Uses provider_id to ensure events from same provider
        are processed in order
        """
        return f"{event.provider_id}:{event.event_type.value}"

    async def get_status(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get processing status of an event"""
        status = await self.cache.get(f"event:{event_id}:status")
        return status
