"""Event ingestion endpoints"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from typing import List
from datetime import datetime
import structlog

from src.models.event import HealthcareEvent, EventResponse, BatchEventRequest
from src.services.event_processor import EventProcessor
from src.services.kinesis_producer import KinesisProducer
from src.utils.auth import verify_api_key

router = APIRouter()
logger = structlog.get_logger()

event_processor = EventProcessor()
kinesis_producer = KinesisProducer()


@router.post("/ingest", response_model=EventResponse)
async def ingest_event(
    event: HealthcareEvent,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key)
):
    """
    Ingest a single healthcare event

    - Validates event schema
    - Encrypts PHI data
    - Publishes to Kinesis stream
    - Returns event ID for tracking
    """
    try:
        # Validate and enrich event
        processed_event = await event_processor.process(event)

        # Send to Kinesis asynchronously
        background_tasks.add_task(
            kinesis_producer.send_event,
            processed_event
        )

        logger.info(
            "event_ingested",
            event_id=processed_event.event_id,
            event_type=processed_event.event_type
        )

        return EventResponse(
            event_id=processed_event.event_id,
            status="accepted",
            timestamp=datetime.utcnow()
        )

    except ValueError as e:
        logger.error("event_validation_failed", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ingest/batch", response_model=List[EventResponse])
async def ingest_batch(
    request: BatchEventRequest,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key)
):
    """
    Ingest multiple healthcare events in batch

    - Processes up to 500 events per request
    - Returns individual status for each event
    """
    if len(request.events) > 500:
        raise HTTPException(
            status_code=400,
            detail="Batch size exceeds maximum of 500 events"
        )

    responses = []
    for event in request.events:
        try:
            processed_event = await event_processor.process(event)
            background_tasks.add_task(
                kinesis_producer.send_event,
                processed_event
            )
            responses.append(EventResponse(
                event_id=processed_event.event_id,
                status="accepted",
                timestamp=datetime.utcnow()
            ))
        except Exception as e:
            responses.append(EventResponse(
                event_id=event.event_id or "unknown",
                status="rejected",
                error=str(e),
                timestamp=datetime.utcnow()
            ))

    logger.info("batch_ingested", total=len(request.events), accepted=sum(1 for r in responses if r.status == "accepted"))

    return responses


@router.get("/status/{event_id}")
async def get_event_status(
    event_id: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get processing status of an event
    """
    status = await event_processor.get_status(event_id)
    if not status:
        raise HTTPException(status_code=404, detail="Event not found")
    return status
