"""Kinesis Producer Service"""

import json
import asyncio
from typing import Optional, List
import structlog
import boto3
from botocore.exceptions import ClientError

from src.models.event import ProcessedEvent
from src.utils.config import settings

logger = structlog.get_logger()


class KinesisProducer:
    """
    AWS Kinesis Data Streams producer

    Features:
    - Async event publishing
    - Automatic retries with exponential backoff
    - Batch publishing support
    - Dead letter queue for failed events
    """

    def __init__(self):
        self.client = boto3.client(
            'kinesis',
            region_name=settings.AWS_REGION
        )
        self.stream_name = settings.KINESIS_STREAM_NAME
        self.dlq_stream_name = settings.KINESIS_DLQ_STREAM_NAME
        self.max_retries = 3
        self.base_delay = 0.1  # seconds

    async def send_event(self, event: ProcessedEvent) -> bool:
        """
        Send a single event to Kinesis stream

        Args:
            event: Processed healthcare event

        Returns:
            True if successful, False otherwise
        """
        for attempt in range(self.max_retries):
            try:
                response = await self._put_record(event)

                logger.info(
                    "event_published",
                    event_id=event.event_id,
                    shard_id=response.get('ShardId'),
                    sequence_number=response.get('SequenceNumber')
                )
                return True

            except ClientError as e:
                error_code = e.response['Error']['Code']
                logger.warning(
                    "kinesis_publish_retry",
                    event_id=event.event_id,
                    attempt=attempt + 1,
                    error_code=error_code
                )

                if error_code == 'ProvisionedThroughputExceededException':
                    # Exponential backoff
                    delay = self.base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    break

        # Send to DLQ after max retries
        await self._send_to_dlq(event)
        return False

    async def send_batch(self, events: List[ProcessedEvent]) -> dict:
        """
        Send multiple events in batch

        Args:
            events: List of processed events

        Returns:
            Dictionary with success and failure counts
        """
        records = [
            {
                'Data': event.model_dump_json().encode(),
                'PartitionKey': event.partition_key
            }
            for event in events
        ]

        try:
            response = self.client.put_records(
                StreamName=self.stream_name,
                Records=records
            )

            failed_count = response.get('FailedRecordCount', 0)
            success_count = len(events) - failed_count

            logger.info(
                "batch_published",
                total=len(events),
                success=success_count,
                failed=failed_count
            )

            # Handle failed records
            if failed_count > 0:
                await self._handle_failed_records(events, response['Records'])

            return {
                "total": len(events),
                "success": success_count,
                "failed": failed_count
            }

        except ClientError as e:
            logger.error("batch_publish_failed", error=str(e))
            # Send all to DLQ
            for event in events:
                await self._send_to_dlq(event)

            return {
                "total": len(events),
                "success": 0,
                "failed": len(events)
            }

    async def _put_record(self, event: ProcessedEvent) -> dict:
        """Put a single record to Kinesis"""
        return self.client.put_record(
            StreamName=self.stream_name,
            Data=event.model_dump_json().encode(),
            PartitionKey=event.partition_key
        )

    async def _send_to_dlq(self, event: ProcessedEvent):
        """Send failed event to Dead Letter Queue"""
        try:
            self.client.put_record(
                StreamName=self.dlq_stream_name,
                Data=event.model_dump_json().encode(),
                PartitionKey=event.partition_key
            )
            logger.warning("event_sent_to_dlq", event_id=event.event_id)
        except ClientError as e:
            logger.error(
                "dlq_publish_failed",
                event_id=event.event_id,
                error=str(e)
            )

    async def _handle_failed_records(
        self,
        events: List[ProcessedEvent],
        results: List[dict]
    ):
        """Handle failed records from batch publish"""
        for event, result in zip(events, results):
            if 'ErrorCode' in result:
                await self._send_to_dlq(event)
