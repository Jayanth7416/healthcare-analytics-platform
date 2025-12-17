"""Kinesis Consumer Service"""

import asyncio
import json
from datetime import datetime
from typing import Optional, Callable, List
import structlog
import boto3
from botocore.exceptions import ClientError

from src.utils.config import settings

logger = structlog.get_logger()


class KinesisConsumer:
    """
    AWS Kinesis Data Streams consumer

    Features:
    - Async event consumption
    - Checkpoint management
    - Automatic shard iteration
    - Error handling with retry
    """

    def __init__(self, processor: Optional[Callable] = None):
        self.client = boto3.client(
            'kinesis',
            region_name=settings.AWS_REGION
        )
        self.stream_name = settings.KINESIS_STREAM_NAME
        self.processor = processor or self._default_processor
        self.running = False
        self.checkpoint_table = {}

    async def start(self):
        """Start consuming from all shards"""
        self.running = True
        logger.info("kinesis_consumer_starting", stream=self.stream_name)

        try:
            shards = await self._get_shards()
            tasks = [
                self._consume_shard(shard['ShardId'])
                for shard in shards
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error("consumer_error", error=str(e))
            self.running = False

    async def stop(self):
        """Stop the consumer"""
        self.running = False
        logger.info("kinesis_consumer_stopped")

    async def _get_shards(self) -> List[dict]:
        """Get all shards for the stream"""
        response = self.client.describe_stream(StreamName=self.stream_name)
        return response['StreamDescription']['Shards']

    async def _consume_shard(self, shard_id: str):
        """Consume records from a specific shard"""
        iterator = await self._get_shard_iterator(shard_id)

        while self.running and iterator:
            try:
                response = self.client.get_records(
                    ShardIterator=iterator,
                    Limit=100
                )

                records = response.get('Records', [])
                if records:
                    await self._process_records(records, shard_id)

                iterator = response.get('NextShardIterator')

                # Avoid hitting rate limits
                if not records:
                    await asyncio.sleep(1)

            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ExpiredIteratorException':
                    iterator = await self._get_shard_iterator(shard_id)
                else:
                    logger.error(
                        "shard_consume_error",
                        shard_id=shard_id,
                        error=str(e)
                    )
                    await asyncio.sleep(5)

    async def _get_shard_iterator(self, shard_id: str) -> str:
        """Get shard iterator, resuming from checkpoint if available"""
        checkpoint = self.checkpoint_table.get(shard_id)

        if checkpoint:
            response = self.client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=checkpoint
            )
        else:
            response = self.client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType='LATEST'
            )

        return response['ShardIterator']

    async def _process_records(self, records: List[dict], shard_id: str):
        """Process a batch of records"""
        for record in records:
            try:
                data = json.loads(record['Data'].decode())
                await self.processor(data)

                # Update checkpoint
                self.checkpoint_table[shard_id] = record['SequenceNumber']

            except Exception as e:
                logger.error(
                    "record_processing_error",
                    sequence_number=record['SequenceNumber'],
                    error=str(e)
                )

        logger.info(
            "records_processed",
            shard_id=shard_id,
            count=len(records)
        )

    async def _default_processor(self, data: dict):
        """Default record processor"""
        logger.info("record_received", event_id=data.get('event_id'))


async def get_kinesis_health() -> dict:
    """Check Kinesis stream health"""
    try:
        client = boto3.client('kinesis', region_name=settings.AWS_REGION)
        response = client.describe_stream(
            StreamName=settings.KINESIS_STREAM_NAME
        )
        status = response['StreamDescription']['StreamStatus']
        return {
            "healthy": status == 'ACTIVE',
            "status": status
        }
    except Exception as e:
        return {
            "healthy": False,
            "status": "error",
            "error": str(e)
        }
