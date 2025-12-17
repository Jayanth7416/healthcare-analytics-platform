"""Analytics Service"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import structlog

from src.models.analytics import (
    RealtimeMetrics,
    PatientAnalytics,
    ProviderAnalytics,
    EventDistribution,
    AggregationType,
    TimeSeriesData,
    TimeSeriesDataPoint
)
from src.services.database import DatabaseService
from src.services.cache import CacheService

logger = structlog.get_logger()


class AnalyticsService:
    """
    Analytics computation and retrieval service

    Provides real-time and historical analytics for:
    - Platform metrics
    - Patient analytics (de-identified)
    - Provider analytics
    - Time series data
    """

    def __init__(self):
        self.db = DatabaseService()
        self.cache = CacheService()

    async def get_realtime_metrics(self) -> RealtimeMetrics:
        """Get real-time platform metrics"""
        # Try cache first
        cached = await self.cache.get("metrics:realtime")
        if cached:
            return RealtimeMetrics(**cached)

        # Query from database
        metrics = await self.db.execute_query("""
            SELECT
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 minute') as events_per_minute,
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') as events_per_hour,
                COUNT(DISTINCT provider_id) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') as active_providers,
                COUNT(DISTINCT patient_id) FILTER (WHERE timestamp > NOW() - INTERVAL '1 hour') as active_patients,
                AVG(processing_time_ms) as avg_latency_ms,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_time_ms) as p99_latency_ms,
                COUNT(*) FILTER (WHERE status = 'error') * 100.0 / NULLIF(COUNT(*), 0) as error_rate
            FROM events
            WHERE timestamp > NOW() - INTERVAL '1 hour'
        """)

        result = RealtimeMetrics(
            events_per_minute=metrics.get('events_per_minute', 0),
            events_per_hour=metrics.get('events_per_hour', 0),
            active_providers=metrics.get('active_providers', 0),
            active_patients=metrics.get('active_patients', 0),
            current_throughput=metrics.get('events_per_minute', 0) / 60.0,
            error_rate=metrics.get('error_rate', 0.0),
            avg_latency_ms=metrics.get('avg_latency_ms', 0.0),
            p99_latency_ms=metrics.get('p99_latency_ms', 0.0)
        )

        # Cache for 10 seconds
        await self.cache.set("metrics:realtime", result.model_dump(), ttl=10)

        return result

    async def get_patient_analytics(
        self,
        patient_id: str,
        time_range: str
    ) -> Optional[PatientAnalytics]:
        """Get analytics for a specific patient"""
        interval = self._parse_time_range(time_range)

        query_result = await self.db.execute_query("""
            SELECT
                COUNT(*) as total_events,
                COUNT(*) FILTER (WHERE event_type = 'patient_visit') as visits,
                COUNT(*) FILTER (WHERE event_type = 'lab_result') as lab_results,
                COUNT(*) FILTER (WHERE event_type = 'prescription') as prescriptions,
                COUNT(*) FILTER (WHERE event_type = 'vitals') as vitals,
                COUNT(*) FILTER (WHERE event_type = 'diagnosis') as diagnoses,
                COUNT(*) FILTER (WHERE event_type = 'procedure') as procedures,
                MIN(timestamp) as first_event,
                MAX(timestamp) as last_event,
                COUNT(DISTINCT provider_id) as providers_count,
                COUNT(DISTINCT facility_id) as facilities_count
            FROM events
            WHERE patient_id_hash = %(patient_id_hash)s
            AND timestamp > NOW() - %(interval)s
        """, {
            'patient_id_hash': self._hash_patient_id(patient_id),
            'interval': interval
        })

        if not query_result or query_result.get('total_events', 0) == 0:
            return None

        return PatientAnalytics(
            patient_id_hash=self._hash_patient_id(patient_id),
            total_events=query_result['total_events'],
            event_distribution=EventDistribution(
                patient_visit=query_result.get('visits', 0),
                lab_result=query_result.get('lab_results', 0),
                prescription=query_result.get('prescriptions', 0),
                vitals=query_result.get('vitals', 0),
                diagnosis=query_result.get('diagnoses', 0),
                procedure=query_result.get('procedures', 0)
            ),
            first_event=query_result['first_event'],
            last_event=query_result['last_event'],
            providers_count=query_result['providers_count'],
            facilities_count=query_result['facilities_count'],
            time_range_days=self._get_days_from_range(time_range)
        )

    async def get_provider_analytics(
        self,
        provider_id: str,
        time_range: str
    ) -> Optional[ProviderAnalytics]:
        """Get analytics for a healthcare provider"""
        interval = self._parse_time_range(time_range)

        query_result = await self.db.execute_query("""
            SELECT
                p.name as provider_name,
                COUNT(e.*) as total_events,
                COUNT(DISTINCT e.patient_id) as unique_patients,
                COUNT(*) FILTER (WHERE e.event_type = 'patient_visit') as visits,
                COUNT(*) FILTER (WHERE e.event_type = 'lab_result') as lab_results,
                COUNT(*) FILTER (WHERE e.event_type = 'prescription') as prescriptions,
                COUNT(*) FILTER (WHERE e.event_type = 'vitals') as vitals,
                COUNT(*) FILTER (WHERE e.event_type = 'diagnosis') as diagnoses,
                COUNT(*) FILTER (WHERE e.event_type = 'procedure') as procedures,
                COUNT(*) * 1.0 / NULLIF(EXTRACT(EPOCH FROM %(interval)s) / 86400, 0) as avg_events_per_day,
                MODE() WITHIN GROUP (ORDER BY EXTRACT(HOUR FROM e.timestamp)) as peak_hour,
                MODE() WITHIN GROUP (ORDER BY EXTRACT(DOW FROM e.timestamp)) as peak_dow,
                COUNT(*) FILTER (WHERE e.status = 'error') * 100.0 / NULLIF(COUNT(*), 0) as error_rate,
                AVG(e.processing_time_ms) as avg_processing_time_ms
            FROM events e
            LEFT JOIN providers p ON e.provider_id = p.id
            WHERE e.provider_id = %(provider_id)s
            AND e.timestamp > NOW() - %(interval)s
            GROUP BY p.name
        """, {
            'provider_id': provider_id,
            'interval': interval
        })

        if not query_result or query_result.get('total_events', 0) == 0:
            return None

        dow_map = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']

        return ProviderAnalytics(
            provider_id=provider_id,
            provider_name=query_result.get('provider_name'),
            total_events=query_result['total_events'],
            unique_patients=query_result['unique_patients'],
            event_distribution=EventDistribution(
                patient_visit=query_result.get('visits', 0),
                lab_result=query_result.get('lab_results', 0),
                prescription=query_result.get('prescriptions', 0),
                vitals=query_result.get('vitals', 0),
                diagnosis=query_result.get('diagnoses', 0),
                procedure=query_result.get('procedures', 0)
            ),
            avg_events_per_day=query_result.get('avg_events_per_day', 0),
            peak_hour=int(query_result.get('peak_hour', 12)),
            peak_day=dow_map[int(query_result.get('peak_dow', 1))],
            error_rate=query_result.get('error_rate', 0.0),
            avg_processing_time_ms=query_result.get('avg_processing_time_ms', 0.0),
            time_range=time_range
        )

    async def get_timeseries(
        self,
        metric: str,
        time_range: str,
        aggregation: AggregationType,
        provider_id: Optional[str] = None
    ) -> TimeSeriesData:
        """Get time series data for a metric"""
        interval = self._parse_time_range(time_range)
        bucket = self._get_bucket_interval(aggregation)

        where_clause = "WHERE timestamp > NOW() - %(interval)s"
        params = {'interval': interval, 'bucket': bucket}

        if provider_id:
            where_clause += " AND provider_id = %(provider_id)s"
            params['provider_id'] = provider_id

        metric_column = {
            'event_count': 'COUNT(*)',
            'unique_patients': 'COUNT(DISTINCT patient_id)',
            'error_rate': 'COUNT(*) FILTER (WHERE status = \'error\') * 100.0 / NULLIF(COUNT(*), 0)',
            'latency_p99': 'PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_time_ms)'
        }.get(metric, 'COUNT(*)')

        query_result = await self.db.execute_query(f"""
            SELECT
                time_bucket(%(bucket)s, timestamp) as bucket_time,
                {metric_column} as value
            FROM events
            {where_clause}
            GROUP BY bucket_time
            ORDER BY bucket_time
        """, params)

        data_points = [
            TimeSeriesDataPoint(timestamp=row['bucket_time'], value=row['value'])
            for row in query_result
        ]

        values = [dp.value for dp in data_points]

        return TimeSeriesData(
            metric=metric,
            aggregation=aggregation,
            data_points=data_points,
            total=sum(values) if values else 0,
            average=sum(values) / len(values) if values else 0,
            min_value=min(values) if values else 0,
            max_value=max(values) if values else 0
        )

    async def get_summary(self, time_range: str) -> Dict[str, Any]:
        """Get platform analytics summary"""
        interval = self._parse_time_range(time_range)

        return await self.db.execute_query("""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT provider_id) as total_providers,
                COUNT(DISTINCT patient_id) as total_patients
            FROM events
            WHERE timestamp > NOW() - %(interval)s
        """, {'interval': interval})

    def _parse_time_range(self, time_range: str) -> str:
        """Convert time range string to PostgreSQL interval"""
        mapping = {
            '1h': 'INTERVAL \'1 hour\'',
            '24h': 'INTERVAL \'24 hours\'',
            '7d': 'INTERVAL \'7 days\'',
            '30d': 'INTERVAL \'30 days\''
        }
        return mapping.get(time_range, 'INTERVAL \'24 hours\'')

    def _get_days_from_range(self, time_range: str) -> int:
        """Get number of days from time range string"""
        mapping = {'1h': 1, '24h': 1, '7d': 7, '30d': 30}
        return mapping.get(time_range, 1)

    def _get_bucket_interval(self, aggregation: AggregationType) -> str:
        """Get time bucket interval for aggregation"""
        mapping = {
            AggregationType.MINUTE: '1 minute',
            AggregationType.HOUR: '1 hour',
            AggregationType.DAY: '1 day',
            AggregationType.WEEK: '1 week'
        }
        return mapping.get(aggregation, '1 hour')

    def _hash_patient_id(self, patient_id: str) -> str:
        """Hash patient ID for privacy"""
        import hashlib
        return hashlib.sha256(patient_id.encode()).hexdigest()[:16]
