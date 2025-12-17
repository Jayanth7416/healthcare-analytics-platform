# Healthcare Analytics Platform

A real-time analytics platform for processing healthcare events at scale. Built with Python, AWS services, and modern data streaming technologies.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Data Sources  │────▶│  AWS Kinesis    │────▶│  Event Processor│
│  (EHR, Devices) │     │  Data Streams   │     │  (Lambda/Spark) │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                        ┌─────────────────┐              │
                        │   Amazon        │◀─────────────┘
                        │   Redshift      │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │  Analytics API  │
                        │   (FastAPI)     │
                        └─────────────────┘
```

## Features

- **Real-time Event Processing**: Handle 1M+ events/hour with automatic scaling
- **HIPAA Compliant**: End-to-end encryption and audit logging
- **Multi-tenant Architecture**: Isolated data processing per healthcare provider
- **Fault Tolerant**: Automatic retry mechanisms and dead letter queues
- **Analytics Dashboard API**: RESTful endpoints for real-time metrics

## Tech Stack

- **Language**: Python 3.11+
- **API Framework**: FastAPI
- **Streaming**: AWS Kinesis, Apache Kafka
- **Processing**: Apache Spark, AWS Lambda
- **Database**: Amazon Redshift, PostgreSQL
- **Infrastructure**: Terraform, AWS CDK
- **Monitoring**: CloudWatch, Prometheus

## Project Structure

```
healthcare-analytics-platform/
├── src/
│   ├── api/              # FastAPI endpoints
│   ├── services/         # Business logic
│   ├── models/           # Data models
│   └── utils/            # Helper functions
├── tests/                # Unit and integration tests
├── config/               # Configuration files
├── terraform/            # Infrastructure as Code
└── docs/                 # Documentation
```

## Quick Start

### Prerequisites

- Python 3.11+
- AWS CLI configured
- Docker (optional)

### Installation

```bash
# Clone the repository
git clone https://github.com/Jayanth7416/healthcare-analytics-platform.git
cd healthcare-analytics-platform

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the API server
uvicorn src.api.main:app --reload
```

### Environment Variables

```bash
export AWS_REGION=us-east-1
export KINESIS_STREAM_NAME=healthcare-events
export REDSHIFT_HOST=your-cluster.redshift.amazonaws.com
export REDSHIFT_DB=analytics
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/events/ingest` | Ingest healthcare events |
| GET | `/analytics/realtime` | Real-time metrics |
| GET | `/analytics/patients/{id}` | Patient analytics |
| GET | `/analytics/providers/{id}` | Provider analytics |

## Event Schema

```json
{
  "event_id": "uuid",
  "event_type": "patient_visit|lab_result|prescription|vitals",
  "timestamp": "ISO8601",
  "provider_id": "string",
  "patient_id": "string (encrypted)",
  "payload": {},
  "metadata": {
    "source": "ehr_system",
    "version": "1.0"
  }
}
```

## Performance

- **Throughput**: 1M+ events/hour
- **Latency**: < 100ms p99 for API responses
- **Availability**: 99.99% uptime SLA

## License

MIT License

## Author

Jayanth Kumar Panuganti - [LinkedIn](https://linkedin.com/in/jayanth7416)
