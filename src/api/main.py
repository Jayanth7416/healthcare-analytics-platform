"""
Healthcare Analytics Platform - Main API
Real-time analytics for healthcare event processing
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import structlog

from src.api.routes import events, analytics, health
from src.services.kinesis_consumer import KinesisConsumer
from src.utils.config import settings

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Healthcare Analytics Platform")

    # Initialize Kinesis consumer in background
    if settings.ENABLE_KINESIS_CONSUMER:
        consumer = KinesisConsumer()
        await consumer.start()

    yield

    logger.info("Shutting down Healthcare Analytics Platform")


app = FastAPI(
    title="Healthcare Analytics Platform",
    description="Real-time analytics API for healthcare event processing",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(events.router, prefix="/events", tags=["Events"])
app.include_router(analytics.router, prefix="/analytics", tags=["Analytics"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Healthcare Analytics Platform",
        "version": "1.0.0",
        "status": "operational",
        "docs": "/docs"
    }
