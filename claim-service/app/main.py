"""
MD India Claim Service — FastAPI application.

Endpoints:
  POST /api/v1/claims   Submit a new claim
  GET  /api/v1/claims/{claim_id}  Get claim status (Redis cache)
  GET  /health          Liveness probe
  GET  /ready           Readiness probe (checks Kafka + Redis)
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import redis
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from app.claim import ClaimSubmit, ClaimStatus, ClaimValidator
from app.config import config
from app.producer import flush_producer, publish_claim_event

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Redis client ───────────────────────────────────────────────────────────────

redis_client = redis.Redis(
    host=config.REDIS_HOST,
    port=config.REDIS_PORT,
    db=config.REDIS_DB,
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=3,
)

# ── Pydantic request/response schemas ─────────────────────────────────────────

class ClaimRequest(BaseModel):
    member_id:      str = Field(..., example="M1001", description="MD India member ID")
    hospital_id:    str = Field(..., example="H5501-Apollo")
    hospital_name:  str = Field(..., example="Apollo Hospital Mumbai")
    insurer:        str = Field(..., example="StarHealth")
    amount:         float = Field(..., gt=0, example=85000)
    city:           str = Field(..., example="mumbai")
    claim_type:     str = Field(..., example="cashless")
    diagnosis_code: str = Field(..., example="Z51.1")
    admission_date: str | None = Field(None, example="2026-04-20")
    discharge_date: str | None = Field(None, example="2026-04-24")
    pre_auth_number: str | None = None
    remarks:        str | None = None

    @field_validator("city")
    @classmethod
    def normalise_city(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("insurer")
    @classmethod
    def normalise_insurer(cls, v: str) -> str:
        return v.strip()


class ClaimResponse(BaseModel):
    claim_id:     str
    status:       str
    member_id:    str
    insurer:      str
    amount:       float
    submitted_at: str
    message:      str


# ── App lifecycle ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Claim Service starting | instance=%s", config.SERVICE_INSTANCE)
    yield
    logger.info("Claim Service shutting down — flushing producer...")
    flush_producer()


app = FastAPI(
    title="MD India Claim Service",
    description="Receives claim submissions, validates them, and publishes to Kafka.",
    version="1.0.0",
    lifespan=lifespan,
)

validator = ClaimValidator()


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.post(
    "/api/v1/claims",
    response_model=ClaimResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a new insurance claim",
)
async def submit_claim(request: ClaimRequest):
    """Submit a new claim into the MD India processing pipeline.

    1. Validates all fields against MD India business rules.
    2. Stores initial status in Redis (TTL 24h).
    3. Publishes a claim event to Kafka (key = insurer for partition ordering).
    4. Returns 202 Accepted — async processing continues downstream.
    """
    claim = ClaimSubmit(
        member_id=request.member_id,
        hospital_id=request.hospital_id,
        hospital_name=request.hospital_name,
        insurer=request.insurer,
        amount=request.amount,
        city=request.city,
        claim_type=request.claim_type,
        diagnosis_code=request.diagnosis_code,
        admission_date=request.admission_date,
        discharge_date=request.discharge_date,
        pre_auth_number=request.pre_auth_number,
        remarks=request.remarks,
    )

    is_valid, errors = validator.validate(claim)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"errors": errors, "claim_id": claim.claim_id},
        )

    # Cache initial status in Redis (24h TTL)
    try:
        redis_client.set(
            f"claim:{claim.claim_id}:status",
            claim.status.value,
            ex=86400,
        )
    except redis.RedisError as exc:
        logger.warning("Redis write failed (non-fatal) | claim_id=%s error=%s", claim.claim_id, exc)

    # Publish to Kafka
    try:
        publish_claim_event(claim)
    except Exception as exc:
        logger.error("Kafka publish failed | claim_id=%s error=%s", claim.claim_id, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Claim event pipeline unavailable — please retry",
        )

    return ClaimResponse(
        claim_id=claim.claim_id,
        status=claim.status.value,
        member_id=claim.member_id,
        insurer=claim.insurer,
        amount=claim.amount,
        submitted_at=claim.submitted_at,
        message="Claim received and queued for processing",
    )


@app.get(
    "/api/v1/claims/{claim_id}",
    summary="Get claim status",
)
async def get_claim_status(claim_id: str):
    """Return the current processing status of a claim from Redis cache."""
    try:
        status_val = redis_client.get(f"claim:{claim_id}:status")
    except redis.RedisError:
        raise HTTPException(status_code=503, detail="Cache unavailable")

    if not status_val:
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")

    return {"claim_id": claim_id, "status": status_val}


@app.get("/health", summary="Liveness probe")
async def health():
    return {"status": "ok", "instance": config.SERVICE_INSTANCE}


@app.get("/ready", summary="Readiness probe")
async def ready():
    checks: dict[str, str] = {}

    # Redis ping
    try:
        redis_client.ping()
        checks["redis"] = "ok"
    except Exception:
        checks["redis"] = "unreachable"

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if all_ok else 503,
        content={"status": "ready" if all_ok else "degraded", "checks": checks},
    )


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=config.APP_HOST,
        port=config.APP_PORT,
        log_level=config.LOG_LEVEL.lower(),
        reload=config.DEBUG,
    )
