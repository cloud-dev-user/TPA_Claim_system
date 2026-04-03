"""
Central configuration — reads from environment variables with safe defaults.
"""

import os


class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    KAFKA_CLAIM_TOPIC: str       = os.getenv("KAFKA_CLAIM_TOPIC", "claim-events")
    KAFKA_AUDIT_TOPIC: str       = os.getenv("KAFKA_AUDIT_TOPIC", "audit-log")
    KAFKA_ACKS: str              = os.getenv("KAFKA_ACKS", "all")
    KAFKA_RETRIES: int           = int(os.getenv("KAFKA_RETRIES", "5"))
    KAFKA_IDEMPOTENT: bool       = os.getenv("KAFKA_IDEMPOTENT", "true").lower() == "true"

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int   = int(os.getenv("REDIS_DB", "0"))

    # App
    APP_PORT: int       = int(os.getenv("APP_PORT", "8080"))
    APP_HOST: str       = os.getenv("APP_HOST", "0.0.0.0")
    SERVICE_INSTANCE: str = os.getenv("SERVICE_INSTANCE", "claim-service-1")
    LOG_LEVEL: str      = os.getenv("LOG_LEVEL", "INFO")
    DEBUG: bool         = os.getenv("DEBUG", "false").lower() == "true"


config = Config()
