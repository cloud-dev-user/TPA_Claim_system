import os


class Config:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    KAFKA_CONSUMER_GROUP: str    = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-consumers")
    KAFKA_INPUT_TOPIC: str       = os.getenv("KAFKA_INPUT_TOPIC", "claim-events")
    KAFKA_OUTPUT_TOPIC: str      = os.getenv("KAFKA_OUTPUT_TOPIC", "fraud-alerts")
    KAFKA_AUDIT_TOPIC: str       = os.getenv("KAFKA_AUDIT_TOPIC", "audit-log")
    KAFKA_AUTO_OFFSET_RESET: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int   = int(os.getenv("REDIS_DB", "0"))

    # Fraud thresholds
    FRAUD_SCORE_THRESHOLD: float   = float(os.getenv("FRAUD_SCORE_THRESHOLD", "50"))
    HIGH_AMOUNT_THRESHOLD: float   = float(os.getenv("HIGH_AMOUNT_THRESHOLD", "500000"))
    DAILY_CLAIM_LIMIT: int         = int(os.getenv("DAILY_CLAIM_LIMIT", "5"))
    DAILY_CLAIM_SCORE_WEIGHT: int  = int(os.getenv("DAILY_CLAIM_SCORE_WEIGHT", "10"))

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
