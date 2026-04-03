import os


class Config:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    KAFKA_CONSUMER_GROUP: str    = os.getenv("KAFKA_CONSUMER_GROUP", "eligibility-consumers")
    KAFKA_INPUT_TOPIC: str       = os.getenv("KAFKA_INPUT_TOPIC", "claim-events")
    KAFKA_OUTPUT_TOPIC: str      = os.getenv("KAFKA_OUTPUT_TOPIC", "eligibility-results")
    KAFKA_AUDIT_TOPIC: str       = os.getenv("KAFKA_AUDIT_TOPIC", "audit-log")
    KAFKA_AUTO_OFFSET_RESET: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int   = int(os.getenv("REDIS_DB", "0"))
    POLICY_TTL: int = int(os.getenv("POLICY_TTL", "900"))   # 15 min cache

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
