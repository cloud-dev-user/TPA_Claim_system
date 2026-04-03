"""
Kafka producer for claim events.

Uses an idempotent producer (enable.idempotence=True, acks=all) to satisfy
the Exactly-Once requirement discussed in Kafka Session 4.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.claim import ClaimSubmit
from app.config import config

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Producer singleton
# ──────────────────────────────────────────────────────────────────────────────

_producer: Optional[Producer] = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        producer_config = {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "acks": config.KAFKA_ACKS,
            "enable.idempotence": config.KAFKA_IDEMPOTENT,
            "retries": config.KAFKA_RETRIES,
            "retry.backoff.ms": 300,
            "linger.ms": 5,
            "compression.type": "snappy",
            "client.id": f"{config.SERVICE_INSTANCE}-producer",
        }
        _producer = Producer(producer_config)
        logger.info(
            "Kafka producer created | bootstrap=%s | idempotent=%s",
            config.KAFKA_BOOTSTRAP_SERVERS,
            config.KAFKA_IDEMPOTENT,
        )
    return _producer


# ──────────────────────────────────────────────────────────────────────────────
# Delivery callback
# ──────────────────────────────────────────────────────────────────────────────

def _delivery_report(err, msg) -> None:
    if err:
        logger.error(
            "Delivery failed | topic=%s key=%s error=%s",
            msg.topic(), msg.key(), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(), msg.key().decode(),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def publish_claim_event(claim: ClaimSubmit) -> None:
    """Publish a claim event to the claim-events topic.

    The message KEY is the insurer code — this ensures all claims from the
    same insurer land in the same partition (ordering guarantee).
    """
    producer = _get_producer()
    payload = json.dumps(claim.to_dict()).encode("utf-8")
    key = claim.insurer.encode("utf-8")

    try:
        producer.produce(
            topic=config.KAFKA_CLAIM_TOPIC,
            key=key,
            value=payload,
            callback=_delivery_report,
        )
        producer.poll(0)  # trigger callbacks without blocking
        logger.info(
            "Claim event queued | claim_id=%s insurer=%s amount=%.2f",
            claim.claim_id, claim.insurer, claim.amount,
        )
    except KafkaException as exc:
        logger.error("Failed to queue claim event | claim_id=%s error=%s", claim.claim_id, exc)
        raise


def flush_producer(timeout: float = 10.0) -> None:
    """Flush all outstanding messages (call on app shutdown)."""
    if _producer:
        remaining = _producer.flush(timeout)
        if remaining:
            logger.warning("Producer flush timed out — %d messages still in queue", remaining)
