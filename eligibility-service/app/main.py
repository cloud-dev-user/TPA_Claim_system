"""
HealthOne TPA Eligibility Service — Kafka consumer loop.

Consumer group: eligibility-consumers
Input topic:    claim-events
Output topics:  eligibility-results, audit-log
"""

from __future__ import annotations

import json
import logging
import signal
import sys

import redis as redis_lib
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from app.config import config
from app.eligibility import EligibilityCheck

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Graceful shutdown ──────────────────────────────────────────────────────────

_running = True


def _handle_signal(sig, frame):
    global _running
    logger.info("Received signal %s — shutting down...", sig)
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Kafka helpers ──────────────────────────────────────────────────────────────

def _make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": config.KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,      # manual commit for at-least-once
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 300000,
        "client.id": "eligibility-consumer",
    })


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "client.id": "eligibility-producer",
    })


def _delivery_report(err, msg):
    if err:
        logger.error("Delivery failed | topic=%s error=%s", msg.topic(), err)


# ── Main consumer loop ─────────────────────────────────────────────────────────

def run():
    redis_client = redis_lib.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        decode_responses=True,
        socket_connect_timeout=5,
    )
    checker  = EligibilityCheck(redis_client)
    consumer = _make_consumer()
    producer = _make_producer()

    consumer.subscribe([config.KAFKA_INPUT_TOPIC])
    logger.info(
        "Eligibility consumer started | group=%s topic=%s",
        config.KAFKA_CONSUMER_GROUP, config.KAFKA_INPUT_TOPIC,
    )

    while _running:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Consumer error: %s", msg.error())
            continue

        try:
            claim = json.loads(msg.value().decode("utf-8"))
            logger.info(
                "Processing claim | claim_id=%s member_id=%s amount=%.0f",
                claim.get("claim_id"), claim.get("member_id"), float(claim.get("amount", 0)),
            )

            result = checker.check(claim)

            # Publish eligibility result
            result_payload = json.dumps(result.to_dict()).encode("utf-8")
            producer.produce(
                topic=config.KAFKA_OUTPUT_TOPIC,
                key=claim.get("claim_id", "").encode("utf-8"),
                value=result_payload,
                callback=_delivery_report,
            )

            # Publish to audit-log
            audit_payload = json.dumps({
                "event":    "eligibility_decision",
                "claim_id": result.claim_id,
                "decision": result.to_dict()["decision"],
                "member_id": result.member_id,
                "reason":   result.reason,
            }).encode("utf-8")
            producer.produce(
                topic=config.KAFKA_AUDIT_TOPIC,
                key=claim.get("claim_id", "").encode("utf-8"),
                value=audit_payload,
                callback=_delivery_report,
            )

            producer.poll(0)

            # Manual commit after successful processing
            consumer.commit(asynchronous=False)

            logger.info(
                "Eligibility decision | claim_id=%s decision=%s reason=%s",
                result.claim_id,
                result.to_dict()["decision"],
                result.reason,
            )

        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("Malformed message | offset=%d error=%s", msg.offset(), exc)
            consumer.commit(asynchronous=False)  # skip bad message
        except Exception as exc:
            logger.exception("Unexpected error processing message | error=%s", exc)

    logger.info("Shutting down eligibility consumer...")
    producer.flush(10)
    consumer.close()
    logger.info("Eligibility consumer stopped.")


if __name__ == "__main__":
    run()
