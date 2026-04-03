"""
MD India Fraud Service — Kafka consumer loop.

Consumer group: fraud-consumers
Input topic:    claim-events
Output topics:  fraud-alerts, audit-log
"""

from __future__ import annotations

import json
import logging
import signal

import redis as redis_lib
from confluent_kafka import Consumer, KafkaError, Producer

from app.config import config
from app.fraud import FraudRule

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_running = True


def _handle_signal(sig, frame):
    global _running
    logger.info("Shutdown signal received")
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _delivery_report(err, msg):
    if err:
        logger.error("Delivery failed | topic=%s error=%s", msg.topic(), err)


def run():
    redis_client = redis_lib.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        decode_responses=True,
        socket_connect_timeout=5,
    )
    fraud_rule = FraudRule(redis_client)

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": config.KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": False,
        "client.id": "fraud-consumer",
    })

    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "client.id": "fraud-producer",
    })

    consumer.subscribe([config.KAFKA_INPUT_TOPIC])
    logger.info(
        "Fraud consumer started | group=%s topic=%s",
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
            assessment = fraud_rule.assess(claim)

            # Always publish to audit-log
            audit_payload = json.dumps({
                "event":      "fraud_assessment",
                "claim_id":   assessment.claim_id,
                "verdict":    assessment.to_dict()["verdict"],
                "risk_score": assessment.risk_score,
                "reasons":    assessment.reasons,
            }).encode("utf-8")
            producer.produce(
                topic=config.KAFKA_AUDIT_TOPIC,
                key=assessment.claim_id.encode("utf-8"),
                value=audit_payload,
                callback=_delivery_report,
            )

            # Publish fraud alert if flagged
            if assessment.is_flagged:
                alert_payload = json.dumps(assessment.to_dict()).encode("utf-8")
                producer.produce(
                    topic=config.KAFKA_OUTPUT_TOPIC,
                    key=assessment.claim_id.encode("utf-8"),
                    value=alert_payload,
                    callback=_delivery_report,
                )

            producer.poll(0)
            consumer.commit(asynchronous=False)

        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("Malformed message | offset=%d error=%s", msg.offset(), exc)
            consumer.commit(asynchronous=False)
        except Exception as exc:
            logger.exception("Unexpected error | error=%s", exc)

    producer.flush(10)
    consumer.close()
    logger.info("Fraud consumer stopped.")


if __name__ == "__main__":
    run()
