import json
import os
import uuid
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

from rules.structuring import StructuringDetector
from rules.layering import LayeringDetector
from rules.velocity import VelocityDetector


KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "txn.raw")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "aml.alerts")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("AML_DETECTOR_GROUP_ID", "aml-detector-group-v1")


def create_consumer():
    return KafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )


def enrich_alert(alert: dict) -> dict:
    transactions = alert.get("transactions", [])
    total_amount = sum(float(transaction.get("amount", 0)) for transaction in transactions)
    customer_id = alert["customer_id"]
    pattern_type = alert["pattern_type"]

    return {
        "alert_id": str(uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{customer_id}:{pattern_type}:{','.join(t.get('transaction_id', '') for t in transactions)}",
        )),
        "customer_id": customer_id,
        "pattern_type": pattern_type,
        "alert_message": alert["alert_message"],
        "risk_score": alert["risk_score"],
        "total_amount": total_amount,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "transactions": transactions,
        "evidence": transactions,
    }


def main():
    consumer = create_consumer()
    producer = create_producer()

    structuring_detector = StructuringDetector()
    layering_detector = LayeringDetector()
    velocity_detector = VelocityDetector()

    print(f"AML detector started. Reading {KAFKA_INPUT_TOPIC}, writing {KAFKA_ALERT_TOPIC}.")

    for message in consumer:
        event = message.value
        print(f"Received event: {event}")

        alerts = []

        structuring_alert = structuring_detector.process_event(event)
        if structuring_alert:
            alerts.append(structuring_alert)

        layering_alert = layering_detector.process_event(event)
        if layering_alert:
            alerts.append(layering_alert)

        velocity_alert = velocity_detector.process_event(event)
        if velocity_alert:
            alerts.append(velocity_alert)

        for alert in alerts:
            enriched_alert = enrich_alert(alert)
            print(f"ALERT GENERATED: {enriched_alert}")
            producer.send(
                KAFKA_ALERT_TOPIC,
                key=enriched_alert["customer_id"],
                value=enriched_alert,
            )
            producer.flush()

        consumer.commit()


if __name__ == "__main__":
    main()
