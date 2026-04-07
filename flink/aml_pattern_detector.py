import json
from kafka import KafkaConsumer, KafkaProducer

from rules.structuring import StructuringDetector


KAFKA_INPUT_TOPIC = "txn.raw"
KAFKA_ALERT_TOPIC = "aml.alerts"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def create_consumer():
    return KafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="aml-detector-group-v1",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def main():
    consumer = create_consumer()
    producer = create_producer()
    detector = StructuringDetector()

    print("AML detector started. Listening for transactions...\n")

    for message in consumer:
        event = message.value
        print(f"Received event: {event}")

        alert = detector.process_event(event)

        if alert:
            print(f"ALERT GENERATED: {alert}")
            producer.send(KAFKA_ALERT_TOPIC, value=alert)
            producer.flush()


if __name__ == "__main__":
    main()