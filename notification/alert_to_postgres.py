import json
import os
import psycopg2
from kafka import KafkaConsumer
from email_service import send_alert_email

KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "aml.alerts")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("AML_POSTGRES_GROUP_ID", "aml-postgres-consumer-v1")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "amldb"),
    "user": os.getenv("POSTGRES_USER", "amluser"),
    "password": os.getenv("POSTGRES_PASSWORD", "amlpass"),
}


def create_consumer():
    return KafkaConsumer(
        KAFKA_ALERT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )


def insert_alert(alert):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    evidence = alert.get("evidence") or alert.get("transactions", [])
    first_txn = evidence[0]

    cur.execute(
        """
        INSERT INTO aml_cases (
            alert_id,
            customer_id,
            transaction_id,
            pattern_type,
            alert_message,
            risk_score,
            priority_score,
            total_amount,
            transaction_time,
            evidence
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (alert_id) DO NOTHING
        """,
        (
            alert["alert_id"],
            alert["customer_id"],
            first_txn.get("transaction_id"),
            alert["pattern_type"],
            alert["alert_message"],
            alert["risk_score"],
            alert["risk_score"],
            alert.get("total_amount", first_txn.get("amount")),
            first_txn.get("transaction_time"),
            json.dumps(evidence),
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted alert into DB: {alert['pattern_type']}")


def notify_alert(alert):
    try:
        send_alert_email(alert)
    except Exception as exc:
        print(f"Email notification failed for alert {alert.get('alert_id')}: {exc}")


def main():
    consumer = create_consumer()
    print(f"Postgres consumer started. Reading {KAFKA_ALERT_TOPIC}.")

    for message in consumer:
        alert = message.value
        insert_alert(alert)
        notify_alert(alert)
        consumer.commit()


if __name__ == "__main__":
    main()
