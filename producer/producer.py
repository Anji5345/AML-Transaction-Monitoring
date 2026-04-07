import os
import json
import time
import zipfile
from datetime import datetime, timedelta

import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC = "txn.raw"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ZIP_FILE_PATH = os.path.join(BASE_DIR, "data", "raw", "PS_20174392719_1491204439457_log.csv.zip")
CSV_FILE_NAME = "PS_20174392719_1491204439457_log.csv"

BASE_TIME = datetime(2026, 4, 1, 0, 0, 0)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8")
    )


def load_transactions_from_zip(zip_path: str, csv_name: str, max_rows: int = 100):
    with zipfile.ZipFile(zip_path) as zf:
        print("ZIP contents:", zf.namelist())
        with zf.open(csv_name) as file:
            df = pd.read_csv(file, nrows=max_rows)

    df = df[df["type"].isin(["TRANSFER", "CASH_OUT", "DEPOSIT", "PAYMENT"])].copy()
    df.reset_index(drop=True, inplace=True)
    return df


def convert_step_to_timestamp(step_value: int) -> str:
    txn_time = BASE_TIME + timedelta(minutes=int(step_value))
    return txn_time.strftime("%Y-%m-%d %H:%M:%S")


def build_event(row, index: int) -> dict:
    return {
        "transaction_id": f"TXN_{index + 1}",
        "customer_id": str(row["nameOrig"]),
        "transaction_type": str(row["type"]),
        "amount": float(row["amount"]),
        "transaction_time": convert_step_to_timestamp(row["step"]),
        "name_dest": str(row["nameDest"]),
        "old_balance_orig": float(row["oldbalanceOrg"]),
        "new_balance_orig": float(row["newbalanceOrig"]),
        "old_balance_dest": float(row["oldbalanceDest"]),
        "new_balance_dest": float(row["newbalanceDest"]),
        "is_fraud": int(row["isFraud"]),
        "is_flagged_fraud": int(row["isFlaggedFraud"])
    }


def send_transactions():
    print("Using ZIP path:", ZIP_FILE_PATH)
    print("File exists:", os.path.exists(ZIP_FILE_PATH))

    producer = create_producer()
    df = load_transactions_from_zip(ZIP_FILE_PATH, CSV_FILE_NAME, max_rows=100)

    print(f"Loaded {len(df)} transactions from PaySim dataset")
    print("Sending transactions to Kafka topic txn.raw...\n")

    for idx, row in df.iterrows():
        event = build_event(row, idx)
        producer.send(
            topic=KAFKA_TOPIC,
            key=event["customer_id"],
            value=event
        )
        print(f"Sent: {event}")
        time.sleep(0.5)

    producer.flush()
    producer.close()
    print("\nAll transactions sent successfully.")


if __name__ == "__main__":
    send_transactions()