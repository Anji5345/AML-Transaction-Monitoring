import os
import json
import time
import zipfile
import argparse
import uuid
from datetime import datetime, timedelta

import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "txn.raw")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ZIP_FILE_PATH = os.path.join(
    BASE_DIR,
    "data",
    "raw",
    "PS_20174392719_1491204439457_log.csv.zip"
)

CSV_FILE_NAME = "PS_20174392719_1491204439457_log.csv"

PROJECT_TEST_CSV_PATH = os.path.join(
    BASE_DIR,
    "data",
    "raw",
    "aml_project_test_paysim.csv"
)

BASE_TIME = datetime(2026, 4, 1, 0, 0, 0)
AML_TYPES = {"TRANSFER", "CASH_OUT", "CASH_IN", "DEPOSIT", "PAYMENT"}


def parse_args():
    parser = argparse.ArgumentParser(description="Replay PaySim transactions to Kafka.")
    parser.add_argument("--max-rows", type=int, default=100, help="Maximum PaySim rows to read.")
    parser.add_argument("--tps", type=float, default=2.0, help="Transactions per second to publish.")
    parser.add_argument("--csv-path", help="Optional plain CSV file to replay instead of the zipped PaySim file.")
    parser.add_argument(
        "--inject",
        choices=["structuring", "layering", "velocity"],
        help="Send a synthetic acceptance-test pattern instead of PaySim rows.",
    )
    parser.add_argument("--customer-id", default="C_TEST_AML", help="Customer ID for injected events.")
    return parser.parse_args()


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )


def load_transactions_from_zip(zip_path: str, csv_name: str, max_rows: int = 100):
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"PaySim ZIP file not found: {zip_path}")

    with zipfile.ZipFile(zip_path) as zf:
        print("ZIP contents:", zf.namelist())
        with zf.open(csv_name) as file:
            df = pd.read_csv(file, nrows=max_rows)

    df = df[df["type"].isin(AML_TYPES)].copy()
    df.reset_index(drop=True, inplace=True)
    return df


def load_transactions_from_csv(csv_path: str, max_rows: int = 100):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path, nrows=max_rows)
    df = df[df["type"].isin(AML_TYPES)].copy()
    df.reset_index(drop=True, inplace=True)
    return df


def normalize_transaction_type(transaction_type: str) -> str:
    if transaction_type == "CASH_IN":
        return "DEPOSIT"
    return transaction_type


def convert_step_to_timestamp(step_value: int) -> str:
    txn_time = BASE_TIME + timedelta(minutes=int(step_value))
    return txn_time.strftime("%Y-%m-%d %H:%M:%S")


def build_event(row, index: int) -> dict:
    return {
        "transaction_id": f"TXN_{index + 1}",
        "customer_id": str(row["nameOrig"]),
        "transaction_type": normalize_transaction_type(str(row["type"])),
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


def build_injected_events(pattern_type: str, customer_id: str) -> list[dict]:
    run_id = uuid.uuid4().hex[:8]

    base_event = {
        "customer_id": customer_id,
        "name_dest": "C_TEST_DEST",
        "old_balance_orig": 100000.0,
        "new_balance_orig": 0.0,
        "old_balance_dest": 0.0,
        "new_balance_dest": 0.0,
        "is_fraud": 0,
        "is_flagged_fraud": 0,
    }

    if pattern_type == "structuring":
        transactions = [
            ("TRANSFER", 9800.0, 0),
            ("TRANSFER", 9800.0, 180),
            ("CASH_OUT", 9800.0, 720),
            ("TRANSFER", 9800.0, 1080),
        ]
    elif pattern_type == "layering":
        transactions = [
            ("DEPOSIT", 20000.0, 0),
            ("TRANSFER", 20000.0, 30),
            ("CASH_OUT", 20000.0, 60),
        ]
    else:
        transactions = [
            ("PAYMENT", 100.0 + index, index * 5)
            for index in range(10)
        ]

    events = []
    for index, (transaction_type, amount, minute_offset) in enumerate(transactions, start=1):
        event = {
            **base_event,
            "transaction_id": f"{pattern_type.upper()}_{run_id}_{index}",
            "transaction_type": transaction_type,
            "amount": amount,
            "transaction_time": (BASE_TIME + timedelta(minutes=minute_offset)).strftime("%Y-%m-%d %H:%M:%S"),
        }
        events.append(event)

    return events


def send_event(producer: KafkaProducer, event: dict):
    producer.send(
        topic=KAFKA_TOPIC,
        key=event["customer_id"],
        value=event,
    )
    print(f"Sent: {event}")


def sleep_for_tps(tps: float):
    if tps > 0:
        time.sleep(1 / tps)


def send_transactions(max_rows: int, tps: float, inject: str | None, customer_id: str, csv_path: str | None):
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Project test CSV path: {PROJECT_TEST_CSV_PATH}")
    print(f"Project test CSV exists: {os.path.exists(PROJECT_TEST_CSV_PATH)}")
    print(f"Original ZIP path: {ZIP_FILE_PATH}")
    print(f"Original ZIP exists: {os.path.exists(ZIP_FILE_PATH)}")

    producer = create_producer()

    if inject:
        events = build_injected_events(inject, customer_id)
        print(f"Sending injected {inject} pattern for customer {customer_id}...\n")

        for event in events:
            send_event(producer, event)
            sleep_for_tps(tps)
    else:
        if csv_path:
            print(f"Using CSV path from argument: {csv_path}")
            df = load_transactions_from_csv(csv_path, max_rows=max_rows)
        elif os.path.exists(PROJECT_TEST_CSV_PATH):
            print(f"Using project test CSV: {PROJECT_TEST_CSV_PATH}")
            df = load_transactions_from_csv(PROJECT_TEST_CSV_PATH, max_rows=max_rows)
        else:
            print(f"Using original zipped PaySim file: {ZIP_FILE_PATH}")
            df = load_transactions_from_zip(ZIP_FILE_PATH, CSV_FILE_NAME, max_rows=max_rows)

        print(f"Loaded {len(df)} transactions from dataset")
        print(f"Sending transactions to Kafka topic {KAFKA_TOPIC}...\n")

        for idx, row in df.iterrows():
            event = build_event(row, idx)
            send_event(producer, event)
            sleep_for_tps(tps)

    producer.flush()
    producer.close()
    print("\nAll transactions sent successfully.")


if __name__ == "__main__":
    args = parse_args()

    send_transactions(
        max_rows=args.max_rows,
        tps=args.tps,
        inject=args.inject,
        customer_id=args.customer_id,
        csv_path=args.csv_path,
    )
