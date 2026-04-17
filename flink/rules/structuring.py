from collections import defaultdict
from datetime import datetime, timedelta


def parse_transaction_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


class StructuringDetector:
    def __init__(self, min_amount=9000, max_amount=10000, threshold_count=3, window_hours=24):
        self.min_amount = min_amount
        self.max_amount = max_amount
        self.threshold_count = threshold_count
        self.window_hours = window_hours
        self.customer_transactions = defaultdict(list)
        self.customer_seen_txns = defaultdict(set)
        self.alert_markers = set()

    def process_event(self, event):
        customer_id = event["customer_id"]
        transaction_id = event["transaction_id"]
        amount = float(event["amount"])
        transaction_time = parse_transaction_time(event["transaction_time"])
        window_start = transaction_time - timedelta(hours=self.window_hours)

        if transaction_id in self.customer_seen_txns[customer_id]:
            return None

        if self.min_amount < amount < self.max_amount:
            self.customer_seen_txns[customer_id].add(transaction_id)
            self.customer_transactions[customer_id].append(event)

        self.customer_transactions[customer_id] = [
            txn
            for txn in self.customer_transactions[customer_id]
            if parse_transaction_time(txn["transaction_time"]) >= window_start
        ]

        evidence = self.customer_transactions[customer_id]
        txn_count = len(evidence)
        alert_marker = self._build_alert_marker(customer_id, evidence)

        if txn_count == self.threshold_count and alert_marker not in self.alert_markers:
            self.alert_markers.add(alert_marker)

            return {
                "customer_id": customer_id,
                "pattern_type": "STRUCTURING",
                "alert_message": f"{self.threshold_count} near-threshold transactions detected within 24 hours",
                "risk_score": 85,
                "transactions": list(evidence),
            }

        return None

    def _build_alert_marker(self, customer_id, transactions):
        transaction_ids = ",".join(txn["transaction_id"] for txn in transactions)
        return f"{customer_id}:STRUCTURING:{transaction_ids}"
