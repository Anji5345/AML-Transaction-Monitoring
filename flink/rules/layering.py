from collections import defaultdict
from datetime import datetime, timedelta


def parse_transaction_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


class LayeringDetector:
    def __init__(self, window_hours=2, min_total_amount=50000):
        self.window_hours = window_hours
        self.min_total_amount = min_total_amount
        self.customer_transactions = defaultdict(list)
        self.alert_markers = set()

    def process_event(self, event):
        customer_id = event["customer_id"]
        transaction_time = parse_transaction_time(event["transaction_time"])
        window_start = transaction_time - timedelta(hours=self.window_hours)

        self.customer_transactions[customer_id].append(event)
        self.customer_transactions[customer_id] = [
            txn
            for txn in self.customer_transactions[customer_id]
            if parse_transaction_time(txn["transaction_time"]) >= window_start
        ][-5:]

        txns = self.customer_transactions[customer_id]

        if len(txns) < 3:
            return None

        sequence = self._find_ordered_sequence(txns, ["DEPOSIT", "TRANSFER", "CASH_OUT"])

        if not sequence:
            return None

        total_amount = sum(float(t["amount"]) for t in sequence)
        alert_marker = self._build_alert_marker(customer_id, sequence)

        if total_amount >= self.min_total_amount and alert_marker not in self.alert_markers:
            self.alert_markers.add(alert_marker)
            return {
                "customer_id": customer_id,
                "pattern_type": "LAYERING",
                "alert_message": "Deposit -> Transfer -> Cash-out sequence detected within 2 hours",
                "risk_score": 90,
                "transactions": sequence,
            }

        return None

    def _find_ordered_sequence(self, transactions, sequence_types):
        matched = []
        next_index = 0

        for transaction in transactions:
            if transaction["transaction_type"] == sequence_types[next_index]:
                matched.append(transaction)
                next_index += 1

                if next_index == len(sequence_types):
                    return matched

        return []

    def _build_alert_marker(self, customer_id, transactions):
        transaction_ids = ",".join(txn["transaction_id"] for txn in transactions)
        return f"{customer_id}:LAYERING:{transaction_ids}"
