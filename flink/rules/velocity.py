from collections import defaultdict, deque
from datetime import datetime, timedelta


class VelocityDetector:
    def __init__(self, threshold_multiplier=3, default_hourly_average=2, window_hours=1):
        self.threshold_multiplier = threshold_multiplier
        self.default_hourly_average = default_hourly_average
        self.window_hours = window_hours
        self.customer_transactions = defaultdict(deque)
        self.last_alert_time = {}

    def process_event(self, event):
        customer_id = event["customer_id"]
        transaction_time = datetime.strptime(event["transaction_time"], "%Y-%m-%d %H:%M:%S")
        window_start = transaction_time - timedelta(hours=self.window_hours)
        transactions = self.customer_transactions[customer_id]

        transactions.append(event)

        while transactions:
            oldest_time = datetime.strptime(transactions[0]["transaction_time"], "%Y-%m-%d %H:%M:%S")
            if oldest_time >= window_start:
                break
            transactions.popleft()

        current_count = len(transactions)
        threshold = self.default_hourly_average * self.threshold_multiplier

        if current_count > threshold and self._can_alert(customer_id, transaction_time):
            self.last_alert_time[customer_id] = transaction_time

            return {
                "customer_id": customer_id,
                "pattern_type": "VELOCITY",
                "alert_message": "Transaction velocity exceeded 3x rolling hourly average",
                "risk_score": 75,
                "transactions": list(transactions),
            }

        return None

    def _can_alert(self, customer_id, transaction_time):
        last_alert_time = self.last_alert_time.get(customer_id)

        if last_alert_time is None:
            return True

        return transaction_time - last_alert_time >= timedelta(hours=self.window_hours)
