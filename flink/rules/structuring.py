from collections import defaultdict


class StructuringDetector:
    def __init__(self, min_amount=9000, max_amount=10000, threshold_count=3):
        self.min_amount = min_amount
        self.max_amount = max_amount
        self.threshold_count = threshold_count
        self.customer_transactions = defaultdict(list)
        self.customer_seen_txns = defaultdict(set)
        self.alerted_customers = set()

    def process_event(self, event):
        customer_id = event["customer_id"]
        transaction_id = event["transaction_id"]
        amount = event["amount"]

        if transaction_id in self.customer_seen_txns[customer_id]:
            return None

        if self.min_amount < amount < self.max_amount:
            self.customer_seen_txns[customer_id].add(transaction_id)
            self.customer_transactions[customer_id].append(event)

        txn_count = len(self.customer_transactions[customer_id])

        if txn_count == self.threshold_count and customer_id not in self.alerted_customers:
            self.alerted_customers.add(customer_id)

            return {
                "customer_id": customer_id,
                "pattern_type": "STRUCTURING",
                "alert_message": f"{self.threshold_count} or more near-threshold transactions detected",
                "risk_score": 85,
                "transactions": self.customer_transactions[customer_id]
            }

        return None