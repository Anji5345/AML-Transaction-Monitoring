import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "txn.raw")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "aml.alerts")
KAFKA_GROUP_ID = os.getenv("AML_FLINK_GROUP_ID", "aml-pattern-detector-flink-v1")


def parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def parse_transaction(raw_event: str) -> dict:
    return json.loads(raw_event)


def transaction_key(transaction: dict) -> str:
    return transaction["customer_id"]


def serialize_alert(alert: dict) -> str:
    return json.dumps(alert)


class AMLPatternProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        self.transactions_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "customer_transactions",
                Types.PICKLED_BYTE_ARRAY(),
            )
        )
        self.alert_markers_state = runtime_context.get_list_state(
            ListStateDescriptor(
                "alert_markers",
                Types.STRING(),
            )
        )

    def process_element(self, transaction, ctx):
        transactions = list(self.transactions_state.get())
        transactions.append(transaction)
        transactions = self._trim_to_30_days(transactions, transaction)

        self.transactions_state.update(transactions)

        for alert in self._detect_structuring(transaction, transactions):
            yield alert

        layering_alert = self._detect_layering(transaction, transactions)
        if layering_alert:
            yield layering_alert

        velocity_alert = self._detect_velocity(transaction, transactions)
        if velocity_alert:
            yield velocity_alert

    def _trim_to_30_days(self, transactions: list[dict], current_transaction: dict) -> list[dict]:
        current_time = parse_timestamp(current_transaction["transaction_time"])
        cutoff = current_time - timedelta(days=30)

        return [
            transaction
            for transaction in transactions
            if parse_timestamp(transaction["transaction_time"]) >= cutoff
        ][-500:]

    def _detect_structuring(self, current_transaction: dict, transactions: list[dict]) -> list[dict]:
        current_time = parse_timestamp(current_transaction["transaction_time"])
        cutoff = current_time - timedelta(hours=24)

        evidence = [
            transaction
            for transaction in transactions
            if (
                parse_timestamp(transaction["transaction_time"]) >= cutoff
                and 9000 < float(transaction["amount"]) < 10000
            )
        ]

        if len(evidence) < 3:
            return []

        if self._already_alerted("STRUCTURING", evidence):
            return []

        return [self._build_alert(
            customer_id=current_transaction["customer_id"],
            pattern_type="STRUCTURING",
            alert_message="Three or more sub-threshold transactions detected within 24 hours",
            risk_score=85,
            evidence=evidence,
        )]

    def _detect_layering(self, current_transaction: dict, transactions: list[dict]) -> Optional[dict]:
        current_time = parse_timestamp(current_transaction["transaction_time"])
        cutoff = current_time - timedelta(hours=2)
        recent = [
            transaction
            for transaction in transactions
            if parse_timestamp(transaction["transaction_time"]) >= cutoff
        ][-5:]

        sequence = self._find_ordered_sequence(
            recent,
            ["DEPOSIT", "TRANSFER", "CASH_OUT"],
        )

        if not sequence:
            return None

        total_amount = sum(float(transaction["amount"]) for transaction in sequence)
        if total_amount <= 50000:
            return None

        if self._already_alerted("LAYERING", sequence):
            return None

        return self._build_alert(
            customer_id=current_transaction["customer_id"],
            pattern_type="LAYERING",
            alert_message="Deposit to transfer to cash-out sequence detected within 2 hours",
            risk_score=90,
            evidence=sequence,
        )

    def _detect_velocity(self, current_transaction: dict, transactions: list[dict]) -> Optional[dict]:
        current_time = parse_timestamp(current_transaction["transaction_time"])
        current_hour_cutoff = current_time - timedelta(hours=1)
        thirty_day_cutoff = current_time - timedelta(days=30)

        current_hour = [
            transaction
            for transaction in transactions
            if parse_timestamp(transaction["transaction_time"]) >= current_hour_cutoff
        ]
        historical = [
            transaction
            for transaction in transactions
            if parse_timestamp(transaction["transaction_time"]) >= thirty_day_cutoff
        ]

        historical_hours = max(1, (current_time - thirty_day_cutoff).total_seconds() / 3600)
        rolling_avg = max(2.0, len(historical) / historical_hours)

        if len(current_hour) <= rolling_avg * 3:
            return None

        if self._already_alerted("VELOCITY", current_hour):
            return None

        return self._build_alert(
            customer_id=current_transaction["customer_id"],
            pattern_type="VELOCITY",
            alert_message="Current hourly transaction count exceeded 3x rolling average",
            risk_score=75,
            evidence=current_hour,
        )

    def _find_ordered_sequence(self, transactions: list[dict], sequence_types: list[str]) -> list[dict]:
        matched = []
        next_index = 0

        for transaction in transactions:
            if transaction["transaction_type"] == sequence_types[next_index]:
                matched.append(transaction)
                next_index += 1

                if next_index == len(sequence_types):
                    return matched

        return []

    def _already_alerted(self, pattern_type: str, evidence: list[dict]) -> bool:
        alert_marker = self._alert_marker(pattern_type, evidence)
        alert_markers = list(self.alert_markers_state.get())

        if alert_marker in alert_markers:
            return True

        alert_markers.append(alert_marker)
        self.alert_markers_state.update(alert_markers[-100:])
        return False

    def _build_alert(
        self,
        customer_id: str,
        pattern_type: str,
        alert_message: str,
        risk_score: int,
        evidence: list[dict],
    ) -> dict:
        transaction_ids = [transaction["transaction_id"] for transaction in evidence]
        total_amount = sum(float(transaction["amount"]) for transaction in evidence)
        alert_id = str(uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{customer_id}:{pattern_type}:{','.join(transaction_ids)}",
        ))

        return {
            "alert_id": alert_id,
            "customer_id": customer_id,
            "pattern_type": pattern_type,
            "alert_message": alert_message,
            "risk_score": risk_score,
            "total_amount": total_amount,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "transactions": evidence,
            "evidence": evidence,
        }

    def _alert_marker(self, pattern_type: str, evidence: list[dict]) -> str:
        transaction_ids = ",".join(transaction["transaction_id"] for transaction in evidence)
        return f"{pattern_type}:{transaction_ids}"


def build_kafka_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_INPUT_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_kafka_sink() -> KafkaSink:
    record_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(KAFKA_ALERT_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_record_serializer(record_serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(16)
    env.enable_checkpointing(30000)

    transactions = (
        env.from_source(
            source=build_kafka_source(),
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="txn.raw",
        )
        .map(parse_transaction, output_type=Types.PICKLED_BYTE_ARRAY())
    )

    alerts = (
        transactions
        .key_by(transaction_key)
        .process(AMLPatternProcessFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .map(serialize_alert, output_type=Types.STRING())
    )

    alerts.sink_to(build_kafka_sink())

    env.execute("AMLPatternDetectorStreamingJob")


if __name__ == "__main__":
    main()
