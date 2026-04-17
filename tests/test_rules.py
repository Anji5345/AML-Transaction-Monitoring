from producer.producer import build_event, build_injected_events, load_transactions_from_csv
from flink.rules.layering import LayeringDetector
from flink.rules.structuring import StructuringDetector
from flink.rules.velocity import VelocityDetector


def collect_alerts(detector, events):
    alerts = []

    for event in events:
        alert = detector.process_event(event)
        if alert:
            alerts.append(alert)

    return alerts


def test_structuring_alerts_once_at_threshold():
    detector = StructuringDetector()
    events = build_injected_events("structuring", "C_STRUCT_UNIT")

    alerts = collect_alerts(detector, events)

    assert len(alerts) == 1
    assert alerts[0]["pattern_type"] == "STRUCTURING"
    assert alerts[0]["customer_id"] == "C_STRUCT_UNIT"
    assert len(alerts[0]["transactions"]) == 3


def test_structuring_ignores_non_qualifying_amounts():
    detector = StructuringDetector()
    events = [
        {
            "transaction_id": f"LOW_{index}",
            "customer_id": "C_LOW",
            "transaction_type": "TRANSFER",
            "amount": 8500.0,
            "transaction_time": f"2026-04-01 0{index}:00:00",
        }
        for index in range(3)
    ]

    alerts = collect_alerts(detector, events)

    assert alerts == []


def test_layering_detects_ordered_sequence_within_window():
    detector = LayeringDetector()
    events = build_injected_events("layering", "C_LAYER_UNIT")

    alerts = collect_alerts(detector, events)

    assert len(alerts) == 1
    assert alerts[0]["pattern_type"] == "LAYERING"
    assert [txn["transaction_type"] for txn in alerts[0]["transactions"]] == [
        "DEPOSIT",
        "TRANSFER",
        "CASH_OUT",
    ]


def test_layering_rejects_low_total_amount():
    detector = LayeringDetector()
    events = build_injected_events("layering", "C_LAYER_LOW")

    for event in events:
        event["amount"] = 1000.0

    alerts = collect_alerts(detector, events)

    assert alerts == []


def test_velocity_alerts_once_per_active_window():
    detector = VelocityDetector()
    events = build_injected_events("velocity", "C_VELOCITY_UNIT")

    alerts = collect_alerts(detector, events)

    assert len(alerts) == 1
    assert alerts[0]["pattern_type"] == "VELOCITY"
    assert len(alerts[0]["transactions"]) == 7


def test_project_csv_generates_expected_alert_types_once():
    df = load_transactions_from_csv("data/raw/aml_project_test_paysim.csv", 100)
    detectors = [StructuringDetector(), LayeringDetector(), VelocityDetector()]
    alert_types = []

    for index, row in df.iterrows():
        event = build_event(row, index)

        for detector in detectors:
            alert = detector.process_event(event)
            if alert:
                alert_types.append(alert["pattern_type"])

    assert alert_types == ["STRUCTURING", "LAYERING", "VELOCITY"]
