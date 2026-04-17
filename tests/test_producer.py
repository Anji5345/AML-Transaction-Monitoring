from producer.producer import (
    PROJECT_TEST_CSV_PATH,
    build_event,
    build_injected_events,
    load_transactions_from_csv,
    normalize_transaction_type,
)


def test_project_csv_loads_100_rows():
    df = load_transactions_from_csv(PROJECT_TEST_CSV_PATH, max_rows=100)

    assert len(df) == 100
    assert set(["step", "type", "amount", "nameOrig", "nameDest"]).issubset(df.columns)


def test_build_event_normalizes_cash_in_to_deposit():
    df = load_transactions_from_csv(PROJECT_TEST_CSV_PATH, max_rows=10)
    cash_in_row = df[df["type"] == "CASH_IN"].iloc[0]

    event = build_event(cash_in_row, index=4)

    assert event["transaction_id"] == "TXN_5"
    assert event["transaction_type"] == "DEPOSIT"
    assert event["customer_id"] == "C_LAYER_REQ_001"
    assert event["amount"] == 20000.0
    assert event["transaction_time"] == "2026-04-01 13:20:00"


def test_normalize_transaction_type_leaves_other_types_unchanged():
    assert normalize_transaction_type("TRANSFER") == "TRANSFER"
    assert normalize_transaction_type("CASH_OUT") == "CASH_OUT"
    assert normalize_transaction_type("PAYMENT") == "PAYMENT"


def test_injected_structuring_events_are_unique_and_keyed_to_customer():
    first_run = build_injected_events("structuring", "C_TEST_STRUCT")
    second_run = build_injected_events("structuring", "C_TEST_STRUCT")

    first_ids = {event["transaction_id"] for event in first_run}
    second_ids = {event["transaction_id"] for event in second_run}

    assert len(first_run) == 4
    assert all(event["customer_id"] == "C_TEST_STRUCT" for event in first_run)
    assert all(event["amount"] == 9800.0 for event in first_run)
    assert first_ids.isdisjoint(second_ids)


def test_injected_layering_sequence_matches_rule_order():
    events = build_injected_events("layering", "C_TEST_LAYER")

    assert [event["transaction_type"] for event in events] == ["DEPOSIT", "TRANSFER", "CASH_OUT"]
    assert sum(event["amount"] for event in events) == 60000.0


def test_injected_velocity_events_create_burst():
    events = build_injected_events("velocity", "C_TEST_VELOCITY")

    assert len(events) == 10
    assert all(event["customer_id"] == "C_TEST_VELOCITY" for event in events)
    assert [event["transaction_type"] for event in events] == ["PAYMENT"] * 10
