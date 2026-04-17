import json
import os
from datetime import datetime

import pandas as pd
import psycopg2
import streamlit as st

try:
    import plotly.express as px
except ImportError:
    px = None


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "amldb"),
    "user": os.getenv("POSTGRES_USER", "amluser"),
    "password": os.getenv("POSTGRES_PASSWORD", "amlpass"),
}


OUTCOME_STATUS = {
    "SAR Filed": "RESOLVED",
    "Closed No Action": "RESOLVED",
    "Escalated": "ESCALATED",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=5)
def load_cases() -> pd.DataFrame:
    query = """
        SELECT
            alert_id::text,
            customer_id,
            pattern_type,
            alert_message,
            risk_score,
            COALESCE(priority_score, risk_score) AS priority_score,
            total_amount,
            transaction_time,
            evidence,
            created_at,
            status,
            sla_deadline,
            GREATEST(0, DATE_PART('day', NOW() - created_at)::int) AS days_open
        FROM aml_cases
        ORDER BY
            CASE WHEN status = 'OPEN' THEN 0 ELSE 1 END,
            COALESCE(priority_score, risk_score) DESC,
            created_at DESC
    """

    with get_connection() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=5)
def load_decisions() -> pd.DataFrame:
    query = """
        SELECT
            decision_id,
            alert_id::text,
            analyst_id,
            outcome,
            notes,
            decided_at
        FROM case_decisions
        ORDER BY decided_at DESC
    """

    with get_connection() as conn:
        return pd.read_sql(query, conn)


def save_decision(alert_id: str, analyst_id: str, outcome: str, notes: str):
    status = OUTCOME_STATUS[outcome]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO case_decisions (alert_id, analyst_id, outcome, notes)
                VALUES (%s::uuid, %s, %s, %s)
                """,
                (alert_id, analyst_id, outcome, notes),
            )
            cur.execute(
                """
                UPDATE aml_cases
                SET status = %s
                WHERE alert_id = %s::uuid
                """,
                (status, alert_id),
            )
        conn.commit()


def normalize_evidence(evidence):
    if evidence is None:
        return []

    if isinstance(evidence, list):
        return evidence

    if isinstance(evidence, str):
        try:
            return json.loads(evidence)
        except json.JSONDecodeError:
            return []

    return evidence


def risk_band(score: int) -> str:
    if score > 80:
        return "Red"
    if score >= 50:
        return "Amber"
    return "Green"


def format_currency(value) -> str:
    if value is None or pd.isna(value):
        return "$0.00"
    return f"${float(value):,.2f}"


def short_case_id(alert_id: str) -> str:
    return alert_id.split("-")[0].upper()


def render_kpis(cases: pd.DataFrame, decisions: pd.DataFrame):
    today = pd.Timestamp(datetime.now().date())
    open_cases = cases[cases["status"] == "OPEN"]
    opened_today = cases[pd.to_datetime(cases["created_at"]).dt.date == today.date()]
    resolved_this_week = decisions[
        pd.to_datetime(decisions["decided_at"]) >= pd.Timestamp.now() - pd.Timedelta(days=7)
    ]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Open Cases", len(open_cases))
    col2.metric("Opened Today", len(opened_today))
    col3.metric("Avg Risk Score", f"{cases['risk_score'].mean():.1f}" if not cases.empty else "0")
    col4.metric("Resolved This Week", len(resolved_this_week))


def render_queue(cases: pd.DataFrame) -> pd.DataFrame:
    queue = cases.copy()
    queue["case_id"] = queue["alert_id"].apply(short_case_id)
    queue["risk_band"] = queue["priority_score"].apply(risk_band)
    queue["total_amount"] = queue["total_amount"].apply(format_currency)

    display_columns = [
        "case_id",
        "customer_id",
        "pattern_type",
        "priority_score",
        "risk_band",
        "days_open",
        "total_amount",
        "status",
        "sla_deadline",
    ]

    st.dataframe(
        queue[display_columns],
        hide_index=True,
        use_container_width=True,
    )

    return queue


def render_pattern_chart(cases: pd.DataFrame):
    counts = cases.groupby("pattern_type").size().reset_index(name="case_count")

    if px:
        fig = px.bar(
            counts,
            x="pattern_type",
            y="case_count",
            color="pattern_type",
            color_discrete_map={
                "STRUCTURING": "#d62728",
                "LAYERING": "#ffbf00",
                "VELOCITY": "#2ca02c",
            },
        )
        fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="Cases")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.bar_chart(counts.set_index("pattern_type"))


def render_evidence(case: pd.Series):
    evidence = normalize_evidence(case["evidence"])

    st.subheader("Case Evidence")
    st.write(case["alert_message"])

    if not evidence:
        st.info("No evidence transactions were stored for this case.")
        return

    evidence_df = pd.DataFrame(evidence)

    if "amount" in evidence_df.columns:
        evidence_df["amount"] = evidence_df["amount"].astype(float)

    if "transaction_time" in evidence_df.columns:
        evidence_df["transaction_time"] = pd.to_datetime(evidence_df["transaction_time"])

    if px and {"transaction_time", "amount", "transaction_type"}.issubset(evidence_df.columns):
        fig = px.scatter(
            evidence_df,
            x="transaction_time",
            y="amount",
            color="transaction_type",
            hover_data=["transaction_id", "customer_id"] if "customer_id" in evidence_df.columns else ["transaction_id"],
        )
        fig.update_traces(marker={"size": 12})
        fig.update_layout(xaxis_title="Transaction Time", yaxis_title="Amount")
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(evidence_df, hide_index=True, use_container_width=True)


def render_false_positive_trend(decisions: pd.DataFrame):
    if decisions.empty:
        st.info("No analyst decisions yet.")
        return

    df = decisions.copy()
    df["week"] = pd.to_datetime(df["decided_at"]).dt.to_period("W").dt.start_time
    weekly = df.groupby("week").agg(
        total_closed=("decision_id", "count"),
        false_positive=("outcome", lambda values: (values == "Closed No Action").sum()),
    )
    weekly["false_positive_rate"] = weekly["false_positive"] / weekly["total_closed"]

    st.line_chart(weekly["false_positive_rate"])


def render_decision_form(case: pd.Series):
    st.subheader("Decision")

    with st.form("case_decision_form", clear_on_submit=False):
        analyst_id = st.text_input("Analyst ID", value=os.getenv("ANALYST_ID", "analyst_001"))
        outcome = st.selectbox("Outcome", list(OUTCOME_STATUS.keys()))
        notes = st.text_area("Notes")
        submitted = st.form_submit_button("Submit Decision")

    if submitted:
        if not analyst_id.strip():
            st.error("Analyst ID is required.")
            return

        save_decision(case["alert_id"], analyst_id.strip(), outcome, notes.strip())
        st.cache_data.clear()
        st.success("Decision saved.")
        st.rerun()


def main():
    st.set_page_config(page_title="AML Case Management", layout="wide")
    st.title("AML Case Management")

    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()

    try:
        cases = load_cases()
        decisions = load_decisions()
    except Exception as exc:
        st.error(f"Could not load dashboard data: {exc}")
        st.stop()

    if cases.empty:
        st.info("No AML cases found. Run the producer, detector, and Postgres writer first.")
        st.stop()

    with st.sidebar:
        st.header("Filters")
        status_options = ["All"] + sorted(cases["status"].dropna().unique().tolist())
        pattern_options = ["All"] + sorted(cases["pattern_type"].dropna().unique().tolist())
        selected_status = st.selectbox("Status", status_options)
        selected_pattern = st.selectbox("Pattern Type", pattern_options)

    filtered_cases = cases.copy()

    if selected_status != "All":
        filtered_cases = filtered_cases[filtered_cases["status"] == selected_status]

    if selected_pattern != "All":
        filtered_cases = filtered_cases[filtered_cases["pattern_type"] == selected_pattern]

    if filtered_cases.empty:
        render_kpis(cases, decisions)
        st.info("No cases match the selected filters.")
        st.stop()

    render_kpis(cases, decisions)

    left, right = st.columns([1.35, 1])

    with left:
        st.subheader("Analyst Queue")
        queue = render_queue(filtered_cases)

        selected_alert_id = st.selectbox(
            "Select Case",
            queue["alert_id"].tolist(),
            format_func=lambda alert_id: (
                f"{queue.loc[queue['alert_id'] == alert_id, 'pattern_type'].iloc[0]} | "
                f"{queue.loc[queue['alert_id'] == alert_id, 'customer_id'].iloc[0]} | "
                f"Score {queue.loc[queue['alert_id'] == alert_id, 'priority_score'].iloc[0]}"
            ),
        )

    selected_case = cases[cases["alert_id"] == selected_alert_id].iloc[0]

    with right:
        st.subheader("Pattern Summary")
        render_pattern_chart(cases)

        st.subheader("False Positive Trend")
        render_false_positive_trend(decisions)

    detail_left, detail_right = st.columns([1.5, 1])

    with detail_left:
        render_evidence(selected_case)

    with detail_right:
        st.subheader("Case Details")
        st.write(f"Customer ID: {selected_case['customer_id']}")
        st.write(f"Pattern: {selected_case['pattern_type']}")
        st.write(f"Risk Score: {selected_case['risk_score']}")
        st.write(f"Priority Score: {selected_case['priority_score']}")
        st.write(f"Total Amount: {format_currency(selected_case['total_amount'])}")
        st.write(f"Status: {selected_case['status']}")
        st.write(f"SLA Deadline: {selected_case['sla_deadline']}")

        render_decision_form(selected_case)


if __name__ == "__main__":
    main()
