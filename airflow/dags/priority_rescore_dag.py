import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task


AML_DB_CONFIG = {
    "host": os.getenv("AML_POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("AML_POSTGRES_PORT", "5432")),
    "dbname": os.getenv("AML_POSTGRES_DB", "amldb"),
    "user": os.getenv("AML_POSTGRES_USER", "amluser"),
    "password": os.getenv("AML_POSTGRES_PASSWORD", "amlpass"),
}


DEFAULT_ARGS = {
    "owner": "aml-compliance",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_connection():
    return psycopg2.connect(**AML_DB_CONFIG)


@dag(
    dag_id="aml_priority_rescore",
    description="Nightly AML case priority re-score and repeat-alert escalation.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["aml", "priority", "postgres"],
)
def aml_priority_rescore():
    @task
    def ensure_history_table():
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS case_priority_history (
                        history_id SERIAL PRIMARY KEY,
                        alert_id UUID REFERENCES aml_cases(alert_id),
                        old_priority_score INT,
                        new_priority_score INT,
                        old_status VARCHAR(20),
                        new_status VARCHAR(20),
                        reason TEXT,
                        rescored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            conn.commit()

    @task
    def rescore_open_cases() -> dict:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH customer_alert_counts AS (
                        SELECT
                            customer_id,
                            COUNT(*) AS prior_alert_count
                        FROM aml_cases
                        GROUP BY customer_id
                    ),
                    scored_cases AS (
                        SELECT
                            c.alert_id,
                            c.customer_id,
                            c.status AS old_status,
                            COALESCE(c.priority_score, c.risk_score) AS old_priority_score,
                            LEAST(
                                100,
                                c.risk_score
                                + CASE
                                    WHEN c.pattern_type = 'LAYERING' THEN 10
                                    WHEN c.pattern_type = 'STRUCTURING' THEN 8
                                    WHEN c.pattern_type = 'VELOCITY' THEN 5
                                    ELSE 0
                                  END
                                + CASE
                                    WHEN COALESCE(ac.prior_alert_count, 0) >= 3 THEN 20
                                    WHEN COALESCE(ac.prior_alert_count, 0) = 2 THEN 10
                                    ELSE 0
                                  END
                                + CASE
                                    WHEN c.created_at <= NOW() - INTERVAL '1 day' THEN 5
                                    ELSE 0
                                  END
                            )::int AS new_priority_score,
                            CASE
                                WHEN COALESCE(ac.prior_alert_count, 0) >= 3 THEN 'ESCALATED'
                                ELSE c.status
                            END AS new_status,
                            CONCAT(
                                'pattern=', c.pattern_type,
                                '; prior_alert_count=', COALESCE(ac.prior_alert_count, 0),
                                '; age_days=', GREATEST(0, DATE_PART('day', NOW() - c.created_at)::int)
                            ) AS reason
                        FROM aml_cases c
                        LEFT JOIN customer_alert_counts ac
                            ON c.customer_id = ac.customer_id
                        WHERE c.status IN ('OPEN', 'ESCALATED')
                    ),
                    changed_cases AS (
                        SELECT *
                        FROM scored_cases
                        WHERE new_priority_score <> old_priority_score
                           OR new_status <> old_status
                    ),
                    updated_cases AS (
                        UPDATE aml_cases c
                        SET priority_score = cc.new_priority_score,
                            status = cc.new_status
                        FROM changed_cases cc
                        WHERE c.alert_id = cc.alert_id
                        RETURNING
                            c.alert_id,
                            cc.old_priority_score,
                            cc.new_priority_score,
                            cc.old_status,
                            cc.new_status,
                            cc.reason
                    )
                    INSERT INTO case_priority_history (
                        alert_id,
                        old_priority_score,
                        new_priority_score,
                        old_status,
                        new_status,
                        reason
                    )
                    SELECT
                        alert_id,
                        old_priority_score,
                        new_priority_score,
                        old_status,
                        new_status,
                        reason
                    FROM updated_cases
                    RETURNING alert_id, new_status
                    """
                )
                changed_rows = cur.fetchall()
            conn.commit()

        escalated_count = sum(1 for _, status in changed_rows if status == "ESCALATED")
        return {
            "rescored_cases": len(changed_rows),
            "escalated_cases": escalated_count,
        }

    @task
    def log_summary(summary: dict):
        print(
            "AML priority rescore complete: "
            f"rescored_cases={summary['rescored_cases']}, "
            f"escalated_cases={summary['escalated_cases']}"
        )

    history_ready = ensure_history_table()
    summary = rescore_open_cases()
    final_log = log_summary(summary)

    history_ready >> summary >> final_log


aml_priority_rescore()
