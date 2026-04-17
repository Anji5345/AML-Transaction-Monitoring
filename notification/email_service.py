import argparse
import os
import smtplib
from email.message import EmailMessage


EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
SMTP_HOST = os.getenv("SMTP_HOST", "sandbox.smtp.mailtrap.io")
SMTP_PORT = int(os.getenv("SMTP_PORT", "2525"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_SECURITY = os.getenv("SMTP_SECURITY", "starttls").lower()
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
EMAIL_FROM = os.getenv("EMAIL_FROM", "aml-monitoring@example.com")
EMAIL_TO = os.getenv("EMAIL_TO", "aml-analyst@example.com")
HIGH_RISK_THRESHOLD = int(os.getenv("HIGH_RISK_THRESHOLD", "85"))


def should_notify(alert: dict) -> bool:
    return int(alert.get("risk_score", 0)) >= HIGH_RISK_THRESHOLD


def build_alert_email(alert: dict) -> EmailMessage:
    evidence = alert.get("evidence") or alert.get("transactions", [])
    pattern_type = alert.get("pattern_type", "UNKNOWN")
    customer_id = alert.get("customer_id", "UNKNOWN")

    message = EmailMessage()
    message["From"] = EMAIL_FROM
    message["To"] = EMAIL_TO
    message["Subject"] = f"High Risk AML Alert: {pattern_type} for {customer_id}"

    body = [
        "A high-risk AML alert was generated.",
        "",
        f"Alert ID: {alert.get('alert_id', 'N/A')}",
        f"Customer ID: {customer_id}",
        f"Pattern Type: {pattern_type}",
        f"Risk Score: {alert.get('risk_score', 'N/A')}",
        f"Total Amount: {alert.get('total_amount', 'N/A')}",
        f"Message: {alert.get('alert_message', 'N/A')}",
        f"Evidence Transactions: {len(evidence)}",
        "",
        "Evidence:",
    ]

    for transaction in evidence[:10]:
        body.append(
            "- "
            f"{transaction.get('transaction_id', 'N/A')} | "
            f"{transaction.get('transaction_type', 'N/A')} | "
            f"${float(transaction.get('amount', 0)):,.2f} | "
            f"{transaction.get('transaction_time', 'N/A')}"
        )

    if len(evidence) > 10:
        body.append(f"- ... {len(evidence) - 10} more transaction(s)")

    body.extend([
        "",
        "Review this case in the AML case management dashboard.",
    ])

    message.set_content("\n".join(body))
    return message


def send_alert_email(alert: dict) -> bool:
    if not should_notify(alert):
        print(
            "Email skipped: "
            f"risk_score={alert.get('risk_score')} threshold={HIGH_RISK_THRESHOLD}"
        )
        return False

    message = build_alert_email(alert)

    if not EMAIL_ENABLED:
        print(
            "EMAIL DRY RUN: "
            f"to={EMAIL_TO} subject={message['Subject']}"
        )
        return False

    if not SMTP_USERNAME or not SMTP_PASSWORD:
        raise ValueError("SMTP_USERNAME and SMTP_PASSWORD are required when EMAIL_ENABLED=true")

    if SMTP_SECURITY not in {"none", "starttls", "ssl"}:
        raise ValueError("SMTP_SECURITY must be one of: none, starttls, ssl")

    if SMTP_SECURITY == "ssl":
        smtp_client = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15)
    else:
        smtp_client = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)

    with smtp_client as smtp:
        smtp.ehlo()

        if SMTP_SECURITY == "starttls" or (SMTP_SECURITY == "none" and SMTP_USE_TLS):
            smtp.starttls()
            smtp.ehlo()

        smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(message)

    print(f"Email notification sent for alert {alert.get('alert_id')}")
    return True


def build_test_alert() -> dict:
    return {
        "alert_id": "TEST-EMAIL-ALERT",
        "customer_id": "C_TEST_EMAIL",
        "pattern_type": "STRUCTURING",
        "risk_score": 95,
        "total_amount": 39200.00,
        "alert_message": "Test high-risk AML alert notification",
        "evidence": [
            {
                "transaction_id": "TEST_TXN_1",
                "transaction_type": "TRANSFER",
                "amount": 9800.00,
                "transaction_time": "2026-04-01 00:01:00",
            },
            {
                "transaction_id": "TEST_TXN_2",
                "transaction_type": "TRANSFER",
                "amount": 9800.00,
                "transaction_time": "2026-04-01 03:00:00",
            },
            {
                "transaction_id": "TEST_TXN_3",
                "transaction_type": "CASH_OUT",
                "amount": 9800.00,
                "transaction_time": "2026-04-01 06:00:00",
            },
        ],
    }


def print_config():
    print("Email notification config:")
    print(f"  EMAIL_ENABLED={EMAIL_ENABLED}")
    print(f"  SMTP_HOST={SMTP_HOST}")
    print(f"  SMTP_PORT={SMTP_PORT}")
    print(f"  SMTP_SECURITY={SMTP_SECURITY}")
    print(f"  SMTP_USE_TLS={SMTP_USE_TLS}")
    print(f"  SMTP_USERNAME={'set' if SMTP_USERNAME else 'not set'}")
    print(f"  SMTP_PASSWORD={'set' if SMTP_PASSWORD else 'not set'}")
    print(f"  EMAIL_FROM={EMAIL_FROM}")
    print(f"  EMAIL_TO={EMAIL_TO}")
    print(f"  HIGH_RISK_THRESHOLD={HIGH_RISK_THRESHOLD}")


def parse_args():
    parser = argparse.ArgumentParser(description="Send or preview AML email notifications.")
    parser.add_argument("--test", action="store_true", help="Send a test high-risk AML alert email.")
    parser.add_argument("--config", action="store_true", help="Print current email configuration.")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.config:
        print_config()

    if args.test:
        send_alert_email(build_test_alert())

    if not args.config and not args.test:
        print("Email service module loaded.")
        print("Use --config to view settings or --test to send a test notification.")


if __name__ == "__main__":
    main()
