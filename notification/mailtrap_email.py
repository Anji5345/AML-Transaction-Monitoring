import os


def set_mailtrap_defaults():
    os.environ.setdefault("EMAIL_ENABLED", "true")
    os.environ.setdefault("SMTP_HOST", "sandbox.smtp.mailtrap.io")
    os.environ.setdefault("SMTP_PORT", "2525")
    os.environ.setdefault("SMTP_SECURITY", "starttls")
    os.environ.setdefault("SMTP_USE_TLS", "true")
    os.environ.setdefault("EMAIL_FROM", "aml-monitoring@example.com")
    os.environ.setdefault("EMAIL_TO", "aml-analyst@example.com")
    os.environ.setdefault("HIGH_RISK_THRESHOLD", "85")


def main():
    set_mailtrap_defaults()

    from email_service import build_test_alert, print_config, send_alert_email

    print_config()

    if not os.getenv("SMTP_USERNAME") or not os.getenv("SMTP_PASSWORD"):
        print("")
        print("Missing SMTP credentials.")
        print("Set them in PowerShell first:")
        print('$env:SMTP_USERNAME="YOUR_MAILTRAP_USERNAME"')
        print('$env:SMTP_PASSWORD="YOUR_MAILTRAP_PASSWORD"')
        return

    send_alert_email(build_test_alert())


if __name__ == "__main__":
    main()
