param(
    [Parameter(Mandatory = $true)]
    [string]$Username,

    [Parameter(Mandatory = $true)]
    [string]$Password,

    [string]$EmailTo = "aml-analyst@example.com"
)

$env:EMAIL_ENABLED = "true"
$env:SMTP_HOST = "sandbox.smtp.mailtrap.io"
$env:SMTP_PORT = "2525"
$env:SMTP_SECURITY = "starttls"
$env:SMTP_USE_TLS = "true"
$env:SMTP_USERNAME = $Username
$env:SMTP_PASSWORD = $Password
$env:EMAIL_FROM = "aml-monitoring@example.com"
$env:EMAIL_TO = $EmailTo
$env:HIGH_RISK_THRESHOLD = "85"

Write-Host "Mailtrap SMTP environment variables set for this PowerShell session."
Write-Host "Run: python notification/email_service.py --test"
