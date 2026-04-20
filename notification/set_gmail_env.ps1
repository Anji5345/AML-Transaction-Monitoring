param(
    [Parameter(Mandatory = $true)]
    [string]$GmailAddress,

    [Parameter(Mandatory = $true)]
    [string]$AppPassword,

    [string]$ReceiverEmail = $GmailAddress,

    [int]$HighRiskThreshold = 75
)

$cleanPassword = $AppPassword -replace "\s", ""

$env:EMAIL_ENABLED = "true"
$env:SMTP_HOST = "smtp.gmail.com"
$env:SMTP_PORT = "587"
$env:SMTP_SECURITY = "starttls"
$env:SMTP_USE_TLS = "true"
$env:SMTP_USERNAME = $GmailAddress
$env:SMTP_PASSWORD = $cleanPassword
$env:EMAIL_FROM = $GmailAddress
$env:EMAIL_TO = $ReceiverEmail
$env:HIGH_RISK_THRESHOLD = "$HighRiskThreshold"

Write-Host "Gmail SMTP alert environment configured for this PowerShell session."
Write-Host "Sender: $GmailAddress"
Write-Host "Receiver: $ReceiverEmail"
Write-Host "High risk threshold: $HighRiskThreshold"
Write-Host ""
Write-Host "Next test command:"
Write-Host "python notification/email_service.py --config --test"
