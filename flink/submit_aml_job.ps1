$ErrorActionPreference = "Stop"

Write-Host "Submitting AMLPatternDetectorStreamingJob to Flink..."

docker exec aml-flink-jobmanager `
    flink run `
    -d `
    -py /opt/aml/flink/aml_streaming_job.py

Write-Host "Submitted. Open Flink UI: http://localhost:8081"
